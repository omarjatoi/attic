//! GCS remote files.

use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use google_cloud_auth::signer::Signer;
use google_cloud_storage::builder::storage::SignedUrlBuilder;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::streaming_source::StreamingSource;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use super::{Download, RemoteFile, StorageBackend};
use crate::error::{ErrorKind, ServerError, ServerResult};
use attic::io::read_chunk_async;

/// The chunk size for each part in a multipart upload.
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Adapter that implements [`StreamingSource`] by greedily reading full
/// chunks from a [`tokio::io::DuplexStream`] via [`read_chunk_async`].
///
/// This exists because the GCS SDK's `StreamingSource` requires `Sync`,
/// which `dyn AsyncRead + Send` does not satisfy. A duplex channel bridges
/// the non-`Sync` input stream to this `Sync`-safe reader.
struct DuplexChunkSource {
    reader: tokio::io::DuplexStream,
    done: bool,
}

impl StreamingSource for DuplexChunkSource {
    type Error = std::io::Error;

    async fn next(&mut self) -> Option<Result<Bytes, Self::Error>> {
        if self.done {
            return None;
        }
        let buf = BytesMut::with_capacity(CHUNK_SIZE);
        match read_chunk_async(&mut self.reader, buf).await {
            Err(e) => Some(Err(e)),
            Ok(chunk) if chunk.is_empty() => {
                self.done = true;
                None
            }
            Ok(chunk) => Some(Ok(chunk)),
        }
    }
}

/// The GCS remote file storage backend.
pub struct GcsBackend {
    client: Storage,
    control: StorageControl,
    signer: Option<Signer>,
    config: GcsStorageConfig,
}

impl std::fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBackend")
            .field("config", &self.config)
            .field("has_signer", &self.signer.is_some())
            .finish_non_exhaustive()
    }
}

/// GCS remote file storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GcsStorageConfig {
    /// The name of the bucket.
    bucket: String,

    /// Path to a service account credentials JSON file.
    ///
    /// If not set, Application Default Credentials (ADC) are used.
    #[serde(rename = "credentials-file")]
    credentials_file: Option<String>,

    /// Custom GCS endpoint.
    ///
    /// Set this if using a GCS emulator or private endpoint.
    endpoint: Option<String>,
}

/// Reference to a file in a GCS bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcsRemoteFile {
    /// Name of the bucket.
    pub bucket: String,

    /// Name of the object.
    pub name: String,
}

impl GcsBackend {
    pub async fn new(config: GcsStorageConfig) -> ServerResult<Self> {
        let cred = match config.credentials_file.as_ref() {
            Some(path) => {
                let json_str = std::fs::read_to_string(path).map_err(|e| {
                    ErrorKind::StorageError(anyhow::anyhow!(
                        "Failed to read GCS credentials file {path}: {e}"
                    ))
                })?;
                let json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                    ErrorKind::StorageError(anyhow::anyhow!(
                        "Invalid GCS credentials JSON in {path}: {e}"
                    ))
                })?;
                let cred = google_cloud_auth::credentials::service_account::Builder::new(json)
                    .build()
                    .map_err(|e| {
                        ErrorKind::StorageError(anyhow::anyhow!(
                            "Failed to build GCS credentials: {e}"
                        ))
                    })?;
                Some(cred)
            }
            None => None,
        };

        let mut storage_builder = Storage::builder().with_resumable_upload_buffer_size(CHUNK_SIZE);
        let mut control_builder = StorageControl::builder();

        if let Some(ref cred) = cred {
            storage_builder = storage_builder.with_credentials(cred.clone());
            control_builder = control_builder.with_credentials(cred.clone());
        }

        if let Some(ref endpoint) = config.endpoint {
            storage_builder = storage_builder.with_endpoint(endpoint);
            control_builder = control_builder.with_endpoint(endpoint);
        }

        let client = storage_builder
            .build()
            .await
            .map_err(ServerError::storage_error)?;

        let control = control_builder
            .build()
            .await
            .map_err(ServerError::storage_error)?;

        let signer = if let Some(ref path) = config.credentials_file {
            let json_str = std::fs::read_to_string(path).map_err(|e| {
                ErrorKind::StorageError(anyhow::anyhow!(
                    "Failed to read credentials for signer: {e}"
                ))
            })?;
            let json: serde_json::Value = serde_json::from_str(&json_str).map_err(|e| {
                ErrorKind::StorageError(anyhow::anyhow!("Invalid credentials JSON for signer: {e}"))
            })?;
            match google_cloud_auth::credentials::service_account::Builder::new(json).build_signer()
            {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("GCS signed URLs unavailable (falling back to streaming): {e}");
                    None
                }
            }
        } else {
            match google_cloud_auth::credentials::Builder::default().build_signer() {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("GCS signed URLs unavailable (falling back to streaming): {e}");
                    None
                }
            }
        };

        Ok(Self {
            client,
            control,
            signer,
            config,
        })
    }

    fn get_db_ref<'a>(&self, file: &'a RemoteFile) -> ServerResult<&'a GcsRemoteFile> {
        if let RemoteFile::GCS(file) = file {
            Ok(file)
        } else {
            Err(ErrorKind::StorageError(anyhow::anyhow!(
                "Does not understand the remote file reference"
            ))
            .into())
        }
    }

    /// Returns the bucket path in the format expected by the GCS API.
    fn bucket_path(bucket: &str) -> String {
        format!("projects/_/buckets/{}", bucket)
    }

    async fn get_download(
        &self,
        bucket: &str,
        name: &str,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let bucket_path = Self::bucket_path(bucket);

        if !prefer_stream {
            if let Some(signer) = &self.signer {
                let url = SignedUrlBuilder::for_object(&bucket_path, name)
                    .with_method(google_cloud_storage::http::Method::GET)
                    .with_expiration(Duration::from_secs(600))
                    .sign_with(signer)
                    .await
                    .map_err(|e| ErrorKind::StorageError(anyhow::anyhow!("{e}")))?;
                return Ok(Download::Url(url));
            }
        }

        let resp = self
            .client
            .read_object(&bucket_path, name)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        let byte_stream = async_stream::stream! {
            let mut resp = resp;
            while let Some(chunk) = resp.next().await {
                match chunk {
                    Ok(bytes) => yield Ok(bytes),
                    Err(e) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        break;
                    }
                }
            }
        };

        let reader = StreamReader::new(Box::pin(byte_stream));
        Ok(Download::AsyncRead(Box::new(reader)))
    }
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn upload_file(
        &self,
        name: String,
        stream: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ServerResult<RemoteFile> {
        let (mut tx, rx) = tokio::io::duplex(CHUNK_SIZE);

        let source = DuplexChunkSource {
            reader: rx,
            done: false,
        };

        let upload_fut = self
            .client
            .write_object(&Self::bucket_path(&self.config.bucket), &name, source)
            .send_buffered();

        let copy_fut = async {
            let _ = tokio::io::copy(stream, &mut tx).await;
            drop(tx);
        };

        let (upload_result, _) = tokio::join!(upload_fut, copy_fut);

        let upload = upload_result.map_err(ServerError::storage_error)?;

        tracing::debug!("upload_object -> {:#?}", upload);

        Ok(RemoteFile::GCS(GcsRemoteFile {
            bucket: self.config.bucket.clone(),
            name,
        }))
    }

    async fn delete_file(&self, name: String) -> ServerResult<()> {
        self.control
            .delete_object()
            .set_bucket(Self::bucket_path(&self.config.bucket))
            .set_object(&name)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file -> {}", name);

        Ok(())
    }

    async fn delete_file_db(&self, file: &RemoteFile) -> ServerResult<()> {
        let file = self.get_db_ref(file)?;

        self.control
            .delete_object()
            .set_bucket(Self::bucket_path(&file.bucket))
            .set_object(&file.name)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file -> {}/{}", file.bucket, file.name);

        Ok(())
    }

    async fn download_file(&self, name: String, prefer_stream: bool) -> ServerResult<Download> {
        self.get_download(&self.config.bucket, &name, prefer_stream)
            .await
    }

    async fn download_file_db(
        &self,
        file: &RemoteFile,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let file = self.get_db_ref(file)?;
        self.get_download(&file.bucket, &file.name, prefer_stream)
            .await
    }

    async fn make_db_reference(&self, name: String) -> ServerResult<RemoteFile> {
        Ok(RemoteFile::GCS(GcsRemoteFile {
            bucket: self.config.bucket.clone(),
            name,
        }))
    }
}
