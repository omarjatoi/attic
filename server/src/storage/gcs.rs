//! Google Cloud Storage remote files.

use std::io::Cursor;

use async_trait::async_trait;
use bytes::BytesMut;
use google_cloud_storage::client::{Client as Storage, ClientConfig as StorageConfig};
use google_cloud_storage::http::objects::delete::DeleteObjectRequest;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{UploadObjectRequest, UploadType};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use futures::StreamExt;
use tokio_util::io::StreamReader;
use std::borrow::Cow;

use super::{Download, RemoteFile, StorageBackend};
use crate::error::{ErrorKind, ServerError, ServerResult};
use attic::io::read_chunk_async;

/// The chunk size for each part in a multipart upload.
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// The GCS remote file storage backend.
pub struct GcsBackend {
    client: Storage,
    config: GcsStorageConfig,
}

impl std::fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBackend")
            .field("config", &self.config)
            .finish()
    }
}

/// GCS remote file storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GcsStorageConfig {
    /// The name of the bucket.
    pub bucket: String,

    /// The Google Cloud project ID.
    ///
    /// If not specified, the project ID from Application Default Credentials
    /// or the special value "_" will be used.
    pub project_id: Option<String>,

    /// The Google Cloud region.
    ///
    /// This is used for regional endpoints and bucket location.
    /// Examples: "us-central1", "europe-west1", "asia-southeast1"
    pub region: Option<String>,
}

/// Reference to a file in a GCS bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcsRemoteFile {
    /// Name of the bucket.
    pub bucket: String,

    /// Object name (key) of the file.
    pub object: String,

    /// Project ID where the bucket resides (optional).
    pub project_id: Option<String>,
}

impl GcsBackend {
    pub async fn new(config: GcsStorageConfig) -> ServerResult<Self> {
        let client_config = StorageConfig::default();
        let client = Storage::new(client_config);

        Ok(Self { client, config })
    }

    /// Format bucket name with project ID for GCS API (using backend config)
    fn bucket_name(&self) -> String {
        let project_id = self.config.project_id.as_deref().unwrap_or("_");
        format!("projects/{}/buckets/{}", project_id, self.config.bucket)
    }

    /// Format bucket name from a GcsRemoteFile for cross-bucket operations
    fn bucket_name_for_file(file: &GcsRemoteFile) -> String {
        let project_id = file.project_id.as_deref().unwrap_or("_");
        format!("projects/{}/buckets/{}", project_id, file.bucket)
    }

    fn get_gcs_file_from_db_ref(file: &RemoteFile) -> ServerResult<&GcsRemoteFile> {
        if let RemoteFile::Gcs(file) = file {
            Ok(file)
        } else {
            Err(ErrorKind::StorageError(anyhow::anyhow!(
                "Does not understand the remote file reference"
            ))
            .into())
        }
    }

    async fn get_download(
        &self,
        bucket_name: &str,
        object_name: &str,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let get_req = GetObjectRequest {
            bucket: bucket_name.to_string(),
            object: object_name.to_string(),
            ..Default::default()
        };

        if prefer_stream {
            // Use streaming download to avoid loading entire file into memory
            let stream = self
                .client
                .download_streamed_object(&get_req, &Range::default())
                .await
                .map_err(ServerError::storage_error)?;

            // Convert Stream to AsyncRead
            let stream = stream.map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
            let reader = StreamReader::new(stream);
            Ok(Download::AsyncRead(Box::new(reader)))
        } else {
            // Download entire object into memory
            let data = self
                .client
                .download_object(&get_req, &Range::default())
                .await
                .map_err(ServerError::storage_error)?;

            let cursor = Cursor::new(data);
            Ok(Download::AsyncRead(Box::new(cursor)))
        }
    }
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn upload_file(
        &self,
        name: String,
        mut stream: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ServerResult<RemoteFile> {
        let buf = BytesMut::with_capacity(CHUNK_SIZE);
        let first_chunk = read_chunk_async(&mut stream, buf)
            .await
            .map_err(ServerError::storage_error)?;

        if first_chunk.len() < CHUNK_SIZE {
            // do a normal upload
            let bucket_name = self.bucket_name();
            let upload_type = UploadType::Simple(google_cloud_storage::http::objects::upload::Media {
                name: Cow::Owned(name.clone()),
                content_type: Cow::Borrowed("application/octet-stream"),
                content_length: Some(first_chunk.len() as u64),
            });

            let upload_req = UploadObjectRequest {
                bucket: bucket_name,
                ..Default::default()
            };

            let _object = self
                .client
                .upload_object(&upload_req, first_chunk.to_vec(), &upload_type)
                .await
                .map_err(ServerError::storage_error)?;

            tracing::debug!("Uploaded object: {}", name);

            return Ok(RemoteFile::Gcs(GcsRemoteFile {
                bucket: self.config.bucket.clone(),
                object: name,
                project_id: self.config.project_id.clone(),
            }));
        }

        // For larger files, we need to read all data since GCS SDK doesn't support streaming uploads
        // This is a limitation of the current GCS SDK compared to S3
        let mut all_data = Vec::from(first_chunk.as_ref());
        loop {
            let buf = BytesMut::with_capacity(CHUNK_SIZE);
            let chunk = read_chunk_async(&mut stream, buf)
                .await
                .map_err(ServerError::storage_error)?;

            if chunk.is_empty() {
                break;
            }

            all_data.extend_from_slice(&chunk);
        }

        let bucket_name = self.bucket_name();
        let upload_type = UploadType::Simple(google_cloud_storage::http::objects::upload::Media {
            name: Cow::Owned(name.clone()),
            content_type: Cow::Borrowed("application/octet-stream"),
            content_length: Some(all_data.len() as u64),
        });

        let upload_req = UploadObjectRequest {
            bucket: bucket_name,
            ..Default::default()
        };

        let _object = self
            .client
            .upload_object(&upload_req, all_data, &upload_type)
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("Uploaded object: {}", name);

        Ok(RemoteFile::Gcs(GcsRemoteFile {
            bucket: self.config.bucket.clone(),
            object: name,
            project_id: self.config.project_id.clone(),
        }))
    }

    async fn delete_file(&self, name: String) -> ServerResult<()> {
        let bucket_name = self.bucket_name();
        let delete_req = DeleteObjectRequest {
            bucket: bucket_name,
            object: name.clone(),
            ..Default::default()
        };

        self.client
            .delete_object(&delete_req)
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file: deleted {}", name);

        Ok(())
    }

    async fn delete_file_db(&self, file: &RemoteFile) -> ServerResult<()> {
        let gcs_file = Self::get_gcs_file_from_db_ref(file)?;
        let bucket_name = Self::bucket_name_for_file(gcs_file);

        let delete_req = DeleteObjectRequest {
            bucket: bucket_name,
            object: gcs_file.object.clone(),
            ..Default::default()
        };

        self.client
            .delete_object(&delete_req)
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file_db: deleted {}", gcs_file.object);

        Ok(())
    }

    async fn download_file(&self, name: String, prefer_stream: bool) -> ServerResult<Download> {
        let bucket_name = self.bucket_name();
        self.get_download(&bucket_name, &name, prefer_stream).await
    }

    async fn download_file_db(
        &self,
        file: &RemoteFile,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let gcs_file = Self::get_gcs_file_from_db_ref(file)?;
        let bucket_name = Self::bucket_name_for_file(gcs_file);

        self.get_download(&bucket_name, &gcs_file.object, prefer_stream)
            .await
    }

    async fn make_db_reference(&self, name: String) -> ServerResult<RemoteFile> {
        Ok(RemoteFile::Gcs(GcsRemoteFile {
            bucket: self.config.bucket.clone(),
            object: name,
            project_id: self.config.project_id.clone(),
        }))
    }
}
