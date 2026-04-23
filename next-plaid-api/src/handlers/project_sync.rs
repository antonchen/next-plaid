//! Staged project sync upload handlers for MCP project uploads.

use std::collections::{HashMap, HashSet};
use std::fs as std_fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path as AxumPath, State},
    http::{
        header::{CONTENT_LENGTH, CONTENT_TYPE},
        HeaderMap,
    },
    Json,
};
use futures_util::StreamExt;
use next_plaid::{filtering, MmapIndex};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio::task;
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::handlers::documents::{
    commit_embeddings_batch_after_repair_locked, get_index_write_lock, max_batch_documents,
    prepare_update_with_encoding_batch, repair_index_db_sync, run_preflight_repair_locked,
    verify_index_db_sync_locked, IndexDbSyncCheck,
};
use crate::models::{
    ErrorResponse, ProjectSyncCreateJobRequest, ProjectSyncCreateJobResponse,
    ProjectSyncFileCheckItem, ProjectSyncFileEntryResponse, ProjectSyncFilesCheckRequest,
    ProjectSyncFilesCheckResponse, ProjectSyncFilesResponse, ProjectSyncJobResponse,
    ProjectSyncJobStatus, UpdateWithEncodingRequest,
};
use crate::state::AppState;
use crate::PrettyJson;

const PROJECT_SYNC_CONTENT_TYPE: &str = "application/x-ndjson";
const DEFAULT_MAX_INGEST_REQUEST_BYTES: u64 = 100 * 1024 * 1024;
const DEFAULT_MAX_PENDING_INGEST_BYTES: u64 = 1024 * 1024 * 1024;
const RETRY_AFTER_SECONDS_HINT: u64 = 5;
const PROJECT_SYNC_UPLOAD_CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(50);
const PROJECT_SYNC_UPLOAD_IDLE_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const PROJECT_SYNC_STALE_JOB_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const PROJECT_SYNC_UPLOAD_PROGRESS_UPDATE_INTERVAL_MS: u64 = 5 * 1000;
const PROJECT_SYNC_UPLOAD_PROGRESS_UPDATE_BYTES: u64 = 1024 * 1024;
const PROJECT_SYNC_CHUNKER_VERSION: u32 = 1;
const PROJECT_SYNC_MAX_LINES_PER_BLOB: usize = 200;

static CREATE_JOB_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();
static JOB_LOCKS: OnceLock<std::sync::Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
static PENDING_BYTES_BY_ROOT: OnceLock<std::sync::Mutex<HashMap<String, u64>>> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
struct ProjectSyncJobManifest {
    job_id: String,
    index_name: String,
    status: ProjectSyncJobStatus,
    declared_bytes: u64,
    uploaded_bytes: u64,
    content_type: String,
    reserved_bytes: bool,
    error: Option<String>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum ProjectSyncRecordOp {
    #[default]
    Upsert,
    Delete,
}

#[derive(Debug, Deserialize)]
struct ProjectSyncUploadRecord {
    #[serde(default)]
    op: ProjectSyncRecordOp,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    relative_path: Option<String>,
    #[serde(default)]
    content_hash: Option<String>,
    #[serde(default)]
    chunk_index: Option<usize>,
    #[serde(default)]
    chunk_count: Option<usize>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSyncUpsertRecord {
    source_path: String,
    relative_path: String,
    content: String,
    language: Option<String>,
    metadata: Option<serde_json::Value>,
    content_hash: Option<String>,
    chunk_index: Option<usize>,
    chunk_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSyncUpsertFile {
    relative_path: String,
    content_hash: Option<String>,
    language: String,
    chunk_count: usize,
    records: Vec<ProjectSyncUpsertRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSyncPlan {
    upsert_files: Vec<ProjectSyncUpsertFile>,
    delete_relative_paths: Vec<String>,
    record_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectSyncExecutablePlan {
    changed_upsert_files: Vec<ProjectSyncUpsertFile>,
    skipped_unchanged_files: Vec<ProjectSyncUpsertFile>,
    delete_relative_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProjectSyncSpoolState {
    final_bytes: Option<u64>,
    temp_bytes: Option<u64>,
}

impl ProjectSyncSpoolState {
    fn has_any_spool(self) -> bool {
        self.final_bytes.is_some() || self.temp_bytes.is_some()
    }

    fn has_exact_final_only(self, expected_bytes: u64) -> bool {
        self.final_bytes == Some(expected_bytes) && self.temp_bytes.is_none()
    }

    fn has_exact_temp_only(self, expected_bytes: u64) -> bool {
        self.temp_bytes == Some(expected_bytes) && self.final_bytes.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectSyncSpoolFix {
    None,
    PromoteTempToFinal,
    DeleteAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectSyncRecoveryAction {
    Persist {
        manifest: ProjectSyncJobManifest,
        requeue: bool,
        spool_fix: ProjectSyncSpoolFix,
    },
    Quarantine {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProjectSyncRecoveredJob {
    reserved_bytes: u64,
    requeue: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectSyncUploadResult {
    Uploaded { uploaded_bytes: u64 },
    Cancelled,
}

async fn verify_project_sync_postflight_locked(
    index_path: &str,
) -> Result<IndexDbSyncCheck, String> {
    verify_index_db_sync_locked(index_path).await
}

fn repair_project_sync_postflight_locked(index_path: &str) -> Result<bool, String> {
    repair_index_db_sync(index_path)
}

async fn run_project_sync_postflight_locked(index_path: &str) -> ApiResult<bool> {
    let postflight_check = verify_project_sync_postflight_locked(index_path)
        .await
        .map_err(|error| {
            ApiError::Internal(format!("Index/DB sync postflight check failed: {}", error))
        })?;
    if postflight_check.in_sync {
        return Ok(false);
    }

    repair_project_sync_postflight_locked(index_path).map_err(|error| {
        ApiError::Internal(format!("Index/DB sync postflight repair failed: {}", error))
    })?;

    let postflight_verify = verify_project_sync_postflight_locked(index_path)
        .await
        .map_err(|error| {
            ApiError::Internal(format!("Index/DB sync re-verify failed: {}", error))
        })?;
    if !postflight_verify.in_sync {
        return Err(ApiError::Internal(format!(
            "Index/DB mismatch persists after postflight repair: index_count={} db_count={}",
            postflight_verify.index_count, postflight_verify.db_count
        )));
    }

    Ok(true)
}

pub(crate) fn max_ingest_request_bytes() -> u64 {
    static VALUE: OnceLock<u64> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("MAX_INGEST_REQUEST_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_INGEST_REQUEST_BYTES)
    })
}

fn max_pending_ingest_bytes() -> u64 {
    static VALUE: OnceLock<u64> = OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("MAX_PENDING_INGEST_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_PENDING_INGEST_BYTES)
    })
}

fn create_job_lock() -> Arc<Mutex<()>> {
    CREATE_JOB_LOCK
        .get_or_init(|| Arc::new(Mutex::new(())))
        .clone()
}

fn job_lock(job_id: &str) -> Arc<Mutex<()>> {
    let locks = JOB_LOCKS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = locks
        .lock()
        .expect("JOB_LOCKS mutex poisoned - a task panicked while holding this lock");
    if let Some(lock) = guard.get(job_id).and_then(Weak::upgrade) {
        return lock;
    }

    guard.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    guard.insert(job_id.to_string(), Arc::downgrade(&lock));
    lock
}

fn pending_bytes_map() -> &'static std::sync::Mutex<HashMap<String, u64>> {
    PENDING_BYTES_BY_ROOT.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn pending_bytes_key(state: &AppState) -> String {
    project_sync_jobs_path(state).to_string_lossy().to_string()
}

fn pending_ingest_bytes(state: &AppState) -> u64 {
    let guard = pending_bytes_map()
        .lock()
        .expect("PENDING_BYTES_BY_ROOT mutex poisoned");
    guard.get(&pending_bytes_key(state)).copied().unwrap_or(0)
}

fn set_pending_ingest_bytes(state: &AppState, bytes: u64) {
    let mut guard = pending_bytes_map()
        .lock()
        .expect("PENDING_BYTES_BY_ROOT mutex poisoned");
    guard.insert(pending_bytes_key(state), bytes);
}

fn reserve_pending_ingest_bytes(state: &AppState, bytes: u64) {
    let mut guard = pending_bytes_map()
        .lock()
        .expect("PENDING_BYTES_BY_ROOT mutex poisoned");
    let entry = guard.entry(pending_bytes_key(state)).or_insert(0);
    *entry = entry.saturating_add(bytes);
}

fn release_pending_ingest_bytes(state: &AppState, bytes: u64) {
    let mut guard = pending_bytes_map()
        .lock()
        .expect("PENDING_BYTES_BY_ROOT mutex poisoned");
    let entry = guard.entry(pending_bytes_key(state)).or_insert(0);
    *entry = entry.saturating_sub(bytes);
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn project_sync_jobs_path(state: &AppState) -> PathBuf {
    state.config.index_dir.join("_project_sync_jobs")
}

fn job_dir(state: &AppState, job_id: &str) -> PathBuf {
    project_sync_jobs_path(state).join(job_id)
}

fn manifest_path(state: &AppState, job_id: &str) -> PathBuf {
    job_dir(state, job_id).join("manifest.json")
}

fn spool_path(state: &AppState, job_id: &str) -> PathBuf {
    job_dir(state, job_id).join("spool.ndjson")
}

fn temp_spool_path(state: &AppState, job_id: &str) -> PathBuf {
    job_dir(state, job_id).join("spool.ndjson.tmp")
}

fn snapshot_path(state: &AppState, job_id: &str) -> PathBuf {
    job_dir(state, job_id).join("index_snapshot")
}

fn sanitize_marker_segment(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => character,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "index".to_string()
    } else {
        sanitized
    }
}

fn dirty_marker_path(state: &AppState, index_name: &str, job_id: &str) -> PathBuf {
    project_sync_jobs_path(state).join("_dirty").join(format!(
        "{}-{}.json",
        sanitize_marker_segment(index_name),
        job_id
    ))
}

fn manifest_to_response(manifest: &ProjectSyncJobManifest) -> ProjectSyncJobResponse {
    ProjectSyncJobResponse {
        job_id: manifest.job_id.clone(),
        index_name: manifest.index_name.clone(),
        status: manifest.status,
        declared_bytes: manifest.declared_bytes,
        uploaded_bytes: manifest.uploaded_bytes,
        content_type: manifest.content_type.clone(),
        error: manifest.error.clone(),
    }
}

fn is_terminal_status(status: ProjectSyncJobStatus) -> bool {
    matches!(
        status,
        ProjectSyncJobStatus::Completed
            | ProjectSyncJobStatus::Failed
            | ProjectSyncJobStatus::Cancelled
            | ProjectSyncJobStatus::Expired
    )
}

fn is_expirable_status(status: ProjectSyncJobStatus) -> bool {
    matches!(
        status,
        ProjectSyncJobStatus::Created
            | ProjectSyncJobStatus::Uploading
            | ProjectSyncJobStatus::Uploaded
            | ProjectSyncJobStatus::Queued
    )
}

fn is_project_sync_manifest_expired(manifest: &ProjectSyncJobManifest, now_ms: u64) -> bool {
    is_expirable_status(manifest.status)
        && now_ms.saturating_sub(manifest.updated_at_ms) > PROJECT_SYNC_STALE_JOB_TIMEOUT_MS
}

fn project_sync_expired_message() -> String {
    format!(
        "project_sync job expired after {} seconds without progress",
        PROJECT_SYNC_STALE_JOB_TIMEOUT_MS / 1000
    )
}

fn is_cancellation_requested(status: ProjectSyncJobStatus) -> bool {
    matches!(
        status,
        ProjectSyncJobStatus::Cancelling | ProjectSyncJobStatus::Cancelled
    )
}

fn upload_cancelled_conflict(job_id: &str) -> ApiError {
    ApiError::Conflict(format!(
        "project_sync job '{}' was cancelled during upload",
        job_id
    ))
}

fn upload_completion_conflict(job_id: &str, status: ProjectSyncJobStatus) -> ApiError {
    ApiError::Conflict(format!(
        "project_sync job '{}' cannot accept upload completion from status {:?}",
        job_id, status
    ))
}

fn normalize_content_type(value: &str) -> &str {
    value.split(';').next().map(str::trim).unwrap_or(value)
}

fn validate_project_sync_content_type(content_type: &str) -> ApiResult<()> {
    let normalized = normalize_content_type(content_type);
    if normalized != PROJECT_SYNC_CONTENT_TYPE {
        return Err(ApiError::BadRequest(format!(
            "project_sync content_type must be '{}', got '{}'",
            PROJECT_SYNC_CONTENT_TYPE, content_type
        )));
    }
    Ok(())
}

fn parse_content_length(headers: &HeaderMap) -> ApiResult<u64> {
    let value = headers
        .get(CONTENT_LENGTH)
        .ok_or_else(|| ApiError::BadRequest("Missing Content-Length header".to_string()))?;
    let text = value
        .to_str()
        .map_err(|error| ApiError::BadRequest(format!("Invalid Content-Length: {}", error)))?;
    text.parse::<u64>()
        .map_err(|error| ApiError::BadRequest(format!("Invalid Content-Length: {}", error)))
}

fn request_content_type(headers: &HeaderMap) -> ApiResult<String> {
    let value = headers
        .get(CONTENT_TYPE)
        .ok_or_else(|| ApiError::BadRequest("Missing Content-Type header".to_string()))?;
    value
        .to_str()
        .map(str::to_string)
        .map_err(|error| ApiError::BadRequest(format!("Invalid Content-Type: {}", error)))
}

fn backlog_details(
    required_bytes: u64,
    pending_ingest_bytes: u64,
    max_pending_ingest_bytes: u64,
) -> serde_json::Value {
    let available_bytes = max_pending_ingest_bytes.saturating_sub(pending_ingest_bytes);
    serde_json::json!({
        "required_bytes": required_bytes,
        "available_bytes": available_bytes,
        "pending_ingest_bytes": pending_ingest_bytes,
        "max_pending_ingest_bytes": max_pending_ingest_bytes,
        "retry_after_seconds_hint": RETRY_AFTER_SECONDS_HINT
    })
}

fn content_too_large_details(
    required_bytes: u64,
    max_ingest_request_bytes: u64,
) -> serde_json::Value {
    serde_json::json!({
        "required_bytes": required_bytes,
        "max_ingest_request_bytes": max_ingest_request_bytes
    })
}

fn project_sync_upload_idle_timeout() -> Duration {
    Duration::from_millis(PROJECT_SYNC_UPLOAD_IDLE_TIMEOUT_MS)
}

fn project_sync_upload_timeout_message(idle_timeout: Duration) -> String {
    format!(
        "project_sync upload timed out after {} seconds without receiving request body data",
        idle_timeout.as_secs()
    )
}

async fn read_manifest(state: &AppState, job_id: &str) -> ApiResult<ProjectSyncJobManifest> {
    let path = manifest_path(state, job_id);
    let contents = fs::read(&path).await.map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => ApiError::ProjectSyncJobNotFound(job_id.to_string()),
        _ => ApiError::Internal(format!("Failed to read project_sync manifest: {}", error)),
    })?;
    serde_json::from_slice::<ProjectSyncJobManifest>(&contents)
        .map_err(|error| ApiError::Internal(format!("Invalid project_sync manifest: {}", error)))
}

async fn write_manifest(state: &AppState, manifest: &ProjectSyncJobManifest) -> ApiResult<()> {
    let dir = job_dir(state, &manifest.job_id);
    fs::create_dir_all(&dir).await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to create project_sync job directory: {}",
            error
        ))
    })?;

    let path = manifest_path(state, &manifest.job_id);
    let tmp_path = dir.join("manifest.json.tmp");
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| ApiError::Internal(format!("Failed to serialize manifest: {}", error)))?;

    fs::write(&tmp_path, bytes)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to write manifest tmp: {}", error)))?;
    fs::rename(&tmp_path, &path)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to persist manifest: {}", error)))
}

async fn update_manifest_status(
    state: &AppState,
    job_id: &str,
    status: ProjectSyncJobStatus,
    error: Option<String>,
) -> ApiResult<ProjectSyncJobManifest> {
    let mut manifest = read_manifest(state, job_id).await?;
    manifest.status = status;
    manifest.error = error;
    manifest.updated_at_ms = now_epoch_ms();
    write_manifest(state, &manifest).await?;
    Ok(manifest)
}

async fn release_reserved_bytes(
    state: &AppState,
    manifest: &mut ProjectSyncJobManifest,
) -> ApiResult<()> {
    if manifest.reserved_bytes {
        release_pending_ingest_bytes(state, manifest.declared_bytes);
        manifest.reserved_bytes = false;
    }
    manifest.updated_at_ms = now_epoch_ms();
    write_manifest(state, manifest).await
}

async fn cleanup_project_sync_spool(state: &AppState, job_id: &str) -> ApiResult<()> {
    remove_file_if_exists(&temp_spool_path(state, job_id)).await?;
    remove_file_if_exists(&spool_path(state, job_id)).await
}

async fn persist_terminal_project_sync_manifest(
    state: &AppState,
    manifest: &mut ProjectSyncJobManifest,
    status: ProjectSyncJobStatus,
    error: Option<String>,
) -> ApiResult<()> {
    manifest.status = status;
    manifest.error = error;
    cleanup_project_sync_spool(state, &manifest.job_id).await?;
    release_reserved_bytes(state, manifest).await
}

async fn remove_file_if_exists(path: &Path) -> ApiResult<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ApiError::Internal(format!(
            "Failed to remove file '{}': {}",
            path.display(),
            error
        ))),
    }
}

async fn inspect_spool_state(state: &AppState, job_id: &str) -> ApiResult<ProjectSyncSpoolState> {
    let final_bytes = file_len_if_exists(&spool_path(state, job_id)).await?;
    let temp_bytes = file_len_if_exists(&temp_spool_path(state, job_id)).await?;
    Ok(ProjectSyncSpoolState {
        final_bytes,
        temp_bytes,
    })
}

async fn file_len_if_exists(path: &Path) -> ApiResult<Option<u64>> {
    match fs::metadata(path).await {
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(ApiError::Internal(format!(
            "Failed to inspect project_sync spool file '{}': {}",
            path.display(),
            error
        ))),
    }
}

async fn apply_spool_fix(
    state: &AppState,
    job_id: &str,
    spool_fix: ProjectSyncSpoolFix,
) -> ApiResult<()> {
    match spool_fix {
        ProjectSyncSpoolFix::None => Ok(()),
        ProjectSyncSpoolFix::PromoteTempToFinal => {
            let temp_path = temp_spool_path(state, job_id);
            let final_path = spool_path(state, job_id);
            remove_file_if_exists(&final_path).await?;
            fs::rename(&temp_path, &final_path).await.map_err(|error| {
                ApiError::Internal(format!(
                    "Failed to promote project_sync temp spool to final: {}",
                    error
                ))
            })
        }
        ProjectSyncSpoolFix::DeleteAll => cleanup_project_sync_spool(state, job_id).await,
    }
}

fn recover_manifest_for_restart(
    mut manifest: ProjectSyncJobManifest,
    spool_state: ProjectSyncSpoolState,
    recovered_at_ms: u64,
) -> ProjectSyncRecoveryAction {
    manifest.updated_at_ms = recovered_at_ms;

    if is_terminal_status(manifest.status) {
        manifest.reserved_bytes = false;
        return ProjectSyncRecoveryAction::Persist {
            manifest,
            requeue: false,
            spool_fix: ProjectSyncSpoolFix::DeleteAll,
        };
    }

    if is_project_sync_manifest_expired(&manifest, recovered_at_ms) {
        manifest.status = ProjectSyncJobStatus::Expired;
        manifest.error = Some(project_sync_expired_message());
        manifest.reserved_bytes = false;
        return ProjectSyncRecoveryAction::Persist {
            manifest,
            requeue: false,
            spool_fix: ProjectSyncSpoolFix::DeleteAll,
        };
    }

    match manifest.status {
        ProjectSyncJobStatus::Created | ProjectSyncJobStatus::Uploading => {
            if spool_state.has_exact_final_only(manifest.declared_bytes) {
                manifest.status = ProjectSyncJobStatus::Uploaded;
                manifest.uploaded_bytes = manifest.declared_bytes;
                manifest.error = None;
                return ProjectSyncRecoveryAction::Persist {
                    manifest,
                    requeue: false,
                    spool_fix: ProjectSyncSpoolFix::None,
                };
            }
            if spool_state.has_exact_temp_only(manifest.declared_bytes) {
                manifest.status = ProjectSyncJobStatus::Uploaded;
                manifest.uploaded_bytes = manifest.declared_bytes;
                manifest.error = None;
                return ProjectSyncRecoveryAction::Persist {
                    manifest,
                    requeue: false,
                    spool_fix: ProjectSyncSpoolFix::PromoteTempToFinal,
                };
            }
            manifest.status = ProjectSyncJobStatus::Created;
            manifest.uploaded_bytes = 0;
            if spool_state.has_any_spool() {
                manifest.error =
                    Some("Interrupted upload was discarded during recovery".to_string());
            }
            ProjectSyncRecoveryAction::Persist {
                manifest,
                requeue: false,
                spool_fix: if spool_state.has_any_spool() {
                    ProjectSyncSpoolFix::DeleteAll
                } else {
                    ProjectSyncSpoolFix::None
                },
            }
        }
        ProjectSyncJobStatus::Uploaded | ProjectSyncJobStatus::Queued => {
            if spool_state.has_exact_final_only(manifest.declared_bytes) {
                manifest.uploaded_bytes = manifest.declared_bytes;
                return ProjectSyncRecoveryAction::Persist {
                    requeue: manifest.status == ProjectSyncJobStatus::Queued,
                    manifest,
                    spool_fix: ProjectSyncSpoolFix::None,
                };
            }
            if spool_state.has_exact_temp_only(manifest.declared_bytes) {
                manifest.uploaded_bytes = manifest.declared_bytes;
                return ProjectSyncRecoveryAction::Persist {
                    requeue: manifest.status == ProjectSyncJobStatus::Queued,
                    manifest,
                    spool_fix: ProjectSyncSpoolFix::PromoteTempToFinal,
                };
            }
            manifest.status = ProjectSyncJobStatus::Failed;
            manifest.error =
                Some("project_sync spool is missing or incomplete after restart".to_string());
            manifest.reserved_bytes = false;
            ProjectSyncRecoveryAction::Persist {
                manifest,
                requeue: false,
                spool_fix: ProjectSyncSpoolFix::DeleteAll,
            }
        }
        ProjectSyncJobStatus::Running => ProjectSyncRecoveryAction::Quarantine {
            reason: "Running jobs must be recovered via snapshot-aware recovery".to_string(),
        },
        ProjectSyncJobStatus::Cancelling => ProjectSyncRecoveryAction::Quarantine {
            reason: "Cancelling jobs must be recovered via snapshot-aware recovery".to_string(),
        },
        ProjectSyncJobStatus::Completed
        | ProjectSyncJobStatus::Failed
        | ProjectSyncJobStatus::Cancelled
        | ProjectSyncJobStatus::Expired => ProjectSyncRecoveryAction::Quarantine {
            reason: "Unexpected terminal state in non-terminal recovery branch".to_string(),
        },
    }
}

async fn quarantine_project_sync_path(
    state: &AppState,
    path: &Path,
    entry_name: &str,
    reason: &str,
) -> ApiResult<()> {
    let corrupt_root = project_sync_jobs_path(state).join("_corrupt");
    fs::create_dir_all(&corrupt_root).await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to create project_sync corrupt directory: {}",
            error
        ))
    })?;
    let quarantine_name = format!("{}-{}", now_epoch_ms(), entry_name);
    let destination = corrupt_root.join(quarantine_name);
    fs::rename(path, &destination).await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to quarantine project_sync path '{}': {}",
            path.display(),
            error
        ))
    })?;
    fs::write(destination.join("reason.txt"), reason.as_bytes())
        .await
        .map_err(|error| {
            ApiError::Internal(format!(
                "Failed to persist project_sync quarantine reason: {}",
                error
            ))
        })?;
    Ok(())
}

async fn recover_project_sync_job_dir(
    state: &Arc<AppState>,
    entry_name: &str,
    entry_path: &Path,
) -> ApiResult<Option<ProjectSyncRecoveredJob>> {
    let manifest_path = entry_path.join("manifest.json");
    let manifest_bytes = match fs::read(&manifest_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            quarantine_project_sync_path(
                state,
                entry_path,
                entry_name,
                "Missing project_sync manifest.json",
            )
            .await?;
            return Ok(None);
        }
        Err(error) => {
            return Err(ApiError::Internal(format!(
                "Failed to read project_sync manifest during recovery: {}",
                error
            )));
        }
    };

    let manifest = match serde_json::from_slice::<ProjectSyncJobManifest>(&manifest_bytes) {
        Ok(manifest) => manifest,
        Err(error) => {
            quarantine_project_sync_path(
                state,
                entry_path,
                entry_name,
                &format!("Invalid project_sync manifest.json: {}", error),
            )
            .await?;
            return Ok(None);
        }
    };

    if manifest.job_id != entry_name {
        quarantine_project_sync_path(
            state,
            entry_path,
            entry_name,
            &format!(
                "Manifest job_id '{}' does not match directory '{}'",
                manifest.job_id, entry_name
            ),
        )
        .await?;
        return Ok(None);
    }

    let recovered_at_ms = now_epoch_ms();
    if manifest.status == ProjectSyncJobStatus::Running
        || manifest.status == ProjectSyncJobStatus::Cancelling
    {
        return recover_interrupted_project_sync_job(state, manifest, recovered_at_ms).await;
    }

    let spool_state = inspect_spool_state(state, entry_name).await?;
    match recover_manifest_for_restart(manifest, spool_state, recovered_at_ms) {
        ProjectSyncRecoveryAction::Persist {
            manifest,
            requeue,
            spool_fix,
        } => {
            apply_spool_fix(state, &manifest.job_id, spool_fix).await?;
            write_manifest(state, &manifest).await?;
            Ok(Some(ProjectSyncRecoveredJob {
                reserved_bytes: if manifest.reserved_bytes {
                    manifest.declared_bytes
                } else {
                    0
                },
                requeue,
            }))
        }
        ProjectSyncRecoveryAction::Quarantine { reason } => {
            quarantine_project_sync_path(state, entry_path, entry_name, &reason).await?;
            Ok(None)
        }
    }
}

async fn recover_interrupted_project_sync_job(
    state: &Arc<AppState>,
    manifest: ProjectSyncJobManifest,
    recovered_at_ms: u64,
) -> ApiResult<Option<ProjectSyncRecoveredJob>> {
    let snapshot = snapshot_path(state, &manifest.job_id);
    if snapshot.exists() {
        if let Err(rollback_error) =
            restore_project_sync_snapshot_locked(state.clone(), &manifest.index_name, &snapshot)
                .await
        {
            let dirty_result = mark_project_sync_dirty(
                state,
                &manifest.index_name,
                &manifest.job_id,
                "Interrupted during startup recovery",
                &rollback_error,
            )
            .await;
            let error = match dirty_result {
                Ok(()) => format!(
                    "Job was interrupted during indexing and snapshot restore failed: {}; index marked dirty",
                    rollback_error
                ),
                Err(dirty_error) => format!(
                    "Job was interrupted during indexing and snapshot restore failed: {}; failed to persist dirty marker: {}",
                    rollback_error, dirty_error
                ),
            };
            let mut failed_manifest = manifest;
            failed_manifest.updated_at_ms = recovered_at_ms;
            persist_terminal_project_sync_manifest(
                state,
                &mut failed_manifest,
                ProjectSyncJobStatus::Failed,
                Some(error),
            )
            .await?;
            return Ok(Some(ProjectSyncRecoveredJob {
                reserved_bytes: 0,
                requeue: false,
            }));
        }

        if let Err(cleanup_error) = remove_project_sync_snapshot(&snapshot).await {
            tracing::warn!(
                job_id = %manifest.job_id,
                error = %cleanup_error,
                "project_sync.recovery.snapshot_cleanup_failed"
            );
        }

        let mut recovered_manifest = manifest;
        let recovered_status = if recovered_manifest.status == ProjectSyncJobStatus::Cancelling {
            ProjectSyncJobStatus::Cancelled
        } else {
            ProjectSyncJobStatus::Failed
        };
        let recovered_error = if recovered_status == ProjectSyncJobStatus::Cancelled {
            "Cancelled by client".to_string()
        } else {
            "Job was interrupted during indexing and rolled back from snapshot".to_string()
        };
        recovered_manifest.updated_at_ms = recovered_at_ms;
        persist_terminal_project_sync_manifest(
            state,
            &mut recovered_manifest,
            recovered_status,
            Some(recovered_error),
        )
        .await?;
        return Ok(Some(ProjectSyncRecoveredJob {
            reserved_bytes: 0,
            requeue: false,
        }));
    }

    let dirty_result = mark_project_sync_dirty(
        state,
        &manifest.index_name,
        &manifest.job_id,
        "Interrupted during startup recovery",
        "Snapshot is missing",
    )
    .await;
    let error = match dirty_result {
        Ok(()) => {
            "Job was interrupted during indexing and snapshot is missing; index marked dirty"
                .to_string()
        }
        Err(dirty_error) => format!(
            "Job was interrupted during indexing and snapshot is missing; failed to persist dirty marker: {}",
            dirty_error
        ),
    };
    let mut failed_manifest = manifest;
    failed_manifest.updated_at_ms = recovered_at_ms;
    persist_terminal_project_sync_manifest(
        state,
        &mut failed_manifest,
        ProjectSyncJobStatus::Failed,
        Some(error),
    )
    .await?;
    Ok(Some(ProjectSyncRecoveredJob {
        reserved_bytes: 0,
        requeue: false,
    }))
}

pub async fn recover_project_sync_jobs(state: Arc<AppState>) -> ApiResult<()> {
    let jobs_root = project_sync_jobs_path(&state);
    fs::create_dir_all(jobs_root.join("_corrupt"))
        .await
        .map_err(|error| {
            ApiError::Internal(format!(
                "Failed to create project_sync recovery directories: {}",
                error
            ))
        })?;

    let mut pending_bytes: u64 = 0;
    let mut requeue_job_ids: Vec<String> = Vec::new();
    let mut entries = fs::read_dir(&jobs_root).await.map_err(|error| {
        ApiError::Internal(format!("Failed to scan project_sync jobs: {}", error))
    })?;

    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        ApiError::Internal(format!("Failed to read project_sync entry: {}", error))
    })? {
        let entry_name = entry.file_name().to_string_lossy().to_string();
        if entry_name == "_corrupt" || entry_name == "_dirty" {
            continue;
        }

        let entry_path = entry.path();
        let file_type = entry.file_type().await.map_err(|error| {
            ApiError::Internal(format!(
                "Failed to inspect project_sync entry type: {}",
                error
            ))
        })?;
        if !file_type.is_dir() {
            quarantine_project_sync_path(
                &state,
                &entry_path,
                &entry_name,
                "Unexpected non-directory entry in project_sync jobs root",
            )
            .await?;
            continue;
        }

        if let Some(recovered_job) =
            recover_project_sync_job_dir(&state, &entry_name, &entry_path).await?
        {
            pending_bytes = pending_bytes.saturating_add(recovered_job.reserved_bytes);
            if recovered_job.requeue {
                requeue_job_ids.push(entry_name);
            }
        }
    }

    set_pending_ingest_bytes(&state, pending_bytes);

    for job_id in &requeue_job_ids {
        let worker_state = state.clone();
        let worker_job_id = job_id.clone();
        tokio::spawn(async move {
            run_project_sync_job(worker_state, worker_job_id).await;
        });
    }

    tracing::info!(
        pending_ingest_bytes = pending_bytes,
        requeued_jobs = requeue_job_ids.len(),
        "project_sync.recovery.completed"
    );

    Ok(())
}

async fn expire_project_sync_job_if_stale(
    state: &AppState,
    job_id: &str,
    now_ms: u64,
) -> ApiResult<()> {
    let lock = job_lock(job_id);
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(state, job_id).await?;
    if !is_project_sync_manifest_expired(&manifest, now_ms) {
        return Ok(());
    }

    persist_terminal_project_sync_manifest(
        state,
        &mut manifest,
        ProjectSyncJobStatus::Expired,
        Some(project_sync_expired_message()),
    )
    .await
}

async fn sweep_expired_project_sync_jobs(state: &AppState, now_ms: u64) -> ApiResult<()> {
    let jobs_root = project_sync_jobs_path(state);
    let mut entries = match fs::read_dir(&jobs_root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ApiError::Internal(format!(
                "Failed to scan project_sync jobs for expiration: {}",
                error
            )))
        }
    };

    while let Some(entry) = entries.next_entry().await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to read project_sync expiration entry: {}",
            error
        ))
    })? {
        let entry_name = entry.file_name().to_string_lossy().to_string();
        if entry_name == "_corrupt" || entry_name == "_dirty" {
            continue;
        }

        let file_type = entry.file_type().await.map_err(|error| {
            ApiError::Internal(format!(
                "Failed to inspect project_sync expiration entry type: {}",
                error
            ))
        })?;
        if !file_type.is_dir() {
            continue;
        }

        expire_project_sync_job_if_stale(state, &entry_name, now_ms).await?;
    }

    Ok(())
}

#[utoipa::path(
    get,
    path = "/indices/{name}/project_sync/files",
    tag = "project_sync",
    params(
        ("name" = String, Path, description = "Index name")
    ),
    responses(
        (status = 200, description = "Project sync file manifest", body = ProjectSyncFilesResponse),
        (status = 404, description = "Index not declared", body = ErrorResponse)
    )
)]
pub async fn get_project_sync_files(
    State(state): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<Json<ProjectSyncFilesResponse>> {
    let index_path = state.index_path(&name);
    if !index_path.join("config.json").exists() {
        return Err(ApiError::IndexNotDeclared(name));
    }
    let index_path_string = index_path.to_string_lossy().to_string();
    let entries = filtering::list_project_files(&index_path_string).map_err(ApiError::NextPlaid)?;
    let config =
        filtering::get_project_manifest_config(&index_path_string).map_err(ApiError::NextPlaid)?;
    let files = entries
        .into_iter()
        .map(project_file_entry_to_response)
        .collect::<Vec<ProjectSyncFileEntryResponse>>();
    Ok(Json(ProjectSyncFilesResponse {
        index_name: name,
        version: filtering::PROJECT_MANIFEST_VERSION,
        chunker_version: config.as_ref().map(|config| config.chunker_version),
        max_lines_per_blob: config.as_ref().map(|config| config.max_lines_per_blob),
        files,
    }))
}

#[utoipa::path(
    post,
    path = "/indices/{name}/project_sync/files/check",
    tag = "project_sync",
    params(
        ("name" = String, Path, description = "Index name")
    ),
    request_body = ProjectSyncFilesCheckRequest,
    responses(
        (status = 200, description = "Project sync file hash comparison", body = ProjectSyncFilesCheckResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 404, description = "Index not declared", body = ErrorResponse)
    )
)]
pub async fn check_project_sync_files(
    State(state): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
    request: Result<Json<ProjectSyncFilesCheckRequest>, JsonRejection>,
) -> ApiResult<Json<ProjectSyncFilesCheckResponse>> {
    let Json(request) = request.map_err(|error| ApiError::BadRequest(error.body_text()))?;
    let index_path = state.index_path(&name);
    if !index_path.join("config.json").exists() {
        return Err(ApiError::IndexNotDeclared(name));
    }
    validate_project_sync_file_check_items(&request.files)?;

    let index_path_string = index_path.to_string_lossy().to_string();
    let config =
        filtering::get_project_manifest_config(&index_path_string).map_err(ApiError::NextPlaid)?;
    let manifest_mismatch = !matches!(
        config,
        Some(filtering::ProjectManifestConfig {
            version: filtering::PROJECT_MANIFEST_VERSION,
            chunker_version,
            max_lines_per_blob
        }) if chunker_version == request.chunker_version
            && max_lines_per_blob == request.max_lines_per_blob
    );
    let relative_paths = request
        .files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<String>>();
    let remote_files = filtering::get_project_files(&index_path_string, &relative_paths)
        .map_err(ApiError::NextPlaid)?;
    let remote_by_path = remote_files
        .into_iter()
        .map(|entry| (entry.relative_path.clone(), entry))
        .collect::<HashMap<String, filtering::ProjectFileEntry>>();

    let mut unchanged = Vec::new();
    let mut changed = Vec::new();
    let mut missing = Vec::new();
    for item in &request.files {
        match remote_by_path.get(&item.relative_path) {
            Some(entry) if !manifest_mismatch && entry.content_hash == item.content_hash => {
                unchanged.push(item.relative_path.clone());
            }
            Some(_) => changed.push(item.relative_path.clone()),
            None => missing.push(item.relative_path.clone()),
        }
    }

    Ok(Json(ProjectSyncFilesCheckResponse {
        unchanged,
        changed,
        missing,
        manifest_mismatch,
    }))
}

#[utoipa::path(
    post,
    path = "/indices/{name}/project_sync/jobs",
    tag = "project_sync",
    params(
        ("name" = String, Path, description = "Index name")
    ),
    request_body = ProjectSyncCreateJobRequest,
    responses(
        (status = 200, description = "Project sync job accepted", body = ProjectSyncCreateJobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 404, description = "Index not declared", body = ErrorResponse),
        (status = 413, description = "Upload is too large", body = ErrorResponse),
        (status = 503, description = "Project sync backlog is full", body = ErrorResponse)
    )
)]
pub async fn create_project_sync_job(
    State(state): State<Arc<AppState>>,
    AxumPath(name): AxumPath<String>,
    request: Result<Json<ProjectSyncCreateJobRequest>, JsonRejection>,
) -> ApiResult<Json<ProjectSyncCreateJobResponse>> {
    let Json(request) = request.map_err(|error| ApiError::BadRequest(error.body_text()))?;

    if name.is_empty() {
        return Err(ApiError::BadRequest(
            "Index name cannot be empty".to_string(),
        ));
    }
    if request.declared_bytes == 0 {
        return Err(ApiError::BadRequest(
            "declared_bytes must be greater than 0".to_string(),
        ));
    }
    validate_project_sync_content_type(&request.content_type)?;

    let index_path = state.index_path(&name);
    if !index_path.join("config.json").exists() {
        return Err(ApiError::IndexNotDeclared(name));
    }

    let max_request_bytes = max_ingest_request_bytes();
    if request.declared_bytes > max_request_bytes {
        return Err(ApiError::ContentTooLarge {
            message: format!(
                "declared_bytes {} exceeds MAX_INGEST_REQUEST_BYTES {}",
                request.declared_bytes, max_request_bytes
            ),
            details: content_too_large_details(request.declared_bytes, max_request_bytes),
        });
    }

    let create_lock = create_job_lock();
    let _create_guard = create_lock.lock().await;
    sweep_expired_project_sync_jobs(&state, now_epoch_ms()).await?;

    let max_pending_bytes = max_pending_ingest_bytes();
    let current_pending_bytes = pending_ingest_bytes(&state);
    let available_bytes = max_pending_bytes.saturating_sub(current_pending_bytes);
    if request.declared_bytes > available_bytes {
        return Err(ApiError::ServiceUnavailableDetailed {
            message: "project_sync backlog is full".to_string(),
            details: backlog_details(
                request.declared_bytes,
                current_pending_bytes,
                max_pending_bytes,
            ),
            retry_after_seconds: Some(RETRY_AFTER_SECONDS_HINT),
        });
    }

    let job_id = Uuid::new_v4().to_string();
    let now = now_epoch_ms();
    let manifest = ProjectSyncJobManifest {
        job_id: job_id.clone(),
        index_name: name,
        status: ProjectSyncJobStatus::Created,
        declared_bytes: request.declared_bytes,
        uploaded_bytes: 0,
        content_type: request.content_type,
        reserved_bytes: true,
        error: None,
        created_at_ms: now,
        updated_at_ms: now,
    };

    reserve_pending_ingest_bytes(&state, manifest.declared_bytes);
    if let Err(error) = write_manifest(&state, &manifest).await {
        release_pending_ingest_bytes(&state, manifest.declared_bytes);
        return Err(error);
    }

    Ok(Json(ProjectSyncCreateJobResponse {
        job_id,
        status: ProjectSyncJobStatus::Created,
        declared_bytes: manifest.declared_bytes,
    }))
}

#[utoipa::path(
    put,
    path = "/project_sync/jobs/{job_id}/upload",
    tag = "project_sync",
    params(
        ("job_id" = String, Path, description = "Project sync job id")
    ),
    request_body(content = String, content_type = "application/x-ndjson"),
    responses(
        (status = 200, description = "Project sync upload stored", body = ProjectSyncJobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 408, description = "Project sync upload timed out", body = ErrorResponse),
        (status = 409, description = "Job is not uploadable", body = ErrorResponse)
    )
)]
pub async fn upload_project_sync_job(
    State(state): State<Arc<AppState>>,
    AxumPath(job_id): AxumPath<String>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<PrettyJson<ProjectSyncJobResponse>> {
    upload_project_sync_job_with_idle_timeout(
        state,
        &job_id,
        headers,
        body,
        project_sync_upload_idle_timeout(),
    )
    .await
}

async fn upload_project_sync_job_with_idle_timeout(
    state: Arc<AppState>,
    job_id: &str,
    headers: HeaderMap,
    body: Body,
    idle_timeout: Duration,
) -> ApiResult<PrettyJson<ProjectSyncJobResponse>> {
    let lock = job_lock(job_id);
    let content_type = request_content_type(&headers)?;
    validate_project_sync_content_type(&content_type)?;
    let content_length = parse_content_length(&headers)?;

    let tmp_path = temp_spool_path(&state, job_id);
    let final_path = spool_path(&state, job_id);

    {
        let _guard = lock.lock().await;
        let mut manifest = read_manifest(&state, job_id).await?;

        if manifest.status != ProjectSyncJobStatus::Created {
            return Err(ApiError::Conflict(format!(
                "project_sync job '{}' is not uploadable from status {:?}",
                job_id, manifest.status
            )));
        }
        if content_length != manifest.declared_bytes {
            return Err(ApiError::BadRequest(format!(
                "Content-Length {} does not match declared_bytes {}",
                content_length, manifest.declared_bytes
            )));
        }
        if normalize_content_type(&content_type) != normalize_content_type(&manifest.content_type) {
            return Err(ApiError::BadRequest(format!(
                "Content-Type '{}' does not match job content_type '{}'",
                content_type, manifest.content_type
            )));
        }

        remove_file_if_exists(&tmp_path).await?;
        remove_file_if_exists(&final_path).await?;
        manifest.status = ProjectSyncJobStatus::Uploading;
        manifest.uploaded_bytes = 0;
        manifest.error = None;
        manifest.updated_at_ms = now_epoch_ms();
        write_manifest(&state, &manifest).await?;
    }

    let upload_result = upload_project_sync_body(
        state.clone(),
        job_id,
        body,
        &tmp_path,
        &final_path,
        content_length,
        idle_timeout,
    )
    .await;
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(&state, job_id).await?;

    match upload_result {
        Ok(ProjectSyncUploadResult::Uploaded { uploaded_bytes }) => {
            if manifest.status == ProjectSyncJobStatus::Uploading {
                manifest.status = ProjectSyncJobStatus::Uploaded;
                manifest.uploaded_bytes = uploaded_bytes;
                manifest.error = None;
                manifest.updated_at_ms = now_epoch_ms();
                write_manifest(&state, &manifest).await?;
                return Ok(PrettyJson(manifest_to_response(&manifest)));
            }
            if is_cancellation_requested(manifest.status) {
                manifest.uploaded_bytes = 0;
                persist_terminal_project_sync_manifest(
                    &state,
                    &mut manifest,
                    ProjectSyncJobStatus::Cancelled,
                    Some("Cancelled by client".to_string()),
                )
                .await?;
                return Err(upload_cancelled_conflict(job_id));
            }
            cleanup_project_sync_spool(&state, job_id).await?;
            Err(upload_completion_conflict(job_id, manifest.status))
        }
        Ok(ProjectSyncUploadResult::Cancelled) => {
            manifest.uploaded_bytes = 0;
            persist_terminal_project_sync_manifest(
                &state,
                &mut manifest,
                ProjectSyncJobStatus::Cancelled,
                Some("Cancelled by client".to_string()),
            )
            .await?;
            Err(upload_cancelled_conflict(job_id))
        }
        Err(error) => {
            if manifest.status == ProjectSyncJobStatus::Uploading {
                cleanup_project_sync_spool(&state, job_id).await?;
                manifest.status = ProjectSyncJobStatus::Created;
                manifest.uploaded_bytes = 0;
                manifest.error = Some(error.to_string());
                manifest.updated_at_ms = now_epoch_ms();
                write_manifest(&state, &manifest).await?;
                return Err(error);
            }
            if is_cancellation_requested(manifest.status) {
                manifest.uploaded_bytes = 0;
                persist_terminal_project_sync_manifest(
                    &state,
                    &mut manifest,
                    ProjectSyncJobStatus::Cancelled,
                    Some("Cancelled by client".to_string()),
                )
                .await?;
                return Err(upload_cancelled_conflict(job_id));
            }
            cleanup_project_sync_spool(&state, job_id).await?;
            Err(upload_completion_conflict(job_id, manifest.status))
        }
    }
}

async fn refresh_project_sync_upload_progress(
    state: &AppState,
    job_id: &str,
    uploaded_bytes: u64,
) -> ApiResult<()> {
    let lock = job_lock(job_id);
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(state, job_id).await?;
    if manifest.status != ProjectSyncJobStatus::Uploading {
        return Ok(());
    }
    manifest.uploaded_bytes = uploaded_bytes;
    manifest.updated_at_ms = now_epoch_ms();
    write_manifest(state, &manifest).await
}

async fn upload_project_sync_body(
    state: Arc<AppState>,
    job_id: &str,
    body: Body,
    tmp_path: &Path,
    final_path: &Path,
    declared_bytes: u64,
    idle_timeout: Duration,
) -> ApiResult<ProjectSyncUploadResult> {
    let mut file = fs::File::create(tmp_path)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to create spool file: {}", error)))?;
    let mut stream = body.into_data_stream();
    let mut uploaded_bytes: u64 = 0;
    let mut last_progress_update_ms = now_epoch_ms();
    let mut next_progress_update_bytes = PROJECT_SYNC_UPLOAD_PROGRESS_UPDATE_BYTES;
    let mut last_chunk_at = tokio::time::Instant::now();

    loop {
        if project_sync_cancellation_requested(&state, job_id).await? {
            return Ok(ProjectSyncUploadResult::Cancelled);
        }

        let next_chunk = tokio::select! {
            chunk = stream.next() => chunk,
            _ = tokio::time::sleep(PROJECT_SYNC_UPLOAD_CANCEL_POLL_INTERVAL) => {
                if last_chunk_at.elapsed() >= idle_timeout {
                    return Err(ApiError::RequestTimeout(project_sync_upload_timeout_message(
                        idle_timeout,
                    )));
                }
                continue;
            }
        };

        let Some(chunk_result) = next_chunk else {
            break;
        };

        if project_sync_cancellation_requested(&state, job_id).await? {
            return Ok(ProjectSyncUploadResult::Cancelled);
        }

        let chunk = chunk_result.map_err(|error| {
            ApiError::BadRequest(format!("Failed to read upload body: {}", error))
        })?;
        if !chunk.is_empty() {
            last_chunk_at = tokio::time::Instant::now();
        }
        uploaded_bytes = uploaded_bytes.saturating_add(chunk.len() as u64);
        if uploaded_bytes > declared_bytes {
            return Err(ApiError::BadRequest(format!(
                "Upload body exceeds declared_bytes {}",
                declared_bytes
            )));
        }
        file.write_all(&chunk).await.map_err(|error| {
            ApiError::Internal(format!("Failed to write spool file: {}", error))
        })?;
        let current_time_ms = now_epoch_ms();
        if uploaded_bytes >= next_progress_update_bytes
            || current_time_ms.saturating_sub(last_progress_update_ms)
                >= PROJECT_SYNC_UPLOAD_PROGRESS_UPDATE_INTERVAL_MS
        {
            refresh_project_sync_upload_progress(&state, job_id, uploaded_bytes).await?;
            last_progress_update_ms = current_time_ms;
            next_progress_update_bytes =
                uploaded_bytes.saturating_add(PROJECT_SYNC_UPLOAD_PROGRESS_UPDATE_BYTES);
        }
        if project_sync_cancellation_requested(&state, job_id).await? {
            return Ok(ProjectSyncUploadResult::Cancelled);
        }
    }

    file.flush()
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to flush spool file: {}", error)))?;
    drop(file);

    if project_sync_cancellation_requested(&state, job_id).await? {
        return Ok(ProjectSyncUploadResult::Cancelled);
    }

    if uploaded_bytes != declared_bytes {
        return Err(ApiError::BadRequest(format!(
            "Uploaded {} bytes, expected {}",
            uploaded_bytes, declared_bytes
        )));
    }

    fs::rename(tmp_path, final_path)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to persist spool file: {}", error)))?;

    if project_sync_cancellation_requested(&state, job_id).await? {
        return Ok(ProjectSyncUploadResult::Cancelled);
    }

    Ok(ProjectSyncUploadResult::Uploaded { uploaded_bytes })
}

#[utoipa::path(
    post,
    path = "/project_sync/jobs/{job_id}/finalize",
    tag = "project_sync",
    params(
        ("job_id" = String, Path, description = "Project sync job id")
    ),
    responses(
        (status = 200, description = "Project sync job queued", body = ProjectSyncJobResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job is not finalizable", body = ErrorResponse)
    )
)]
pub async fn finalize_project_sync_job(
    State(state): State<Arc<AppState>>,
    AxumPath(job_id): AxumPath<String>,
) -> ApiResult<PrettyJson<ProjectSyncJobResponse>> {
    let lock = job_lock(&job_id);
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(&state, &job_id).await?;

    if manifest.status != ProjectSyncJobStatus::Uploaded {
        return Err(ApiError::Conflict(format!(
            "project_sync job '{}' is not finalizable from status {:?}",
            job_id, manifest.status
        )));
    }
    if manifest.uploaded_bytes != manifest.declared_bytes {
        return Err(ApiError::Conflict(format!(
            "project_sync job '{}' uploaded {} bytes but declared {}",
            job_id, manifest.uploaded_bytes, manifest.declared_bytes
        )));
    }
    if fs::metadata(spool_path(&state, &job_id)).await.is_err() {
        return Err(ApiError::Conflict(format!(
            "project_sync job '{}' has no persisted spool payload",
            job_id
        )));
    }

    manifest.status = ProjectSyncJobStatus::Queued;
    manifest.error = None;
    manifest.updated_at_ms = now_epoch_ms();
    write_manifest(&state, &manifest).await?;

    let worker_state = state.clone();
    let worker_job_id = job_id.clone();
    tokio::spawn(async move {
        run_project_sync_job(worker_state, worker_job_id).await;
    });

    Ok(PrettyJson(manifest_to_response(&manifest)))
}

#[utoipa::path(
    get,
    path = "/project_sync/jobs/{job_id}",
    tag = "project_sync",
    params(
        ("job_id" = String, Path, description = "Project sync job id")
    ),
    responses(
        (status = 200, description = "Project sync job status", body = ProjectSyncJobResponse),
        (status = 404, description = "Job not found", body = ErrorResponse)
    )
)]
pub async fn get_project_sync_job(
    State(state): State<Arc<AppState>>,
    AxumPath(job_id): AxumPath<String>,
) -> ApiResult<PrettyJson<ProjectSyncJobResponse>> {
    let manifest = read_manifest(&state, &job_id).await?;
    Ok(PrettyJson(manifest_to_response(&manifest)))
}

#[utoipa::path(
    delete,
    path = "/project_sync/jobs/{job_id}",
    tag = "project_sync",
    params(
        ("job_id" = String, Path, description = "Project sync job id")
    ),
    responses(
        (status = 200, description = "Project sync job cancelled", body = ProjectSyncJobResponse),
        (status = 404, description = "Job not found", body = ErrorResponse),
        (status = 409, description = "Job cannot be cancelled", body = ErrorResponse)
    )
)]
pub async fn cancel_project_sync_job(
    State(state): State<Arc<AppState>>,
    AxumPath(job_id): AxumPath<String>,
) -> ApiResult<PrettyJson<ProjectSyncJobResponse>> {
    let lock = job_lock(&job_id);
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(&state, &job_id).await?;

    if is_terminal_status(manifest.status) {
        return Ok(PrettyJson(manifest_to_response(&manifest)));
    }
    if manifest.status == ProjectSyncJobStatus::Running
        || manifest.status == ProjectSyncJobStatus::Uploading
    {
        manifest.status = ProjectSyncJobStatus::Cancelling;
        manifest.error = Some("Cancellation requested by client".to_string());
        manifest.updated_at_ms = now_epoch_ms();
        write_manifest(&state, &manifest).await?;
        return Ok(PrettyJson(manifest_to_response(&manifest)));
    }
    if manifest.status == ProjectSyncJobStatus::Cancelling {
        return Ok(PrettyJson(manifest_to_response(&manifest)));
    }

    persist_terminal_project_sync_manifest(
        &state,
        &mut manifest,
        ProjectSyncJobStatus::Cancelled,
        Some("Cancelled by client".to_string()),
    )
    .await?;
    Ok(PrettyJson(manifest_to_response(&manifest)))
}

async fn run_project_sync_job(state: Arc<AppState>, job_id: String) {
    if let Err(error) = run_project_sync_job_inner(state.clone(), &job_id).await {
        let lock = job_lock(&job_id);
        let _guard = lock.lock().await;
        match read_manifest(&state, &job_id).await {
            Ok(mut manifest) if !is_terminal_status(manifest.status) => {
                if let Err(release_error) = persist_terminal_project_sync_manifest(
                    &state,
                    &mut manifest,
                    ProjectSyncJobStatus::Failed,
                    Some(error.to_string()),
                )
                .await
                {
                    tracing::error!(
                        job_id = %job_id,
                        error = %release_error,
                        "project_sync.worker.failed_to_release_reserved_bytes"
                    );
                }
            }
            Ok(_) => {}
            Err(read_error) => {
                tracing::error!(
                    job_id = %job_id,
                    error = %read_error,
                    "project_sync.worker.failed_to_update_manifest"
                );
            }
        }
    }
}

async fn run_project_sync_job_inner(state: Arc<AppState>, job_id: &str) -> ApiResult<()> {
    {
        let lock = job_lock(job_id);
        let _guard = lock.lock().await;
        let manifest = read_manifest(&state, job_id).await?;
        if is_cancellation_requested(manifest.status) {
            return Ok(());
        }
        if manifest.status != ProjectSyncJobStatus::Queued {
            return Err(ApiError::Conflict(format!(
                "project_sync job '{}' worker expected queued status, got {:?}",
                job_id, manifest.status
            )));
        }
        update_manifest_status(&state, job_id, ProjectSyncJobStatus::Running, None).await?;
    }

    let manifest = read_manifest(&state, job_id).await?;
    let spool = spool_path(&state, job_id);
    let plan = collect_project_sync_plan(&spool).await?;
    let path_str = state
        .index_path(&manifest.index_name)
        .to_string_lossy()
        .to_string();
    let index_lock = get_index_write_lock(&state, &manifest.index_name);
    let _index_guard = index_lock.lock().await;
    let mut snapshot: Option<PathBuf> = None;
    let process_result: ApiResult<()> = async {
        ensure_project_sync_job_not_cancelled(&state, job_id).await?;
        run_preflight_repair_locked(&path_str)
            .await
            .map_err(|error| {
                ApiError::Internal(format!("Index/DB sync preflight repair failed: {}", error))
            })?;
        ensure_project_sync_job_not_cancelled(&state, job_id).await?;
        let executable_plan = build_project_sync_executable_plan(&path_str, plan.clone())?;
        if executable_plan.changed_upsert_files.is_empty()
            && executable_plan.delete_relative_paths.is_empty()
        {
            tracing::info!(
                job_id = %job_id,
                index = %manifest.index_name,
                skipped_unchanged_files = executable_plan.skipped_unchanged_files.len(),
                records = plan.record_count,
                "project_sync.noop.skip_all_unchanged"
            );
            return Ok(());
        }
        snapshot =
            Some(create_project_sync_snapshot(state.clone(), &manifest.index_name, job_id).await?);
        ensure_project_sync_job_not_cancelled(&state, job_id).await?;
        let changed_relative_paths = executable_plan
            .changed_upsert_files
            .iter()
            .map(|file| file.relative_path.clone())
            .collect::<Vec<String>>();
        let mut relative_paths_to_delete = changed_relative_paths;
        relative_paths_to_delete.extend(executable_plan.delete_relative_paths.iter().cloned());
        relative_paths_to_delete.sort();
        relative_paths_to_delete.dedup();
        let deleted_existing = delete_existing_relative_paths_after_repair_locked(
            state.clone(),
            &manifest.index_name,
            &relative_paths_to_delete,
        )
        .await?;
        let deleted_manifest_rows =
            filtering::delete_project_files(&path_str, &executable_plan.delete_relative_paths)
                .map_err(ApiError::NextPlaid)?;
        tracing::info!(
            job_id = %job_id,
            index = %manifest.index_name,
            deleted_existing = deleted_existing,
            deleted_manifest_rows = deleted_manifest_rows,
            changed_files = executable_plan.changed_upsert_files.len(),
            deleted_files = executable_plan.delete_relative_paths.len(),
            skipped_unchanged_files = executable_plan.skipped_unchanged_files.len(),
            records = plan.record_count,
            "project_sync.replace.deleted_existing"
        );
        ensure_project_sync_job_not_cancelled(&state, job_id).await?;
        process_project_sync_files_locked(
            state.clone(),
            job_id,
            &manifest.index_name,
            &path_str,
            executable_plan,
        )
        .await?;
        ensure_project_sync_job_not_cancelled(&state, job_id).await?;
        run_project_sync_postflight_locked(&path_str).await?;
        Ok(())
    }
    .await;

    if let Err(error) = process_result {
        if snapshot.is_none() {
            let cancellation_requested =
                match project_sync_cancellation_requested(&state, job_id).await {
                    Ok(value) => value,
                    Err(status_error) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %status_error,
                            "project_sync.cancel_check.failed_before_snapshot"
                        );
                        false
                    }
                };
            let status = if cancellation_requested {
                ProjectSyncJobStatus::Cancelled
            } else {
                ProjectSyncJobStatus::Failed
            };
            let message = if cancellation_requested {
                "Cancelled by client".to_string()
            } else {
                error.to_string()
            };
            finish_project_sync_job(&state, job_id, status, Some(message)).await?;
            return Ok(());
        }

        let snapshot_path = snapshot
            .as_ref()
            .ok_or_else(|| ApiError::Internal("project_sync snapshot missing".to_string()))?;
        if let Err(rollback_error) =
            restore_project_sync_snapshot_locked(state.clone(), &manifest.index_name, snapshot_path)
                .await
        {
            let dirty_result = mark_project_sync_dirty(
                &state,
                &manifest.index_name,
                job_id,
                &error.to_string(),
                &rollback_error,
            )
            .await;
            let message = match dirty_result {
                Ok(()) => format!(
                    "{}; rollback failed: {}; index marked dirty",
                    error, rollback_error
                ),
                Err(dirty_error) => format!(
                    "{}; rollback failed: {}; failed to persist dirty marker: {}",
                    error, rollback_error, dirty_error
                ),
            };
            finish_project_sync_job(&state, job_id, ProjectSyncJobStatus::Failed, Some(message))
                .await?;
            return Ok(());
        }

        if let Err(cleanup_error) = remove_project_sync_snapshot(snapshot_path).await {
            tracing::warn!(
                job_id = %job_id,
                error = %cleanup_error,
                "project_sync.rollback.snapshot_cleanup_failed"
            );
        }

        let cancellation_requested = match project_sync_cancellation_requested(&state, job_id).await
        {
            Ok(value) => value,
            Err(status_error) => {
                tracing::warn!(
                    job_id = %job_id,
                    error = %status_error,
                    "project_sync.cancel_check.failed_after_rollback"
                );
                false
            }
        };
        let status = if cancellation_requested {
            ProjectSyncJobStatus::Cancelled
        } else {
            ProjectSyncJobStatus::Failed
        };
        let message = if cancellation_requested {
            "Cancelled by client".to_string()
        } else {
            error.to_string()
        };
        finish_project_sync_job(&state, job_id, status, Some(message)).await?;
        return Ok(());
    }

    if let Some(snapshot_path) = snapshot.as_ref() {
        if let Err(cleanup_error) = remove_project_sync_snapshot(snapshot_path).await {
            tracing::warn!(
                job_id = %job_id,
                error = %cleanup_error,
                "project_sync.snapshot.cleanup_failed"
            );
        }
    }

    finish_project_sync_job(&state, job_id, ProjectSyncJobStatus::Completed, None).await?;
    Ok(())
}

fn parse_project_sync_record(line: &str) -> ApiResult<ProjectSyncUploadRecord> {
    let record: ProjectSyncUploadRecord = serde_json::from_str(line)
        .map_err(|error| ApiError::BadRequest(format!("Invalid NDJSON record: {}", error)))?;
    match record.op {
        ProjectSyncRecordOp::Upsert => {
            let source_path = record.path.as_deref().ok_or_else(|| {
                ApiError::BadRequest("project_sync upsert record path is required".to_string())
            })?;
            if source_path.is_empty() {
                return Err(ApiError::BadRequest(
                    "project_sync upsert record path cannot be empty".to_string(),
                ));
            }
            if record.content.is_none() {
                return Err(ApiError::BadRequest(format!(
                    "project_sync upsert record '{}' content is required",
                    source_path
                )));
            }
        }
        ProjectSyncRecordOp::Delete => {
            if record.relative_path.is_none() {
                return Err(ApiError::BadRequest(
                    "project_sync delete record relative_path is required".to_string(),
                ));
            }
        }
    }
    Ok(record)
}

fn project_sync_metadata_object(
    metadata: Option<serde_json::Value>,
) -> ApiResult<serde_json::Map<String, serde_json::Value>> {
    let object = match metadata {
        Some(serde_json::Value::Object(object)) => object,
        Some(_) => {
            return Err(ApiError::BadRequest(
                "project_sync record metadata must be a JSON object".to_string(),
            ))
        }
        None => serde_json::Map::new(),
    };
    Ok(object)
}

fn build_project_sync_metadata(record: &ProjectSyncUpsertRecord) -> ApiResult<serde_json::Value> {
    let mut object = project_sync_metadata_object(record.metadata.clone())?;
    object.insert(
        "source_path".to_string(),
        serde_json::Value::String(record.source_path.clone()),
    );
    object.insert(
        "relative_path".to_string(),
        serde_json::Value::String(record.relative_path.clone()),
    );
    if let Some(language) = &record.language {
        object.insert(
            "language".to_string(),
            serde_json::Value::String(language.clone()),
        );
    }
    if let Some(content_hash) = &record.content_hash {
        object.insert(
            "content_hash".to_string(),
            serde_json::Value::String(content_hash.clone()),
        );
    }
    if let Some(chunk_index) = record.chunk_index {
        object.insert("chunk_index".to_string(), serde_json::json!(chunk_index));
    }
    if let Some(chunk_count) = record.chunk_count {
        object.insert("chunk_count".to_string(), serde_json::json!(chunk_count));
    }
    object
        .entry("source".to_string())
        .or_insert_with(|| serde_json::Value::String("project_sync".to_string()));
    Ok(serde_json::Value::Object(object))
}

async fn finish_project_sync_job(
    state: &Arc<AppState>,
    job_id: &str,
    status: ProjectSyncJobStatus,
    error: Option<String>,
) -> ApiResult<()> {
    let lock = job_lock(job_id);
    let _guard = lock.lock().await;
    let mut manifest = read_manifest(state, job_id).await?;
    persist_terminal_project_sync_manifest(state, &mut manifest, status, error).await
}

async fn ensure_project_sync_job_not_cancelled(state: &AppState, job_id: &str) -> ApiResult<()> {
    let manifest = read_manifest(state, job_id).await?;
    if is_cancellation_requested(manifest.status) {
        return Err(ApiError::Conflict(format!(
            "project_sync job '{}' was cancelled",
            job_id
        )));
    }
    Ok(())
}

async fn project_sync_cancellation_requested(state: &AppState, job_id: &str) -> ApiResult<bool> {
    let manifest = read_manifest(state, job_id).await?;
    Ok(is_cancellation_requested(manifest.status))
}

fn project_file_entry_to_response(
    entry: filtering::ProjectFileEntry,
) -> ProjectSyncFileEntryResponse {
    ProjectSyncFileEntryResponse {
        relative_path: entry.relative_path,
        content_hash: entry.content_hash,
        language: entry.language,
        chunk_count: entry.chunk_count,
        updated_at_ms: entry.updated_at_ms,
    }
}

fn validate_project_sync_file_check_items(files: &[ProjectSyncFileCheckItem]) -> ApiResult<()> {
    let mut seen_paths = HashSet::new();
    for file in files {
        validate_project_sync_relative_path(&file.relative_path)?;
        validate_project_sync_content_hash(&file.content_hash)?;
        if !seen_paths.insert(file.relative_path.clone()) {
            return Err(ApiError::BadRequest(format!(
                "project_sync files/check contains duplicate relative_path '{}'",
                file.relative_path
            )));
        }
    }
    Ok(())
}

fn validate_project_sync_relative_path(relative_path: &str) -> ApiResult<()> {
    if relative_path.is_empty() {
        return Err(ApiError::BadRequest(
            "project_sync relative_path cannot be empty".to_string(),
        ));
    }
    if relative_path.contains('\\') {
        return Err(ApiError::BadRequest(format!(
            "project_sync relative_path '{}' must use forward slashes",
            relative_path
        )));
    }
    let path = Path::new(relative_path);
    let components = path.components().collect::<Vec<Component<'_>>>();
    if components.is_empty()
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ApiError::BadRequest(format!(
            "project_sync relative_path '{}' must be a safe project-relative path",
            relative_path
        )));
    }
    Ok(())
}

fn validate_project_sync_content_hash(content_hash: &str) -> ApiResult<()> {
    let Some(hex) = content_hash.strip_prefix("sha256:") else {
        return Err(ApiError::BadRequest(format!(
            "project_sync content_hash '{}' must start with 'sha256:'",
            content_hash
        )));
    };
    if hex.len() != 64 || !hex.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(ApiError::BadRequest(format!(
            "project_sync content_hash '{}' must contain 64 hexadecimal SHA-256 characters",
            content_hash
        )));
    }
    Ok(())
}

fn project_sync_metadata_string(
    metadata: Option<&serde_json::Value>,
    key: &str,
) -> ApiResult<Option<String>> {
    match metadata {
        Some(serde_json::Value::Object(object)) => Ok(object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)),
        Some(_) => Err(ApiError::BadRequest(
            "project_sync record metadata must be a JSON object".to_string(),
        )),
        None => Ok(None),
    }
}

fn resolve_project_sync_record_relative_path(
    record: &ProjectSyncUploadRecord,
    source_path: Option<&str>,
) -> ApiResult<String> {
    if let Some(relative_path) = &record.relative_path {
        validate_project_sync_relative_path(relative_path)?;
        return Ok(relative_path.clone());
    }
    if let Some(relative_path) =
        project_sync_metadata_string(record.metadata.as_ref(), "relative_path")?
    {
        validate_project_sync_relative_path(&relative_path)?;
        return Ok(relative_path);
    }
    let fallback_path = source_path.ok_or_else(|| {
        ApiError::BadRequest("project_sync relative_path is required".to_string())
    })?;
    validate_project_sync_relative_path(fallback_path)?;
    Ok(fallback_path.to_string())
}

fn resolve_project_sync_record_language(
    record: &ProjectSyncUploadRecord,
) -> ApiResult<Option<String>> {
    match &record.language {
        Some(language) => Ok(Some(language.clone())),
        None => project_sync_metadata_string(record.metadata.as_ref(), "language"),
    }
}

fn build_project_sync_upsert_record(
    record: ProjectSyncUploadRecord,
) -> ApiResult<ProjectSyncUpsertRecord> {
    let source_path = record.path.clone().ok_or_else(|| {
        ApiError::BadRequest("project_sync upsert record path is required".to_string())
    })?;
    let content = record.content.clone().ok_or_else(|| {
        ApiError::BadRequest(format!(
            "project_sync upsert record '{}' content is required",
            source_path
        ))
    })?;
    let relative_path = resolve_project_sync_record_relative_path(&record, Some(&source_path))?;
    if let Some(content_hash) = &record.content_hash {
        validate_project_sync_content_hash(content_hash)?;
    }
    if record.chunk_index == Some(0) {
        return Err(ApiError::BadRequest(format!(
            "project_sync record '{}' chunk_index must be greater than 0",
            source_path
        )));
    }
    if record.chunk_count == Some(0) {
        return Err(ApiError::BadRequest(format!(
            "project_sync record '{}' chunk_count must be greater than 0",
            source_path
        )));
    }
    if let (Some(chunk_index), Some(chunk_count)) = (record.chunk_index, record.chunk_count) {
        if chunk_index > chunk_count {
            return Err(ApiError::BadRequest(format!(
                "project_sync record '{}' chunk_index {} exceeds chunk_count {}",
                source_path, chunk_index, chunk_count
            )));
        }
    }
    let language = resolve_project_sync_record_language(&record)?;
    Ok(ProjectSyncUpsertRecord {
        source_path,
        relative_path,
        content,
        language,
        metadata: record.metadata,
        content_hash: record.content_hash,
        chunk_index: record.chunk_index,
        chunk_count: record.chunk_count,
    })
}

fn validate_project_sync_delete_record(record: &ProjectSyncUploadRecord) -> ApiResult<String> {
    let relative_path = resolve_project_sync_record_relative_path(record, None)?;
    if let Some(content_hash) = &record.content_hash {
        validate_project_sync_content_hash(content_hash)?;
    }
    Ok(relative_path)
}

fn validate_project_sync_file_group(
    relative_path: String,
    mut records: Vec<ProjectSyncUpsertRecord>,
) -> ApiResult<ProjectSyncUpsertFile> {
    let mut content_hashes = records
        .iter()
        .filter_map(|record| record.content_hash.clone())
        .collect::<Vec<String>>();
    content_hashes.sort();
    content_hashes.dedup();
    if content_hashes.len() > 1 {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' has inconsistent content_hash values",
            relative_path
        )));
    }
    let content_hash = content_hashes.into_iter().next();
    if content_hash.is_some()
        && records
            .iter()
            .any(|record| record.content_hash.as_ref() != content_hash.as_ref())
    {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' must include content_hash on every chunk",
            relative_path
        )));
    }

    let language = validate_project_sync_file_language(&relative_path, &records)?;
    let chunk_count = validate_project_sync_file_chunk_count(&relative_path, &records)?;
    sort_project_sync_file_records(&relative_path, chunk_count, &mut records)?;

    Ok(ProjectSyncUpsertFile {
        relative_path,
        content_hash,
        language,
        chunk_count,
        records,
    })
}

fn validate_project_sync_file_language(
    relative_path: &str,
    records: &[ProjectSyncUpsertRecord],
) -> ApiResult<String> {
    let mut languages = records
        .iter()
        .filter_map(|record| record.language.clone())
        .collect::<Vec<String>>();
    languages.sort();
    languages.dedup();
    if languages.len() > 1 {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' has inconsistent language values",
            relative_path
        )));
    }
    Ok(languages
        .into_iter()
        .next()
        .unwrap_or_else(|| "unknown".to_string()))
}

fn validate_project_sync_file_chunk_count(
    relative_path: &str,
    records: &[ProjectSyncUpsertRecord],
) -> ApiResult<usize> {
    let mut chunk_counts = records
        .iter()
        .filter_map(|record| record.chunk_count)
        .collect::<Vec<usize>>();
    chunk_counts.sort_unstable();
    chunk_counts.dedup();
    if chunk_counts.len() > 1 {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' has inconsistent chunk_count values",
            relative_path
        )));
    }
    let chunk_count = chunk_counts.into_iter().next().unwrap_or(records.len());
    if chunk_count != records.len() {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' declared chunk_count {} but uploaded {} chunks",
            relative_path,
            chunk_count,
            records.len()
        )));
    }
    Ok(chunk_count)
}

fn sort_project_sync_file_records(
    relative_path: &str,
    chunk_count: usize,
    records: &mut [ProjectSyncUpsertRecord],
) -> ApiResult<()> {
    let has_chunk_index = records.iter().any(|record| record.chunk_index.is_some());
    if !has_chunk_index {
        return Ok(());
    }
    if records.iter().any(|record| record.chunk_index.is_none()) {
        return Err(ApiError::BadRequest(format!(
            "project_sync file '{}' must include chunk_index on every chunk",
            relative_path
        )));
    }
    let mut seen_indexes = HashSet::new();
    for record in records.iter() {
        let chunk_index = record.chunk_index.ok_or_else(|| {
            ApiError::BadRequest(format!(
                "project_sync file '{}' is missing chunk_index",
                relative_path
            ))
        })?;
        if chunk_index > chunk_count {
            return Err(ApiError::BadRequest(format!(
                "project_sync file '{}' chunk_index {} exceeds chunk_count {}",
                relative_path, chunk_index, chunk_count
            )));
        }
        if !seen_indexes.insert(chunk_index) {
            return Err(ApiError::BadRequest(format!(
                "project_sync file '{}' contains duplicate chunk_index {}",
                relative_path, chunk_index
            )));
        }
    }
    records.sort_by_key(|record| record.chunk_index.unwrap_or(usize::MAX));
    Ok(())
}

async fn collect_project_sync_plan(spool: &Path) -> ApiResult<ProjectSyncPlan> {
    let file = fs::File::open(spool)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to open spool file: {}", error)))?;
    let mut lines = BufReader::new(file).lines();
    let mut seen_source_paths = HashSet::new();
    let mut grouped_records: HashMap<String, Vec<ProjectSyncUpsertRecord>> = HashMap::new();
    let mut delete_paths = Vec::new();
    let mut seen_delete_paths = HashSet::new();
    let mut processed_records = 0usize;

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| ApiError::BadRequest(format!("Failed to read spool line: {}", error)))?
    {
        if line.trim().is_empty() {
            continue;
        }
        let record = parse_project_sync_record(&line)?;
        processed_records += 1;
        match record.op {
            ProjectSyncRecordOp::Upsert => {
                let upsert_record = build_project_sync_upsert_record(record)?;
                if !seen_source_paths.insert(upsert_record.source_path.clone()) {
                    return Err(ApiError::BadRequest(format!(
                        "project_sync upload contains duplicate path '{}'",
                        upsert_record.source_path
                    )));
                }
                grouped_records
                    .entry(upsert_record.relative_path.clone())
                    .or_default()
                    .push(upsert_record);
            }
            ProjectSyncRecordOp::Delete => {
                let relative_path = validate_project_sync_delete_record(&record)?;
                if seen_delete_paths.insert(relative_path.clone()) {
                    delete_paths.push(relative_path);
                }
            }
        }
    }

    if processed_records == 0 {
        return Err(ApiError::BadRequest(
            "project_sync upload contains no records".to_string(),
        ));
    }

    for relative_path in grouped_records.keys() {
        if seen_delete_paths.contains(relative_path) {
            return Err(ApiError::BadRequest(format!(
                "project_sync upload cannot delete and upsert '{}' in the same job",
                relative_path
            )));
        }
    }

    let mut upsert_files = Vec::with_capacity(grouped_records.len());
    let mut grouped_pairs = grouped_records
        .into_iter()
        .collect::<Vec<(String, Vec<ProjectSyncUpsertRecord>)>>();
    grouped_pairs.sort_by(|left, right| left.0.cmp(&right.0));
    for (relative_path, records) in grouped_pairs {
        upsert_files.push(validate_project_sync_file_group(relative_path, records)?);
    }

    Ok(ProjectSyncPlan {
        upsert_files,
        delete_relative_paths: delete_paths,
        record_count: processed_records,
    })
}

fn project_sync_manifest_config_matches(config: Option<filtering::ProjectManifestConfig>) -> bool {
    matches!(
        config,
        Some(filtering::ProjectManifestConfig {
            version: filtering::PROJECT_MANIFEST_VERSION,
            chunker_version: PROJECT_SYNC_CHUNKER_VERSION,
            max_lines_per_blob: PROJECT_SYNC_MAX_LINES_PER_BLOB,
        })
    )
}

fn build_project_sync_executable_plan(
    index_path: &str,
    plan: ProjectSyncPlan,
) -> ApiResult<ProjectSyncExecutablePlan> {
    if plan.upsert_files.is_empty() && plan.delete_relative_paths.is_empty() {
        return Err(ApiError::BadRequest(
            "project_sync upload contains no actionable records".to_string(),
        ));
    }
    filtering::ensure_project_files(index_path).map_err(ApiError::NextPlaid)?;
    let config = filtering::get_project_manifest_config(index_path).map_err(ApiError::NextPlaid)?;
    let config_matches = project_sync_manifest_config_matches(config);
    let hash_paths = plan
        .upsert_files
        .iter()
        .filter(|file| file.content_hash.is_some())
        .map(|file| file.relative_path.clone())
        .collect::<Vec<String>>();
    let existing_files =
        filtering::get_project_files(index_path, &hash_paths).map_err(ApiError::NextPlaid)?;
    let existing_by_path = existing_files
        .into_iter()
        .map(|file| (file.relative_path.clone(), file))
        .collect::<HashMap<String, filtering::ProjectFileEntry>>();

    let mut changed_upsert_files = Vec::new();
    let mut skipped_unchanged_files = Vec::new();
    for file in plan.upsert_files {
        let should_skip = match (
            &file.content_hash,
            existing_by_path.get(&file.relative_path),
        ) {
            (Some(content_hash), Some(existing_file)) => {
                config_matches
                    && existing_file.content_hash == *content_hash
                    && existing_file.language == file.language
                    && existing_file.chunk_count == file.chunk_count
            }
            _ => false,
        };
        if should_skip {
            skipped_unchanged_files.push(file);
        } else {
            changed_upsert_files.push(file);
        }
    }

    Ok(ProjectSyncExecutablePlan {
        changed_upsert_files,
        skipped_unchanged_files,
        delete_relative_paths: plan.delete_relative_paths,
    })
}

async fn process_project_sync_files_locked(
    state: Arc<AppState>,
    job_id: &str,
    index_name: &str,
    index_path: &str,
    plan: ProjectSyncExecutablePlan,
) -> ApiResult<()> {
    let batch_size = max_batch_documents();
    let mut documents: Vec<String> = Vec::with_capacity(batch_size);
    let mut metadata: Vec<serde_json::Value> = Vec::with_capacity(batch_size);
    let mut processed_records = 0usize;

    for file in &plan.changed_upsert_files {
        for record in &file.records {
            let project_metadata = build_project_sync_metadata(record)?;
            documents.push(record.content.clone());
            metadata.push(project_metadata);
            processed_records += 1;

            if documents.len() >= batch_size {
                process_project_sync_batch_locked(
                    state.clone(),
                    job_id,
                    index_name,
                    documents,
                    metadata,
                )
                .await?;
                documents = Vec::with_capacity(batch_size);
                metadata = Vec::with_capacity(batch_size);
            }
        }
    }

    if !documents.is_empty() {
        process_project_sync_batch_locked(state.clone(), job_id, index_name, documents, metadata)
            .await?;
    }

    ensure_project_sync_job_not_cancelled(&state, job_id).await?;
    update_project_sync_manifest_after_success(index_path, &plan)?;
    tracing::info!(
        index = %index_name,
        changed_files = plan.changed_upsert_files.len(),
        skipped_unchanged_files = plan.skipped_unchanged_files.len(),
        deleted_files = plan.delete_relative_paths.len(),
        processed_records = processed_records,
        "project_sync.manifest.updated"
    );
    Ok(())
}

fn update_project_sync_manifest_after_success(
    index_path: &str,
    plan: &ProjectSyncExecutablePlan,
) -> ApiResult<()> {
    filtering::set_project_manifest_config(
        index_path,
        PROJECT_SYNC_CHUNKER_VERSION,
        PROJECT_SYNC_MAX_LINES_PER_BLOB,
    )
    .map_err(ApiError::NextPlaid)?;
    let now_ms = now_epoch_ms();
    let manifest_entries = plan
        .changed_upsert_files
        .iter()
        .filter_map(|file| {
            file.content_hash
                .as_ref()
                .map(|content_hash| filtering::ProjectFileEntry {
                    relative_path: file.relative_path.clone(),
                    content_hash: content_hash.clone(),
                    language: file.language.clone(),
                    chunk_count: file.chunk_count,
                    updated_at_ms: now_ms,
                })
        })
        .collect::<Vec<filtering::ProjectFileEntry>>();
    let unhashed_paths = plan
        .changed_upsert_files
        .iter()
        .filter(|file| file.content_hash.is_none())
        .map(|file| file.relative_path.clone())
        .collect::<Vec<String>>();

    filtering::delete_project_files(index_path, &unhashed_paths).map_err(ApiError::NextPlaid)?;
    filtering::upsert_project_files(index_path, &manifest_entries).map_err(ApiError::NextPlaid)?;
    Ok(())
}

async fn process_project_sync_batch_locked(
    state: Arc<AppState>,
    job_id: &str,
    index_name: &str,
    documents: Vec<String>,
    metadata: Vec<serde_json::Value>,
) -> ApiResult<()> {
    ensure_project_sync_job_not_cancelled(&state, job_id).await?;
    let request = UpdateWithEncodingRequest {
        documents,
        metadata,
        pool_factor: None,
    };
    let prepared = prepare_update_with_encoding_batch(state.clone(), request).await?;
    ensure_project_sync_job_not_cancelled(&state, job_id).await?;
    commit_embeddings_batch_after_repair_locked(
        index_name,
        prepared.embeddings,
        prepared.metadata,
        &state,
    )
    .await
    .map_err(ApiError::Internal)?;
    Ok(())
}

async fn create_project_sync_snapshot(
    state: Arc<AppState>,
    index_name: &str,
    job_id: &str,
) -> ApiResult<PathBuf> {
    let index_path = state.index_path(index_name);
    let snapshot = snapshot_path(&state, job_id);
    let snapshot_for_task = snapshot.clone();
    task::spawn_blocking(move || -> Result<(), String> {
        remove_dir_if_exists_blocking(&snapshot_for_task)?;
        copy_dir_recursive_blocking(&index_path, &snapshot_for_task)
    })
    .await
    .map_err(|error| ApiError::Internal(format!("Snapshot task failed: {}", error)))?
    .map_err(|error| {
        ApiError::Internal(format!("Failed to create project_sync snapshot: {}", error))
    })?;
    Ok(snapshot)
}

async fn remove_project_sync_snapshot(snapshot: &Path) -> Result<(), String> {
    let snapshot_path = snapshot.to_path_buf();
    task::spawn_blocking(move || remove_dir_if_exists_blocking(&snapshot_path))
        .await
        .map_err(|error| format!("Snapshot cleanup task failed: {}", error))?
}

async fn restore_project_sync_snapshot_locked(
    state: Arc<AppState>,
    index_name: &str,
    snapshot: &Path,
) -> Result<(), String> {
    let index_path = state.index_path(index_name);
    let snapshot_path = snapshot.to_path_buf();
    let index_name_owned = index_name.to_string();
    state.unload_index(index_name);

    task::spawn_blocking(move || {
        restore_project_sync_snapshot_blocking(&snapshot_path, &index_path)
    })
    .await
    .map_err(|error| format!("Snapshot restore task failed: {}", error))??;

    if state
        .index_path(&index_name_owned)
        .join("metadata.json")
        .exists()
    {
        state
            .reload_index(&index_name_owned)
            .map_err(|error| format!("Failed to reload restored index: {}", error))?;
    }
    Ok(())
}

fn restore_project_sync_snapshot_blocking(
    snapshot: &Path,
    index_path: &Path,
) -> Result<(), String> {
    let parent = index_path.parent().ok_or_else(|| {
        format!(
            "Failed to resolve parent directory for restored index '{}'",
            index_path.display()
        )
    })?;
    let index_name = index_path.file_name().ok_or_else(|| {
        format!(
            "Failed to resolve index directory name for '{}'",
            index_path.display()
        )
    })?;
    let staging_path = parent.join(format!(".{}.restore-staging", index_name.to_string_lossy()));
    let backup_path = parent.join(format!(".{}.restore-backup", index_name.to_string_lossy()));

    remove_dir_if_exists_blocking(&staging_path)?;
    remove_dir_if_exists_blocking(&backup_path)?;
    copy_dir_recursive_blocking(snapshot, &staging_path)?;

    if index_path.exists() {
        if let Err(error) = std_fs::rename(index_path, &backup_path) {
            remove_dir_if_exists_blocking(&staging_path)?;
            return Err(format!(
                "Failed to move live index '{}' to backup '{}': {}",
                index_path.display(),
                backup_path.display(),
                error
            ));
        }
    }

    if let Err(error) = std_fs::rename(&staging_path, index_path) {
        if backup_path.exists() {
            std_fs::rename(&backup_path, index_path).map_err(|restore_error| {
                format!(
                    "Failed to promote staging '{}' to '{}': {}; backup restore also failed: {}",
                    staging_path.display(),
                    index_path.display(),
                    error,
                    restore_error
                )
            })?;
        }
        remove_dir_if_exists_blocking(&staging_path)?;
        return Err(format!(
            "Failed to promote staging '{}' to '{}': {}",
            staging_path.display(),
            index_path.display(),
            error
        ));
    }

    if let Err(error) = remove_dir_if_exists_blocking(&backup_path) {
        tracing::warn!(
            backup_path = %backup_path.display(),
            error = %error,
            "project_sync.restore.backup_cleanup_failed"
        );
    }
    Ok(())
}

async fn mark_project_sync_dirty(
    state: &AppState,
    index_name: &str,
    job_id: &str,
    operation_error: &str,
    rollback_error: &str,
) -> ApiResult<()> {
    let marker_path = dirty_marker_path(state, index_name, job_id);
    let marker_dir = marker_path.parent().ok_or_else(|| {
        ApiError::Internal("Failed to resolve project_sync dirty marker directory".to_string())
    })?;
    fs::create_dir_all(marker_dir).await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to create project_sync dirty marker directory: {}",
            error
        ))
    })?;
    let payload = serde_json::json!({
        "index_name": index_name,
        "job_id": job_id,
        "operation_error": operation_error,
        "rollback_error": rollback_error,
        "created_at_ms": now_epoch_ms()
    });
    let bytes = serde_json::to_vec_pretty(&payload).map_err(|error| {
        ApiError::Internal(format!("Failed to serialize dirty marker: {}", error))
    })?;
    fs::write(&marker_path, bytes).await.map_err(|error| {
        ApiError::Internal(format!(
            "Failed to write project_sync dirty marker '{}': {}",
            marker_path.display(),
            error
        ))
    })?;
    tracing::error!(
        index = %index_name,
        job_id = %job_id,
        operation_error = %operation_error,
        rollback_error = %rollback_error,
        "project_sync.index_dirty"
    );
    Ok(())
}

async fn delete_existing_relative_paths_after_repair_locked(
    state: Arc<AppState>,
    index_name: &str,
    relative_paths: &[String],
) -> ApiResult<usize> {
    if relative_paths.is_empty() {
        return Ok(0);
    }

    let path_str = state.index_path(index_name).to_string_lossy().to_string();
    let index_name_owned = index_name.to_string();
    let paths: Vec<String> = relative_paths.to_vec();
    let state_clone = state.clone();

    task::spawn_blocking(move || -> Result<usize, String> {
        if !filtering::exists(&path_str) {
            return Ok(0);
        }

        let has_relative_path =
            filtering::has_column(&path_str, "relative_path").map_err(|error| {
                format!("Failed to inspect relative_path metadata column: {}", error)
            })?;
        let has_source_path = filtering::has_column(&path_str, "source_path")
            .map_err(|error| format!("Failed to inspect source_path metadata column: {}", error))?;
        if !has_relative_path && !has_source_path {
            return Ok(0);
        }

        let mut doc_ids = if has_relative_path {
            collect_document_ids_by_metadata_values(&path_str, "relative_path", &paths).map_err(
                |error| format!("Failed to find existing relative_path documents: {}", error),
            )?
        } else {
            Vec::new()
        };
        if has_source_path {
            let mut source_path_doc_ids =
                collect_document_ids_by_metadata_values(&path_str, "source_path", &paths).map_err(
                    |error| format!("Failed to find existing source_path documents: {}", error),
                )?;
            let mut legacy_chunk_doc_ids =
                collect_legacy_chunk_source_path_doc_ids(&path_str, &paths).map_err(|error| {
                    format!(
                        "Failed to find existing legacy chunk source_path documents: {}",
                        error
                    )
                })?;
            doc_ids.append(&mut source_path_doc_ids);
            doc_ids.append(&mut legacy_chunk_doc_ids);
        }
        doc_ids.sort_unstable();
        doc_ids.dedup();
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let mut index = MmapIndex::load(&path_str)
            .map_err(|error| format!("Failed to load index: {}", error))?;
        let deleted = index.delete(&doc_ids).map_err(|error| {
            format!(
                "Failed to delete existing project_sync documents: {}",
                error
            )
        })?;
        index.reload().map_err(|error| {
            format!(
                "Failed to reload index after project_sync delete: {}",
                error
            )
        })?;
        state_clone
            .reload_index(&index_name_owned)
            .map_err(|error| {
                format!(
                    "Failed to reload state after project_sync delete: {}",
                    error
                )
            })?;
        Ok(deleted)
    })
    .await
    .map_err(|error| ApiError::Internal(format!("Relative path delete task failed: {}", error)))?
    .map_err(ApiError::Internal)
}

fn collect_document_ids_by_metadata_values(
    index_path: &str,
    column: &str,
    values: &[String],
) -> Result<Vec<i64>, next_plaid::Error> {
    let mut doc_ids = Vec::new();
    for value_chunk in values.chunks(500) {
        let (condition, parameters) = build_metadata_value_condition(column, value_chunk);
        let mut chunk_ids = filtering::where_condition(index_path, &condition, &parameters)?;
        doc_ids.append(&mut chunk_ids);
    }
    Ok(doc_ids)
}

fn collect_legacy_chunk_source_path_doc_ids(
    index_path: &str,
    relative_paths: &[String],
) -> Result<Vec<i64>, next_plaid::Error> {
    let mut doc_ids = Vec::new();
    for path_chunk in relative_paths.chunks(100) {
        let (condition, parameters) = build_legacy_chunk_source_path_condition(path_chunk);
        let mut chunk_ids = filtering::where_condition_regexp(index_path, &condition, &parameters)?;
        doc_ids.append(&mut chunk_ids);
    }
    Ok(doc_ids)
}

fn build_metadata_value_condition(
    column: &str,
    values: &[String],
) -> (String, Vec<serde_json::Value>) {
    if values.len() == 1 {
        return (
            format!("\"{}\" = ?", column),
            vec![serde_json::Value::String(values[0].clone())],
        );
    }

    let placeholders = std::iter::repeat_n("?", values.len())
        .collect::<Vec<&str>>()
        .join(", ");
    let parameters = values
        .iter()
        .map(|value| serde_json::Value::String(value.clone()))
        .collect::<Vec<serde_json::Value>>();
    (format!("\"{}\" IN ({})", column, placeholders), parameters)
}

fn build_legacy_chunk_source_path_condition(
    relative_paths: &[String],
) -> (String, Vec<serde_json::Value>) {
    let condition = std::iter::repeat_n("\"source_path\" REGEXP ?", relative_paths.len())
        .collect::<Vec<&str>>()
        .join(" OR ");
    let parameters = relative_paths
        .iter()
        .map(|relative_path| {
            serde_json::Value::String(format!(
                "^{}#chunk[0-9]+of[0-9]+$",
                escape_project_sync_regex_literal(relative_path)
            ))
        })
        .collect::<Vec<serde_json::Value>>();
    (condition, parameters)
}

fn escape_project_sync_regex_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn copy_dir_recursive_blocking(source: &Path, destination: &Path) -> Result<(), String> {
    if !source.exists() {
        return Err(format!(
            "Snapshot source directory '{}' does not exist",
            source.display()
        ));
    }
    std_fs::create_dir_all(destination).map_err(|error| {
        format!(
            "Failed to create snapshot directory '{}': {}",
            destination.display(),
            error
        )
    })?;

    for entry_result in std_fs::read_dir(source)
        .map_err(|error| format!("Failed to read directory '{}': {}", source.display(), error))?
    {
        let entry = entry_result.map_err(|error| {
            format!(
                "Failed to read directory entry '{}': {}",
                source.display(),
                error
            )
        })?;
        let entry_type = entry.file_type().map_err(|error| {
            format!(
                "Failed to inspect directory entry '{}': {}",
                entry.path().display(),
                error
            )
        })?;
        let target = destination.join(entry.file_name());
        if entry_type.is_dir() {
            copy_dir_recursive_blocking(&entry.path(), &target)?;
        } else if entry_type.is_file() {
            std_fs::copy(entry.path(), &target).map_err(|error| {
                format!(
                    "Failed to copy '{}' to '{}': {}",
                    entry.path().display(),
                    target.display(),
                    error
                )
            })?;
        } else {
            return Err(format!(
                "Unsupported file type in snapshot source '{}'",
                entry.path().display()
            ));
        }
    }
    Ok(())
}

fn remove_dir_if_exists_blocking(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_file() {
        return std_fs::remove_file(path)
            .map_err(|error| format!("Failed to remove file '{}': {}", path.display(), error));
    }
    std_fs::remove_dir_all(path)
        .map_err(|error| format!("Failed to remove directory '{}': {}", path.display(), error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ApiConfig, AppState};
    use axum::body::{Body, Bytes};
    use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use axum::http::{HeaderMap, HeaderValue};
    use futures_util::stream;
    use next_plaid::filtering;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    #[cfg(feature = "model")]
    fn build_test_state(index_dir: PathBuf) -> AppState {
        AppState::with_model_pool(
            ApiConfig {
                index_dir,
                default_top_k: 10,
            },
            None,
            None,
        )
    }

    #[cfg(not(feature = "model"))]
    fn build_test_state(index_dir: PathBuf) -> AppState {
        AppState::new(ApiConfig {
            index_dir,
            default_top_k: 10,
        })
    }

    fn write_metadata_file(index_path: &Path, num_documents: usize) {
        let metadata = json!({
            "num_chunks": 0,
            "nbits": 4,
            "num_partitions": 1,
            "num_embeddings": 0,
            "avg_doclen": 0.0,
            "num_documents": num_documents,
            "embedding_dim": 0,
            "next_plaid_compatible": true
        });
        std_fs::create_dir_all(index_path).expect("index path should exist");
        std_fs::write(
            index_path.join("metadata.json"),
            serde_json::to_vec(&metadata).expect("metadata should serialize"),
        )
        .expect("metadata file should persist");
    }

    #[test]
    fn metadata_value_condition_uses_in_clause_for_multiple_values() {
        let (condition, parameters) = build_metadata_value_condition(
            "relative_path",
            &["src/lib.rs".to_string(), "src/main.rs".to_string()],
        );

        assert_eq!(condition, "\"relative_path\" IN (?, ?)");
        assert_eq!(
            parameters,
            vec![
                serde_json::Value::String("src/lib.rs".to_string()),
                serde_json::Value::String("src/main.rs".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn relative_path_delete_skips_legacy_metadata_without_project_paths() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let state = Arc::new(build_test_state(temp_dir.path().to_path_buf()));
        let index_path = state.index_path("idx");
        let index_path_str = index_path
            .to_str()
            .expect("index path should be valid UTF-8");
        filtering::create(index_path_str, &[json!({"name": "src/lib.rs"})], &[0])
            .expect("legacy metadata should exist");

        let deleted = delete_existing_relative_paths_after_repair_locked(
            state,
            "idx",
            &["src/lib.rs".to_string()],
        )
        .await
        .expect("legacy metadata should be skipped");

        assert_eq!(deleted, 0);
    }

    #[test]
    fn dirty_marker_path_sanitizes_index_name() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let state = build_test_state(temp_dir.path().to_path_buf());
        let marker = dirty_marker_path(&state, "repo/name:prod", "job-1");

        assert!(
            marker.ends_with("_dirty/repo_name_prod-job-1.json"),
            "unexpected marker path: {}",
            marker.display()
        );
    }

    #[tokio::test]
    async fn recovery_restores_snapshot_for_interrupted_running_job() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let state = Arc::new(build_test_state(temp_dir.path().to_path_buf()));
        let index_dir = state.index_path("idx");
        fs::create_dir_all(&index_dir)
            .await
            .expect("index dir should exist");
        fs::write(
            index_dir.join("config.json"),
            br#"{"nbits":4,"batch_size":50000,"start_from_scratch":999}"#,
        )
        .await
        .expect("config should exist");
        fs::write(index_dir.join("state.txt"), b"current")
            .await
            .expect("current state should exist");

        let snapshot_dir = snapshot_path(&state, "job-running");
        fs::create_dir_all(&snapshot_dir)
            .await
            .expect("snapshot dir should exist");
        fs::write(
            snapshot_dir.join("config.json"),
            br#"{"nbits":4,"batch_size":50000,"start_from_scratch":999}"#,
        )
        .await
        .expect("snapshot config should exist");
        fs::write(snapshot_dir.join("state.txt"), b"snapshot")
            .await
            .expect("snapshot state should exist");

        let manifest = ProjectSyncJobManifest {
            job_id: "job-running".to_string(),
            index_name: "idx".to_string(),
            status: ProjectSyncJobStatus::Running,
            declared_bytes: 7,
            uploaded_bytes: 7,
            content_type: PROJECT_SYNC_CONTENT_TYPE.to_string(),
            reserved_bytes: true,
            error: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        write_manifest(&state, &manifest)
            .await
            .expect("manifest should persist");
        fs::write(spool_path(&state, "job-running"), b"content")
            .await
            .expect("spool should persist");

        recover_project_sync_jobs(state.clone())
            .await
            .expect("recovery should restore snapshot");

        let recovered_manifest = read_manifest(&state, "job-running")
            .await
            .expect("manifest should still exist");
        assert_eq!(recovered_manifest.status, ProjectSyncJobStatus::Failed);
        assert!(recovered_manifest
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("rolled back from snapshot"));
        assert_eq!(pending_ingest_bytes(&state), 0);

        let recovered_state = fs::read_to_string(index_dir.join("state.txt"))
            .await
            .expect("restored state should exist");
        assert_eq!(recovered_state, "snapshot");
        assert!(!snapshot_path(&state, "job-running").exists());
    }

    #[cfg(unix)]
    #[test]
    fn restore_project_sync_snapshot_blocking_preserves_live_index_on_snapshot_copy_failure() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let root = temp_dir.path();
        let index_path = root.join("idx");
        let snapshot_path = root.join("snapshot");
        let unsupported_path = snapshot_path.join("unsupported-link");

        write_metadata_file(&index_path, 1);
        std_fs::write(index_path.join("state.txt"), b"live").expect("live state should exist");

        write_metadata_file(&snapshot_path, 1);
        std_fs::write(snapshot_path.join("state.txt"), b"snapshot")
            .expect("snapshot state should exist");
        std::os::unix::fs::symlink(index_path.join("state.txt"), &unsupported_path)
            .expect("symlink should exist");

        let error = restore_project_sync_snapshot_blocking(&snapshot_path, &index_path)
            .expect_err("snapshot copy should fail for unsupported file type");

        assert!(
            error.contains("Unsupported file type in snapshot source"),
            "unexpected error: {}",
            error
        );

        let live_state = std_fs::read_to_string(index_path.join("state.txt"))
            .expect("live index should remain readable");
        assert_eq!(live_state, "live");
    }

    #[tokio::test]
    async fn project_sync_postflight_repairs_count_mismatch() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let index_dir = temp_dir.path().join("idx");
        let index_path = index_dir
            .to_str()
            .expect("index path should be valid UTF-8")
            .to_string();
        write_metadata_file(&index_dir, 1);
        filtering::create(
            &index_path,
            &[json!({ "kind": "doc-0" }), json!({ "kind": "doc-1" })],
            &[0, 1],
        )
        .expect("metadata db should persist");

        let repair_applied = run_project_sync_postflight_locked(&index_path)
            .await
            .expect("postflight should repair mismatch");
        let after_check = verify_index_db_sync_locked(&index_path)
            .await
            .expect("verify should succeed");

        assert!(repair_applied);
        assert!(after_check.in_sync);
        assert_eq!(after_check.index_count, 1);
        assert_eq!(after_check.db_count, 1);
    }

    #[test]
    fn job_lock_rebuilds_expired_entry() {
        let job_id = format!("job-lock-{}", uuid::Uuid::new_v4());
        let first_lock = job_lock(&job_id);
        let first_weak = Arc::downgrade(&first_lock);

        drop(first_lock);
        assert!(first_weak.upgrade().is_none());

        let second_lock = job_lock(&job_id);
        let locks = JOB_LOCKS
            .get()
            .expect("job lock registry should be initialized");
        let guard = locks
            .lock()
            .expect("job lock registry should not be poisoned");
        let stored_lock = guard
            .get(&job_id)
            .and_then(Weak::upgrade)
            .expect("job lock entry should be rebuilt");

        assert!(Arc::ptr_eq(&second_lock, &stored_lock));
    }

    #[tokio::test]
    async fn upload_timeout_resets_manifest_to_created_and_cleans_spool() {
        let temp_dir = TempDir::new().expect("temp dir should exist");
        let state = Arc::new(build_test_state(temp_dir.path().to_path_buf()));
        let job_id = "job-upload-timeout";
        let declared_bytes = 10_u64;
        let manifest = ProjectSyncJobManifest {
            job_id: job_id.to_string(),
            index_name: "idx".to_string(),
            status: ProjectSyncJobStatus::Created,
            declared_bytes,
            uploaded_bytes: 0,
            content_type: PROJECT_SYNC_CONTENT_TYPE.to_string(),
            reserved_bytes: true,
            error: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        };
        reserve_pending_ingest_bytes(&state, declared_bytes);
        write_manifest(&state, &manifest)
            .await
            .expect("manifest should persist");

        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static(PROJECT_SYNC_CONTENT_TYPE),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&declared_bytes.to_string())
                .expect("content-length header should be valid"),
        );

        let body = Body::from_stream(stream::unfold(0_u8, |step| async move {
            match step {
                0 => Some((Ok::<Bytes, std::io::Error>(Bytes::from_static(b"12345")), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Some((Ok::<Bytes, std::io::Error>(Bytes::from_static(b"67890")), 2))
                }
                _ => None,
            }
        }));

        let result = upload_project_sync_job_with_idle_timeout(
            state.clone(),
            job_id,
            headers,
            body,
            Duration::from_millis(50),
        )
        .await;

        match result {
            Err(ApiError::RequestTimeout(message)) => {
                assert!(
                    message.contains("timed out"),
                    "unexpected timeout message: {}",
                    message
                );
            }
            _ => panic!("expected request timeout"),
        }

        let manifest = read_manifest(&state, job_id)
            .await
            .expect("manifest should still exist");
        assert_eq!(manifest.status, ProjectSyncJobStatus::Created);
        assert_eq!(manifest.uploaded_bytes, 0);
        assert!(manifest.reserved_bytes);
        assert_eq!(pending_ingest_bytes(&state), declared_bytes);
        assert!(manifest
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("timed out"));
        assert!(!spool_path(&state, job_id).exists());
        assert!(!temp_spool_path(&state, job_id).exists());
    }
}
