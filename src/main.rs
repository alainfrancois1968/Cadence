use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

mod library;
mod sources;

use library::Library;
use sources::qobuz::QobuzSource;

#[derive(Clone)]
struct AppState {
    qobuz: Arc<Mutex<Option<QobuzSource>>>,
    library: Arc<Library>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    dotenvy::dotenv().ok();

    let db_path = std::env::var("DATABASE_URL").unwrap_or_else(|_| "cadence.db".into());
    let library = Arc::new(Library::new(&db_path).await?);
    let qobuz = Arc::new(Mutex::new(None));

    {
        let mut q = qobuz.lock().await;
        match QobuzSource::new() {
            Ok(src) => {
                info!("Qobuz authenticated successfully");
                *q = Some(src);
            }
            Err(e) => warn!("Qobuz authentication failed (continuing without it): {}", e),
        }
    }

    let state = AppState {
        qobuz: qobuz.clone(),
        library: library.clone(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/qobuz/status", get(qobuz_status))
        .route("/qobuz/search", get(qobuz_search))
        .route("/qobuz/stream", get(qobuz_stream))
        .route("/qobuz/download", get(qobuz_download))
        .route("/qobuz/album-id", get(qobuz_album_id))
        .route("/qobuz/queue-albums", get(qobuz_queue_albums))
        .route("/qobuz/queue-artists", get(qobuz_queue_artists))
        .route(
            "/qobuz/queue-albums-from-artists",
            get(qobuz_queue_albums_from_artists),
        )
        .route(
            "/qobuz/download-artist-albums",
            get(qobuz_download_artist_albums),
        )
        .route("/library/search", get(library_search))
        .route("/library/scan", get(library_scan))
        .route("/library/stream_local", get(stream_local))
        .route("/library/stream_qobuz", get(stream_qobuz))
        .route("/library/ai-search", get(library_ai_search))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Cadence Music Server listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn qobuz_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let q = state.qobuz.lock().await;
    Json(serde_json::json!({
        "status": "ok",
        "authenticated": q.is_some()
    }))
}

#[derive(Deserialize)]
struct SearchParams {
    q: Option<String>,
    limit: Option<u32>,
}

async fn qobuz_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let query = params.q.clone().unwrap_or_default();
    let limit = params.limit.unwrap_or(20) as i32;

    let service = source.service();
    let result = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        let albums = service
            .search_albums(&query, Some(limit), None)
            .map_err(|e| e.to_string())?;
        let tracks = service
            .search_tracks(&query, Some(limit), None)
            .map_err(|e| e.to_string())?;
        Ok::<_, String>((albums, tracks))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "query": params.q,
        "albums": result.0,
        "tracks": result.1
    })))
}

#[derive(Deserialize)]
struct StreamParams {
    track_id: i32,
    format_id: Option<i32>,
}

async fn qobuz_stream(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let format_id = params.format_id.unwrap_or(6);

    let service = source.service();
    let file_url = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        service
            .get_track_file_url(params.track_id, format_id)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "track_id": params.track_id,
        "format_id": format_id,
        "duration": file_url.duration,
        "mime_type": file_url.mime_type,
        "url": file_url.url
    })))
}

#[derive(Deserialize)]
struct DownloadParams {
    track_id: Option<i32>,
    album_id: Option<String>,
    format_id: Option<i32>,
}

async fn qobuz_download(
    State(state): State<AppState>,
    Query(params): Query<DownloadParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let format_id = params.format_id.unwrap_or(6);

    if let Some(track_id) = params.track_id {
        let path = download_track_to_library(source, track_id, format_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
        return Ok(Json(serde_json::json!({
            "type": "track",
            "track_id": track_id,
            "path": path
        })));
    }

    if let Some(album_id) = params.album_id {
        // Album download: get album then download each track by id
        // (adjust field access once you inspect the actual Album type)
        let service = source.service();
        let album_id_clone = album_id.clone();
        let _album = tokio::task::spawn_blocking(move || {
            let mut service = service.blocking_lock();
            service
                .get_album(&album_id_clone, Some("tracks"))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        return Ok(Json(serde_json::json!({
            "type": "album",
            "album_id": album_id,
            "note": "Track list download not fully wired – inspect Album fields"
        })));
    }

    Err((
        StatusCode::BAD_REQUEST,
        "track_id or album_id required".into(),
    ))
}

async fn download_track_to_library(
    source: &QobuzSource,
    track_id: i32,
    format_id: i32,
) -> Result<String, String> {
    use lofty::config::WriteOptions;
    use lofty::file::{AudioFile, TaggedFileExt};
    use lofty::tag::{Accessor, Tag, TagType};

    let output_dir = std::path::PathBuf::from(
        std::env::var("LIBRARY_PATH").unwrap_or_else(|_| "library".into()),
    )
    .join("Qobuz");
    std::fs::create_dir_all(&output_dir).map_err(|e| e.to_string())?;

    let service = source.service();
    let file_url = tokio::task::spawn_blocking({
        let service = service.clone();
        move || {
            let mut service = service.blocking_lock();
            service
                .get_track_file_url(track_id, format_id)
                .map_err(|e| e.to_string())
        }
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e)?;

    let url = file_url.url.ok_or("No stream URL")?;
    let temp_path = output_dir.join(format!("{}.flac", track_id));

    let client = reqwest::Client::new();
    let mut resp = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(|e| e.to_string())?;
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
        use tokio::io::AsyncWriteExt;
        file.write_all(&chunk).await.map_err(|e| e.to_string())?;
    }
    drop(file);

    let final_path = tokio::task::spawn_blocking({
        let service = source.service();
        let temp_path = temp_path.clone();
        let output_dir = output_dir.clone();
        move || {
            let mut service = service.blocking_lock();
            let track = service.get_track(track_id).map_err(|e| e.to_string())?;
            let title = track.title.unwrap_or_else(|| format!("Track {}", track_id));
            let artist = track
                .performer
                .as_ref()
                .and_then(|p| p.name.clone())
                .unwrap_or_else(|| "Unknown Artist".into());
            let album = track
                .album
                .as_ref()
                .and_then(|a| a.title.clone())
                .unwrap_or_else(|| "Unknown Album".into());
            let artist_id = track.performer.as_ref().and_then(|p| p.id);
            let album_id = track
                .album
                .as_ref()
                .and_then(|a| a.id.clone())
                .unwrap_or_default();
            let genre = track
                .album
                .as_ref()
                .and_then(|a| a.genre.as_ref())
                .and_then(|g| g.name.clone());

            if let Ok(mut tagged) = lofty::read_from_path(&temp_path) {
                let mut tag = tagged
                    .primary_tag_mut()
                    .cloned()
                    .unwrap_or_else(|| Tag::new(TagType::VorbisComments));
                tag.set_title(title.clone());
                tag.set_artist(artist.clone());
                tag.set_album(album.clone());
                if let Some(g) = &genre {
                    tag.set_genre(g.clone());
                }
                tagged.insert_tag(tag);
                let _ = tagged.save_to_path(&temp_path, WriteOptions::default());
            }

            let year = track
                .album
                .as_ref()
                .and_then(|a| a.release_date_original.as_ref())
                .and_then(|d| d.split('-').next())
                .unwrap_or("0000")
                .to_string();
            let track_num = track.track_number.unwrap_or(0);

            let artist_folder = format!("{} - {}", artist_id.unwrap_or(0), sanitize(&artist));
            let album_folder = format!("{} - {}", album_id, sanitize(&album));
            let dir = output_dir
                .join(&artist_folder)
                .join(&year)
                .join(&album_folder);
            let _ = std::fs::create_dir_all(&dir);

            let filename = format!("{:02}. {} - {}.flac", track_num, track_id, sanitize(&title));
            let new_path = dir.join(filename);
            let _ = std::fs::rename(&temp_path, &new_path);

            Ok::<_, String>(new_path.display().to_string())
        }
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok(final_path)
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == ' ' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim()
        .to_string()
}

#[derive(Deserialize)]
struct AlbumIdParams {
    q: String,
}

async fn qobuz_album_id(
    State(state): State<AppState>,
    Query(params): Query<AlbumIdParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let service = source.service();
    let query = params.q.clone();

    let album_id = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        let results = service
            .search_albums(&query, Some(1), None)
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(
            results
                .items
                .and_then(|i| i.into_iter().next())
                .and_then(|a| a.id),
        )
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({ "album_id": album_id })))
}

#[derive(Deserialize)]
struct QueueAlbumsParams {
    artist_id: i32,
    limit: Option<i32>,
}

async fn qobuz_queue_albums(
    State(_state): State<AppState>,
    Query(_params): Query<QueueAlbumsParams>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "queued": 0 }))
}

#[derive(Deserialize)]
struct QueueArtistsParams {
    q: String,
    limit: Option<i32>,
}

async fn qobuz_queue_artists(
    State(_state): State<AppState>,
    Query(_params): Query<QueueArtistsParams>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "queued": 0 }))
}

async fn qobuz_queue_albums_from_artists(
    State(_state): State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "queued": 0 }))
}

async fn qobuz_download_artist_albums(
    State(state): State<AppState>,
    Query(params): Query<QueueAlbumsParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let limit = params.limit.unwrap_or(10);
    let service = source.service();

    let releases = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        service
            .get_release_list(params.artist_id, Some(limit), None)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "albums": releases.items
    })))
}

#[derive(Deserialize)]
struct LibrarySearchParams {
    q: String,
}

async fn library_search(
    State(state): State<AppState>,
    Query(params): Query<LibrarySearchParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let results = state
        .library
        .search(&params.q)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "query": params.q,
        "tracks": results
    })))
}

async fn library_scan(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let lib_path = std::env::var("LIBRARY_PATH").unwrap_or_else(|_| "library".into());
    // Adjust method name if your Library uses a different one
    let scanned = state
        .library
        .scan_directory(&lib_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "scanned": scanned })))
}

/// Full range-aware FLAC streaming.
/// Supports `Range: bytes=start-end` and returns 206 Partial Content.
async fn stream_local(
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let path = params
        .get("path")
        .ok_or((StatusCode::BAD_REQUEST, "missing path".into()))?;
    let file_path = std::path::Path::new(path);

    let mut file = tokio::fs::File::open(file_path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let metadata = file
        .metadata()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let file_size = metadata.len();

    let mut status = StatusCode::OK;
    let mut start: u64 = 0;
    let mut end: u64 = file_size.saturating_sub(1);
    let mut content_length = file_size;

    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            if let Some(range) = range_str.strip_prefix("bytes=") {
                let parts: Vec<&str> = range.split('-').collect();
                if parts.len() == 2 {
                    if !parts[0].is_empty() {
                        start = parts[0].parse().unwrap_or(0);
                    }
                    if !parts[1].is_empty() {
                        end = parts[1].parse().unwrap_or(end);
                    }
                    if start > end || start >= file_size {
                        return Err((StatusCode::RANGE_NOT_SATISFIABLE, "invalid range".into()));
                    }
                    end = end.min(file_size.saturating_sub(1));
                    content_length = end - start + 1;
                    status = StatusCode::PARTIAL_CONTENT;

                    file.seek(SeekFrom::Start(start))
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
            }
        }
    }

    let limited = file.take(content_length);
    let stream = tokio_util::io::ReaderStream::new(limited);
    let body = Body::from_stream(stream);

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "audio/flac")
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::ACCEPT_RANGES, "bytes");

    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        );
    }

    Ok(builder.body(body).unwrap())
}

async fn stream_qobuz(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let format_id = params.format_id.unwrap_or(6);

    // 1. Try to find an already-downloaded local file for this track_id
    if let Ok(Some(path)) = state.library.find_path_by_track_id(params.track_id).await {
        // Re-use the range-aware local streamer
        let mut q = std::collections::HashMap::new();
        q.insert("path".to_string(), path);
        return stream_local(Query(q), headers).await;
    }

    // 2. Fall back to a live Qobuz stream URL (no seeking yet)
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;

    let service = source.service();
    let file_url = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        service
            .get_track_file_url(params.track_id, format_id)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let url = file_url
        .url
        .ok_or((StatusCode::NOT_FOUND, "No stream URL from Qobuz".into()))?;

    // Simple redirect to the Qobuz CDN URL
    Ok(Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, url)
        .body(Body::empty())
        .unwrap())
}

async fn library_ai_search(
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "message": "AI search placeholder" }))
}
