use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

mod library;
mod qobuz;
mod queue;
mod queue_handlers;

use library::Library;
use qobuz::QobuzSource;
use queue::PlayQueue;

use axum::{extract::Query, Json};
use serde::Deserialize;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[derive(Clone)]
struct AppState {
    qobuz: Arc<Mutex<Option<QobuzSource>>>,
    library: Arc<Library>,
    queue: Arc<Mutex<PlayQueue>>,
}

#[derive(Deserialize)]
struct DownloadParams {
    track_id: i32,
    format_id: Option<i32>,
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
        queue: Arc::new(Mutex::new(PlayQueue::default())),
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
        .route("/stream_from_track", get(queue_handlers::stream_from_track))
        .route("/queue", get(queue_handlers::queue_status))
        .route("/queue/next", axum::routing::post(queue_handlers::queue_next))
        .route("/queue/previous", axum::routing::post(queue_handlers::queue_previous))
        .route("/queue/next_stream", get(queue_handlers::queue_next_stream))
        .route("/queue/previous_stream", get(queue_handlers::queue_previous_stream))
        .route("/queue/clear", axum::routing::post(queue_handlers::queue_clear))
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
        let service = service.blocking_lock();
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
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let format_id = params.format_id.unwrap_or(6);

    let path = if let Ok(Some(p)) = state.library.track_path(params.track_id).await {
        if std::path::Path::new(&p).exists() {
            p
        } else {
            download_track_to_library(
                state.qobuz.lock().await.as_ref().ok_or((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Qobuz not available".into(),
                ))?,
                params.track_id,
                format_id,
                state.library.clone(),
                None,
                None,
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
        }
    } else {
        download_track_to_library(
            state.qobuz.lock().await.as_ref().ok_or((
                StatusCode::SERVICE_UNAVAILABLE,
                "Qobuz not available".into(),
            ))?,
            params.track_id,
            format_id,
            state.library.clone(),
            None,
            None,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
    };

    let mut q = std::collections::HashMap::new();
    q.insert("path".to_string(), path);
    stream_local(Query(q), headers).await
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
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let track_id = params.track_id;
    let format_id = params.format_id.unwrap_or(6); // 6 = FLAC

    let guard = state.qobuz.lock().await;
    let source = guard
        .as_ref()
        .ok_or((axum::http::StatusCode::SERVICE_UNAVAILABLE, "Qobuz not authenticated".into()))?;

    // 1. Get stream URL + track metadata
    let (file_url, track) = {
        let mut service = source.service().blocking_lock();
        let file_url = service
            .get_track_file_url(track_id, format_id)
            .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, format!("stream url: {}", e)))?;
        let track = service
            .get_track(track_id)
            .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, format!("track meta: {}", e)))?;
        (file_url, track)
    };

    let url = file_url
        .url
        .ok_or((axum::http::StatusCode::BAD_GATEWAY, "empty stream url".into()))?;

    // 2. Download the file
    let output_dir = PathBuf::from("library");
    tokio::fs::create_dir_all(&output_dir).await.map_err(|e| {
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("mkdir: {}", e))
    })?;

    let temp_path = output_dir.join(format!("{}.flac", track_id));
    let client = reqwest::Client::new();
    let mut resp = client.get(&url).send().await.map_err(|e| {
        (axum::http::StatusCode::BAD_GATEWAY, format!("download: {}", e))
    })?;

    if !resp.status().is_success() {
        return Err((
            axum::http::StatusCode::BAD_GATEWAY,
            format!("HTTP {}", resp.status()),
        ));
    }

    let mut file = File::create(&temp_path).await.map_err(|e| {
        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("create file: {}", e))
    })?;

    while let Some(chunk) = resp.chunk().await.map_err(|e| {
        (axum::http::StatusCode::BAD_GATEWAY, format!("chunk: {}", e))
    })? {
        file.write_all(&chunk).await.map_err(|e| {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("write: {}", e))
        })?;
    }
    file.flush().await.ok();
    drop(file);

    // 3. Tag + embed cover (blocking)
    let final_path = {
        let temp_path_clone = temp_path.clone();
        tokio::task::spawn_blocking(move || {
            use lofty::file::{AudioFile, TaggedFileExt};
            use lofty::picture::{MimeType, Picture, PictureType};
            use lofty::tag::{Accessor, Tag};

            let mut tagged = lofty::read_from_path(&temp_path_clone)
                .map_err(|e| format!("lofty read: {}", e))?;

            let mut tag = tagged.primary_tag_mut().cloned().unwrap_or_else(Tag::new);
            if let Some(title) = track.title.as_deref() {
                tag.set_title(title.to_string());
            }
            if let Some(artist) = track.performer.as_ref().and_then(|p| p.name.as_deref()) {
                tag.set_artist(artist.to_string());
            }
            if let Some(album) = track.album.as_ref().and_then(|a| a.title.as_deref()) {
                tag.set_album(album.to_string());
            }
            if let Some(track_num) = track.track_number {
                tag.set_track(track_num as u32);
            }

            // Cover art
            if let Some(image_url) = track
                .album
                .as_ref()
                .and_then(|a| a.image.as_ref())
                .and_then(|img| img.large.as_deref().or(img.small.as_deref()))
            {
                if let Ok(bytes) = reqwest::blocking::get(image_url).and_then(|r| r.bytes()) {
                    if let Ok(picture) = Picture::new_unchecked(
                        PictureType::CoverFront,
                        Some(MimeType::Jpeg),
                        None,
                        bytes.to_vec(),
                    ) {
                        tag.set_picture(0, picture);
                    }
                }
            }

            tagged.insert_tag(tag);
            tagged
                .save_to_path(&temp_path_clone)
                .map_err(|e| format!("lofty save: {}", e))?;

            // Nice filename
            let artist = track
                .performer
                .as_ref()
                .and_then(|p| p.name.as_deref())
                .unwrap_or("Unknown");
            let title = track.title.as_deref().unwrap_or("Unknown");
            let nice = format!("{} - {}.flac", sanitize(artist), sanitize(title));
            let nice_path = temp_path_clone.with_file_name(nice);
            if nice_path != temp_path_clone {
                std::fs::rename(&temp_path_clone, &nice_path).ok();
                Ok(nice_path)
            } else {
                Ok(temp_path_clone)
            }
        })
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("join: {}", e)))?
        .unwrap_or(temp_path)
    };

    // 4. Dynamic range score
    let dr_score = dr_score::score_and_log(&final_path, track_id);

    Ok(Json(serde_json::json!({
        "path": final_path,
        "track_id": track_id,
        "format_id": format_id,
        "dr_score": dr_score,
        "status": "ok"
    })))
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == ' ' || c == '-' { c } else { '_' })
        .collect::<String>()
        .trim()
        .to_string()
}

pub async fn download_track_to_library(
    source: &QobuzSource,
    track_id: i32,
    format_id: i32,
    library: Arc<Library>,
    album_dir: Option<std::path::PathBuf>,
    sequence: Option<usize>,
) -> Result<String, String> {
    use lofty::config::WriteOptions;
    use lofty::file::{AudioFile, TaggedFileExt};
    use lofty::picture::Picture;
    use lofty::tag::{Accessor, Tag, TagType};
    use std::io::Cursor;

    let service = source.service();
    let output_dir = album_dir.unwrap_or_else(|| std::path::PathBuf::from("library"));
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(|e| e.to_string())?;

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

    let url = file_url
        .url
        .ok_or_else(|| "No streaming URL returned".to_string())?;

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("download request: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("read body: {}", e))?;
    let temp_path = output_dir.join(format!("{}.flac", track_id));
    tokio::fs::write(&temp_path, &bytes)
        .await
        .map_err(|e| format!("write flac: {}", e))?;

    let (final_path, title, artist, album, track_number, duration) = tokio::task::spawn_blocking({
        let service = service.clone();
        let temp_path = temp_path.clone();
        let output_dir = output_dir.clone();
        let sequence = sequence;
        move || -> Result<(std::path::PathBuf, Option<String>, Option<String>, Option<String>, Option<i32>, Option<f64>), String> {
            let service = service.blocking_lock();
            let track = service
                .get_track(track_id)
                .map_err(|e| format!("get_track: {}", e))?;

            let mut tagged = lofty::read_from_path(&temp_path)
                .map_err(|e| format!("lofty read: {}", e))?;

            if tagged.primary_tag().is_none() {
                tagged.insert_tag(Tag::new(TagType::VorbisComments));
            }
            let tag = tagged.primary_tag_mut().unwrap();

            let title_val = track.title.clone();
            if let Some(ref t) = title_val {
                tag.set_title(t.clone());
            }

            let artist_val = track
                .performer
                .as_ref()
                .and_then(|p| p.name.clone())
                .or_else(|| {
                    track.album
                        .as_ref()
                        .and_then(|a| a.artist.as_ref())
                        .and_then(|a| a.name.clone())
                });
            if let Some(ref a) = artist_val {
                tag.set_artist(a.clone());
            }

            let album_val = track.album.as_ref().and_then(|a| a.title.clone());
            if let Some(ref a) = album_val {
                tag.set_album(a.clone());
            }

            let track_number_val = track.track_number;
            if let Some(num) = track_number_val {
                tag.set_track(num as u32);
            }

            let duration_val = track.duration.map(|d| d as f64);

            if let Some(image_url) = track
                .album
                .as_ref()
                .and_then(|a| a.image.as_ref())
                .and_then(|img| img.large.as_deref().or(img.thumbnail.as_deref()))
            {
                if let Ok(resp) = reqwest::blocking::Client::new().get(image_url).send() {
                    if resp.status().is_success() {
                        if let Ok(cover_bytes) = resp.bytes() {
                            if let Ok(picture) = Picture::from_reader(&mut Cursor::new(&cover_bytes[..])) {
                                tag.set_picture(0, picture);
                            }
                        }
                    }
                }
            }

            tagged
                .save_to_path(&temp_path, WriteOptions::default())
                .map_err(|e| format!("lofty save: {}", e))?;

            let title_str = title_val.as_deref().unwrap_or("Unknown");
            let seq = sequence.unwrap_or(0);
            let safe_name = format!("{:02}. {} - {}.flac", seq, track_id, sanitize(title_str));
            
            let final_path = output_dir.join(safe_name);

            if final_path != temp_path {
                let _ = std::fs::rename(&temp_path, &final_path);
            }

            Ok((final_path, title_val, artist_val, album_val, track_number_val, duration_val))
        }
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e)?;

    library
        .upsert_track(
            final_path.to_string_lossy().as_ref(),
            title.as_deref(),
            artist.as_deref(),
            None,
            album.as_deref(),
            None,
            Some(track_id),
            track_number,
            duration,
            Some("flac"),
            None,
            None,
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(final_path.to_string_lossy().to_string())
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == ' ' || c == '-' {
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
        let service = service.blocking_lock();
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
        let service = service.blocking_lock();
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
pub async fn stream_local(
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let path = params
        .get("path")
        .ok_or((StatusCode::BAD_REQUEST, "missing path".into()))?;

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    let file_size = bytes.len() as u64;

    let mut status = StatusCode::OK;
    let mut start: u64 = 0;
    let mut end: u64 = file_size.saturating_sub(1);
    let mut content_length = file_size;

    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            tracing::info!("Range request: {}", range_str);
            if let Some(range) = range_str.strip_prefix("bytes=") {
                let parts: Vec<&str> = range.split('-').collect();
                if parts.len() == 2 {
                    if parts[0].is_empty() && !parts[1].is_empty() {
                        let suffix: u64 = parts[1].parse().unwrap_or(0);
                        start = file_size.saturating_sub(suffix);
                    } else {
                        if !parts[0].is_empty() {
                            start = parts[0].parse().unwrap_or(0);
                        }
                        if !parts[1].is_empty() {
                            end = parts[1].parse().unwrap_or(end);
                        }
                    }

                    if start >= file_size {
                        return Err((StatusCode::RANGE_NOT_SATISFIABLE, "invalid range".into()));
                    }
                    end = end.min(file_size.saturating_sub(1));
                    if start > end {
                        return Err((StatusCode::RANGE_NOT_SATISFIABLE, "invalid range".into()));
                    }
                    content_length = end - start + 1;
                    status = StatusCode::PARTIAL_CONTENT;
                    tracing::info!("Serving range {}-{} / {}", start, end, file_size);
                }
            }
        }
    }

    let slice = bytes[start as usize..=end as usize].to_vec();

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "audio/flac")
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            "Access-Control-Expose-Headers",
            "Content-Range, Accept-Ranges, Content-Length, Content-Type",
        )
        .header(header::CACHE_CONTROL, "no-cache");

    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        );
    }

    Ok(builder.body(Body::from(slice)).unwrap())
}

async fn library_ai_search(
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "message": "AI search placeholder" }))
}
