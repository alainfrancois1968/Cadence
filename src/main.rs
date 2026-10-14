use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
    Router,
    Json
};
use serde::Deserialize;
use std::sync::Arc;
//use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use lofty::file::AudioFile;

mod library;
mod qobuz;
mod queue;
mod queue_handlers;
mod dr_score;

use library::Library;
use qobuz::QobuzSource;
use queue::PlayQueue;

use std::env;

mod eq;
mod eq_fir;

use std::path::PathBuf;

use math_audio_iir_fir::{Biquad, BiquadFilterType, Fir};


#[derive(Clone)]
struct AppState {
    qobuz: Arc<Mutex<Option<QobuzSource>>>,
    library: Arc<Library>,
    queue: Arc<Mutex<PlayQueue>>,
}

#[derive(Deserialize)]
struct DownloadParams {
    track_id: Option<i32>,
    album_id: Option<String>,
    format_id: Option<i32>,
}

#[derive(Deserialize)]
struct UpsampleTrackParams {
    track_id: i32,
}

#[derive(Deserialize)]
struct UpsampleAlbumParams {
    album_id: String,
}

#[derive(Deserialize)]
struct EqPresetParams {
    path: String,
    seq: String, // "00", "01", ...
}

#[derive(Deserialize)]
struct GlobalEqParams {
    seq: String,
}

/// Kill any previous process listening on port 3000 so a new run can bind.
fn kill_previous_instance() {
    if let Ok(output) = std::process::Command::new("lsof")
        .args(["-ti", ":3000"])
        .output()
    {
        let pids = String::from_utf8_lossy(&output.stdout);
        for pid in pids.lines().map(str::trim).filter(|s| !s.is_empty()) {
            let _ = std::process::Command::new("kill")
                .args(["-9", pid])
                .status();
            tracing::info!("Killed previous process on port 3000 (pid {})", pid);
        }
    }
}

#[tokio::main]

async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    kill_previous_instance();

    if !std::path::Path::new("eq_global.json").exists() {
        let _ = std::fs::write("eq_global.json", "{}");
    }

    dotenvy::dotenv().ok();

    let db_path = std::env::var("DATABASE_URL").unwrap_or_else(|_| "cadence.db".into());
    let library = Arc::new(Library::new(&db_path).await?);
    let qobuz = Arc::new(Mutex::new(None));

    {
        let mut q = qobuz.lock().await;
        match QobuzSource::new().await {
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
        .route("/stream_from_track_gapless", get(queue_handlers::stream_from_track))
        .route("/queue", get(queue_handlers::queue_status))
        .route("/queue/next", axum::routing::post(queue_handlers::queue_next))
        .route("/queue/previous", axum::routing::post(queue_handlers::queue_previous))
        .route("/queue/next_stream", get(queue_handlers::queue_next_stream))
        .route("/queue/previous_stream", get(queue_handlers::queue_previous_stream))
        .route("/queue/clear", axum::routing::post(queue_handlers::queue_clear))
        .route("/upsample_track", get(upsample_track))
        .route("/upsample_album", get(upsample_album))
        .route("/library/eq/apply", post(apply_eq_preset))
        .route("/library/eq/presets", get(list_eq_presets))
        .route("/library/eq/global", post(set_global_eq))
        .route("/library/eq/global", get(get_global_eq))
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
    stream_local(State(state), Query(q), headers).await
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

    // ---------- Single track ----------
    if let Some(track_id) = params.track_id {
        let path = download_track_to_library(
            source,
            track_id,
            format_id,
            state.library.clone(),
            None,
            None,
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        return Ok(Json(serde_json::json!({
            "type": "track",
            "track_id": track_id,
            "path": path,
            "status": "ok"
        })));
    }

    // ---------- Album ----------
    if let Some(album_id) = params.album_id {
        let service = source.service();
        let album_id_clone = album_id.clone();

        let album = tokio::task::spawn_blocking(move || {
            let service = service.blocking_lock();
            service
                .get_album(&album_id_clone, Some("track_ids"))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

        let artist_id = album.artist.as_ref().and_then(|a| a.id).unwrap_or(0);
        let artist_name = album
            .artist
            .as_ref()
            .and_then(|a| a.name.as_deref())
            .unwrap_or("Unknown Artist");
        let year = album
            .released_at
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts, 0)
                    .map(|d| d.format("%Y").to_string())
                    .unwrap_or_else(|| "0000".to_string())
            })
            .unwrap_or_else(|| "0000".to_string());
        let album_title = album.title.as_deref().unwrap_or("Unknown Album");

        let album_dir = std::path::PathBuf::from("library")
            .join("Qobuz")
            .join(format!("{} - {}", artist_id, sanitize(artist_name)))
            .join(&year)
            .join(format!("{} - {}", album_id, sanitize(album_title)));

        tokio::fs::create_dir_all(&album_dir)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Cover
        let cover_path = album_dir.join("cover.jpg");
        if !cover_path.exists() {
            if let Some(image_url) = album
                .image
                .as_ref()
                .and_then(|img| img.large.as_deref().or(img.thumbnail.as_deref()))
            {
                if let Ok(resp) = reqwest::Client::new().get(image_url).send().await {
                    if resp.status().is_success() {
                        if let Ok(bytes) = resp.bytes().await {
                            let _ = tokio::fs::write(&cover_path, &bytes).await;
                        }
                    }
                }
            }
        }

        let track_ids = album.track_ids.unwrap_or_default();
        let mut paths = Vec::new();
        let mut skipped = 0;

        for (idx, tid) in track_ids.iter().enumerate() {
            let sequence = idx + 1;

            // Skip if already on disk
            if let Ok(Some(path)) = state.library.track_path(*tid).await {
                if std::path::Path::new(&path).exists() {
                    skipped += 1;
                    tracing::info!("Track {} already on disk, skipping", tid);
                    paths.push(path);
                    continue;
                }
            }

            match download_track_to_library(
                source,
                *tid,
                format_id,
                state.library.clone(),
                Some(album_dir.clone()),
                Some(sequence),
            )
            .await
            {
                Ok(p) => {
                    paths.push(p);
                }
                Err(e) => tracing::warn!("Failed to download track {}: {}", tid, e),
            }
        }

        return Ok(Json(serde_json::json!({
            "type": "album",
            "album_id": album_id,
            "downloaded": paths.len() - skipped,
            "skipped": skipped,
            "paths": paths,
            "status": "ok"
        })));
    }

    Err((
        StatusCode::BAD_REQUEST,
        "track_id or album_id required".into(),
    ))
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

// Build the output directory for a track/album.
/// library/Qobuz/{artist_id} - {artist}/{year}/{album_id} - {album}/
fn build_output_dir(
    artist_id: &str,
    artist: &str,
    year: &str,
    album_id: &str,
    album: &str,
) -> PathBuf {
    PathBuf::from("library")
        .join("Qobuz")
        .join(format!("{} - {}", sanitize(artist_id), sanitize(artist)))
        .join(sanitize(year))
        .join(format!("{} - {}", sanitize(album_id), sanitize(album)))
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
/// Prefer the upsampled copy under …/upsampled/<filename> when it exists.
fn prefer_upsampled(path: &str) -> String {
    let p = std::path::Path::new(path);
    if let (Some(parent), Some(name)) = (p.parent(), p.file_name()) {
        let up = parent.join("upsampled").join(name);
        if up.exists() {
            return up.to_string_lossy().into_owned();
        }
    }
    path.to_string()
}

async fn stream_local(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let raw_path = params
        .get("path")
        .ok_or((StatusCode::BAD_REQUEST, "missing path".into()))?;

    let track_eq = state.library.get_eq_settings(raw_path).await.ok().flatten();

    let eq_json = if track_eq.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
        std::fs::read_to_string("eq_global.json").ok()
    } else {
        track_eq.clone()
    };

    let eq_active = eq_json
        .as_ref()
        .map(|s| !s.trim().is_empty() && s.trim() != "{}")
        .unwrap_or(false);

    let preferred = prefer_upsampled(raw_path);
    let path = if eq_active {
        // Prefer upsampled when EQ is on, but only if Symphonia can read it.
        if probe_flac_format(&preferred).is_some() {
            preferred
        } else {
            tracing::warn!("upsampled unreadable, using original for EQ: {}", preferred);
            raw_path.clone()
        }
    } else {
        preferred
    };

    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    tracing::info!(
        "raw_path={}, resolved={}, eq_active={}, eq_json={:?}",
        raw_path,
        path,
        eq_active,
        eq_json.as_deref()
    );

    let (body_bytes, content_type, bit_depth, sample_rate) = if let Some(json) = eq_json {
        if json.trim().is_empty() || json.trim() == "{}" {
            let (bits, rate) = probe_flac_format(&path).unwrap_or((0, 0));
            (bytes, "audio/flac", bits, rate)
        } else {
            match serde_json::from_str::<crate::eq::EqSettings>(&json) {
                Ok(settings) if !crate::eq::is_flat(&settings) => {

                match decode_and_eq(&path, &settings) {
                    Ok((wav, rate, bits)) => (wav, "audio/wav", bits, rate),
                    Err(e) => {
                        tracing::warn!("EQ failed, falling back to FLAC: {}", e);
                        let (b, r) = probe_flac_format(&path).unwrap_or((0, 0));
                        (bytes, "audio/flac", b, r)
                    }
                }

                }
                _ => {
                    let (bits, rate) = probe_flac_format(&path).unwrap_or((0, 0));
                    (bytes, "audio/flac", bits, rate)
                }
            }
        }
    } else {
        let (bits, rate) = probe_flac_format(&path).unwrap_or((0, 0));
        (bytes, "audio/flac", bits, rate)
    };

    let file_size = body_bytes.len() as u64;

    let mut status = StatusCode::OK;
    let mut start: u64 = 0;
    let mut end: u64 = file_size.saturating_sub(1);
    let mut content_length = file_size;

    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
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
                }
            }
        }
    }

    let slice = body_bytes[start as usize..=end as usize].to_vec();

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header("X-Bit-Depth", bit_depth.to_string())
        .header("X-Sample-Rate", sample_rate.to_string())
        .header(
            "Access-Control-Expose-Headers",
            "Content-Range, Accept-Ranges, Content-Length, X-Bit-Depth, X-Sample-Rate",
        );

    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        );
    }

    builder
        .body(Body::from(slice))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

fn probe_flac_format(path: &str) -> Option<(u32, u32)> {
    use symphonia::core::formats::FormatOptions;
    use symphonia::core::io::MediaSourceStream;
    use symphonia::core::meta::MetadataOptions;
    use symphonia::core::probe::Hint;
    use std::fs::File;

    let file = File::open(path).ok()?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    hint.with_extension("flac");
    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &FormatOptions::default(), &MetadataOptions::default())
        .ok()?;
    let t = probed.format.tracks().iter().next()?;
    let rate = t.codec_params.sample_rate?;
    let bits = t.codec_params.bits_per_sample.unwrap_or(0);
    Some((bits, rate))
}

/// Decode FLAC bytes, apply EQ, return WAV bytes and sample rate.
fn decode_and_eq(path: &str, settings: &crate::eq::EqSettings) -> Result<(Vec<u8>, u32, u32), String> {
    use symphonia::core::audio::SampleBuffer;
    use symphonia::core::codecs::DecoderOptions;
    use symphonia::core::formats::FormatOptions;
    use symphonia::core::io::MediaSourceStream;
    use symphonia::core::meta::MetadataOptions;
    use symphonia::core::probe::Hint;
    use std::fs::File;

    tracing::info!("decode_and_eq: opening {}", path);

    let file = File::open(path).map_err(|e| format!("open: {}", e))?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    hint.with_extension("flac");

    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &FormatOptions::default(), &MetadataOptions::default())
        .map_err(|e| format!("probe: {}", e))?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.sample_rate.is_some())
        .ok_or("no audio track")?;

    let sample_rate = track.codec_params.sample_rate.unwrap_or(44100);
    let bit_depth = track.codec_params.bits_per_sample.unwrap_or(16);
    let track_id = track.id;

    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|e| format!("decoder: {}", e))?;

    let mut pcm = Vec::new();
    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(_) => break,
        };
        if packet.track_id() != track_id {
            continue;
        }
        let decoded = match decoder.decode(&packet) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let mut buf = SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec());
        buf.copy_interleaved_ref(decoded);
        pcm.extend_from_slice(buf.samples());
    }

    let n_taps = std::env::var("FIR_N_TAPS")
    .ok()
    .and_then(|s| s.parse().ok())
    .filter(|&n| n >= 3 && n % 2 == 1)
    .unwrap_or(1025);

    let processed = if settings.linear_phase {
        eq_fir::apply_eq_linear_phase(&pcm, sample_rate, &settings, n_taps)
    } else {
        crate::eq::apply_eq(&pcm, sample_rate, &settings)
    };

    let wav = crate::eq::to_wav(&processed, sample_rate, bit_depth);
    Ok((wav, sample_rate, bit_depth))
}

/// GET /upsample_track?track_id=13176086
///
/// Reads UPSAMPLE_BIT_DEPTH and UPSAMPLE_SAMPLE_RATE from the environment,
/// decodes the local FLAC, resamples, re-encodes, copies tags, and writes
/// the result to <album_dir>/upsampled/<original_filename>.
/// Skips the work if an upsampled file already exists with the same
/// bit depth and sample rate.
async fn upsample_track(
    State(state): State<AppState>,
    Query(params): Query<UpsampleTrackParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let bit_depth = env::var("UPSAMPLE_BIT_DEPTH")
        .ok()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(24);
    let target_rate = env::var("UPSAMPLE_SAMPLE_RATE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| if n < 1000 { n * 1000 } else { n })
        .unwrap_or(96000);

    if !matches!(bit_depth, 16 | 24) {
        return Err((
            StatusCode::BAD_REQUEST,
            "UPSAMPLE_BIT_DEPTH must be 16 or 24".into(),
        ));
    }
    if !matches!(target_rate, 44100 | 48000 | 96000 | 192000) {
        return Err((
            StatusCode::BAD_REQUEST,
            "UPSAMPLE_SAMPLE_RATE must be 44100, 48000, 96000 or 192000".into(),
        ));
    }

    let src_path = state
        .library
        .track_path(params.track_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "track not in library".into()))?;

    let src = PathBuf::from(&src_path);
    if !src.exists() {
        return Err((StatusCode::NOT_FOUND, "source file missing".into()));
    }

    let album_dir = src.parent().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "no parent directory".into(),
    ))?;
    let filename = src.file_name().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "no filename".into(),
    ))?;
    let dest_dir = album_dir.join("upsampled");
    tokio::fs::create_dir_all(&dest_dir)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let dest = dest_dir.join(filename);

    // Skip if already upsampled with the same settings
    if dest.exists() {
    if let Ok(tagged) = lofty::read_from_path(&dest) {
        let props = tagged.properties();
        let existing_rate = props.sample_rate().unwrap_or(0) as usize;
        let existing_bits = props.bit_depth().unwrap_or(0);

        if existing_rate == target_rate && existing_bits == bit_depth {
            return Ok(Json(serde_json::json!({
                "track_id": params.track_id,
                "source": src_path,
                "upsampled": dest.to_string_lossy(),
                "bit_depth": bit_depth,
                "sample_rate": target_rate,
                "status": "already_upsampled"
            })));
        }
    }
}

    // Decode → resample → encode
    let (pcm, src_rate, channels) = decode_flac_pcm(&src_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let out_pcm = if src_rate == target_rate {
        pcm
    } else {
        upsample_pcm(&pcm, channels, src_rate, target_rate)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
    };

    let flac_bytes = encode_flac_bits(&out_pcm, target_rate, channels, bit_depth as usize)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    tokio::fs::write(&dest, &flac_bytes)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    copy_tags(&src, &dest).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "track_id": params.track_id,
        "source": src_path,
        "upsampled": dest.to_string_lossy(),
        "bit_depth": bit_depth,
        "sample_rate": target_rate,
        "status": "ok"
    })))
}

fn decode_flac_pcm(path: &str) -> Result<(Vec<f32>, usize, usize), String> {
    use symphonia::core::audio::SampleBuffer;
    use symphonia::core::codecs::DecoderOptions;
    use symphonia::core::formats::FormatOptions;
    use symphonia::core::io::MediaSourceStream;
    use symphonia::core::meta::MetadataOptions;
    use symphonia::core::probe::Hint;

    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());

    let mut hint = Hint::new();
    hint.with_extension("flac");

    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            mss,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|e| e.to_string())?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.codec_params.codec == symphonia::core::codecs::CODEC_TYPE_FLAC)
        .ok_or("no FLAC track")?;

    let track_id = track.id;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|e| e.to_string())?;

    let sample_rate = track.codec_params.sample_rate.unwrap_or(44100) as usize;
    let channels = track.codec_params.channels.map(|c| c.count()).unwrap_or(2);

    let mut pcm = Vec::new();
    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(_) => break,
        };
        if packet.track_id() != track_id {
            continue;
        }
        let decoded = decoder.decode(&packet).map_err(|e| e.to_string())?;
        let mut buf = SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec());
        buf.copy_interleaved_ref(decoded);
        pcm.extend_from_slice(buf.samples());
    }
    Ok((pcm, sample_rate, channels))
}

fn upsample_pcm(
    input: &[f32],
    channels: usize,
    from_rate: usize,
    to_rate: usize,
) -> Result<Vec<f32>, String> {
    use audioadapter_buffers::owned::InterleavedOwned;
    use rubato::audioadapter::Adapter;
    use rubato::{Fft, FixedSync, Resampler};

    if from_rate == to_rate || input.is_empty() {
        return Ok(input.to_vec());
    }

    let frames = input.len() / channels;
    let input_f64: Vec<f64> = input.iter().map(|&s| s as f64).collect();
    let input_buf = InterleavedOwned::<f64>::new_from(input_f64, channels, frames)
        .map_err(|e| e.to_string())?;

    let mut resampler = Fft::<f64>::new(from_rate, to_rate, 1024, channels, FixedSync::Both)
        .map_err(|e| e.to_string())?;

    let needed = resampler.process_all_needed_output_len(frames);
    let mut output = InterleavedOwned::<f64>::new(0.0, channels, needed);

    let (_consumed, produced) = resampler
        .process_all_into_buffer(&input_buf, &mut output, frames, None)
        .map_err(|e| e.to_string())?;

    let mut result = Vec::with_capacity(produced * channels);
    for i in 0..produced {
        for c in 0..channels {
            result.push(output.read_sample(c, i).unwrap() as f32);
        }
    }
    Ok(result)
}

fn encode_flac_bits(
    pcm: &[f32],
    sample_rate: usize,
    channels: usize,
    bit_depth: usize,
) -> Result<Vec<u8>, String> {
    use libflac_rs::{Encoder, EncoderConfig};

    let max = (1i32 << (bit_depth - 1)) - 1;
    let scale = max as f32;
    let samples: Vec<i32> = pcm
        .iter()
        .map(|&s| (s.clamp(-1.0, 1.0) * scale).round() as i32)
        .collect();

    let enc = Encoder::new(EncoderConfig::new(
        channels as u32,
        bit_depth as u32,
        sample_rate as u32,
    ));

    Ok(enc.encode(&samples))
}

fn copy_tags(src: &PathBuf, dest: &PathBuf) -> Result<(), String> {
    use lofty::config::WriteOptions;
    use lofty::file::TaggedFileExt;

    let src_tagged = lofty::read_from_path(src).map_err(|e| e.to_string())?;
    let mut dest_tagged = lofty::read_from_path(dest).map_err(|e| e.to_string())?;

    dest_tagged.clear();
    if let Some(src_tag) = src_tagged.primary_tag() {
        dest_tagged.insert_tag(src_tag.clone());
    }

    dest_tagged
        .save_to_path(dest, WriteOptions::default())
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn library_ai_search(
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "message": "AI search placeholder" }))
}

/// GET /upsample_album?album_id=...
///
/// Upsamples every track of the album (using the same rules as /upsample_track)
/// and writes cover.jpg into the upsampled folder if it is not already there.
async fn upsample_album(
    State(state): State<AppState>,
    Query(params): Query<UpsampleAlbumParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let q = state.qobuz.lock().await;
    let source = q.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;

    let album_id = params.album_id.clone();
    let service = source.service();

    let album = tokio::task::spawn_blocking(move || {
        let service = service.blocking_lock();
        service
            .get_album(&album_id, Some("track_ids"))
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let track_ids = album.track_ids.unwrap_or_default();
    let mut results = Vec::new();
    let mut upsampled_dir: Option<PathBuf> = None;

    for tid in &track_ids {
        // Re-use the same logic as /upsample_track
        let fake = UpsampleTrackParams { track_id: *tid };
        match upsample_track(State(state.clone()), Query(fake)).await {
            Ok(json) => {
                let v = json.0;
                if let Some(path) = v.get("upsampled").and_then(|p| p.as_str()) {
                    if upsampled_dir.is_none() {
                        upsampled_dir = PathBuf::from(path).parent().map(|p| p.to_path_buf());
                    }
                }
                results.push(v);
            }
            Err((code, msg)) => {
                results.push(serde_json::json!({
                    "track_id": tid,
                    "status": "error",
                    "code": code.as_u16(),
                    "message": msg
                }));
            }
        }
    }

    // Download cover into the upsampled folder if missing
    if let Some(dir) = upsampled_dir {
        let cover_path = dir.join("cover.jpg");
        if !cover_path.exists() {
            if let Some(image_url) = album
                .image
                .as_ref()
                .and_then(|img| img.large.as_deref().or(img.thumbnail.as_deref()))
            {
                if let Ok(resp) = reqwest::Client::new().get(image_url).send().await {
                    if resp.status().is_success() {
                        if let Ok(bytes) = resp.bytes().await {
                            let _ = tokio::fs::write(&cover_path, &bytes).await;
                        }
                    }
                }
            }
        }
    }

    Ok(Json(serde_json::json!({
        "album_id": params.album_id,
        "tracks": results,
        "status": "ok"
    })))
}

async fn apply_eq_preset(
    State(state): State<AppState>,
    Query(params): Query<EqPresetParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let presets: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("eq_presets.json").map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("presets: {}", e))
        })?,
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let seq = if params.seq == "00" { "00" } else { &params.seq };

    let preset = presets
        .get(seq)
        .ok_or((StatusCode::BAD_REQUEST, format!("unknown preset {}", seq)))?;

    // Store the full preset object (name + left + right) on the track
    let settings_json = preset.to_string();

    if seq == "00" {
        // Explicitly clear EQ
        state
            .library
            .set_eq_settings(&params.path, "")
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    } else {
        state
            .library
            .set_eq_settings(&params.path, &settings_json)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(Json(serde_json::json!({
        "status": "ok",
        "seq": seq,
        "name": preset.get("name").and_then(|n| n.as_str()).unwrap_or("unknown")
    })))
}

async fn list_eq_presets() -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let raw = std::fs::read_to_string("eq_presets.json")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let presets: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(presets))
}

async fn set_global_eq(
    Query(params): Query<GlobalEqParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let presets: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("eq_presets.json")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?,
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if params.seq == "00" {
        std::fs::write("eq_global.json", "{}")
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        return Ok(Json(serde_json::json!({ "status": "ok", "seq": "00", "name": "No EQ" })));
    }

    let preset = presets
        .get(&params.seq)
        .ok_or((StatusCode::BAD_REQUEST, format!("unknown preset {}", params.seq)))?;

    std::fs::write("eq_global.json", preset.to_string())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "seq": params.seq,
        "name": preset.get("name").and_then(|n| n.as_str()).unwrap_or("unknown")
    })))
}

async fn get_global_eq() -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let raw = std::fs::read_to_string("eq_global.json").unwrap_or_else(|_| "{}".into());
    let v: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or(serde_json::json!({}));
    Ok(Json(v))
}
