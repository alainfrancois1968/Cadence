use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

mod library;
mod sources;

use sources::qobuz::QobuzSource;

#[derive(Clone)]
struct AppState {
    qobuz: Arc<Mutex<Option<QobuzSource>>>,
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    authenticated: bool,
}

#[derive(Deserialize)]
struct SearchParams {
    q: Option<String>,
    limit: Option<i32>,
}

#[derive(Deserialize)]
struct LimitParams {
    limit: Option<i32>,
}

#[derive(Deserialize)]
struct ArtistDownloadParams {
    artist_id: i32,
    limit: Option<i32>,
    format_id: Option<i32>,
}

#[derive(Deserialize)]
struct ArtistQueueParams {
    artist_id: i32,
    limit: Option<i32>,
}

#[derive(Deserialize)]
struct StreamParams {
    track_id: i32,
    format_id: Option<i32>,
}

#[derive(Deserialize)]
struct DownloadParams {
    track_id: Option<i32>,
    album_id: Option<String>,
    format_id: Option<i32>,
}

#[derive(Deserialize)]
struct LibraryStreamParams {
    path: Option<String>,
}

#[derive(Deserialize)]
struct QobuzStreamParams {
    track_id: i32,
    format_id: Option<i32>,
}

fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    println!("EMAIL set: {}", std::env::var("QOBUZ_EMAIL").is_ok());
    println!("PASSWORD set: {}", std::env::var("QOBUZ_PASSWORD").is_ok());
    println!("APP_ID set: {}", std::env::var("QOBUZ_APP_ID").is_ok());
    println!(
        "APP_SECRET set: {}",
        std::env::var("QOBUZ_APP_SECRET").is_ok()
    );

    let qobuz = match QobuzSource::new() {
        Ok(src) => {
            info!("Qobuz authenticated successfully");
            Some(src)
        }
        Err(e) => {
            tracing::warn!("Qobuz authentication failed (continuing without it): {}", e);
            None
        }
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let state = AppState {
                qobuz: Arc::new(Mutex::new(qobuz)),
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
                .route("/library/stream_local", get(library_stream_local))
                .route("/library/stream_qobuz", get(library_stream_qobuz))
                .route("/library/ai-search", get(library_ai_search))
                .with_state(state);

            let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
            info!("Cadence Music Server listening on http://0.0.0.0:3000");
            axum::serve(listener, app).await?;

            Ok(())
        })
}

async fn health() -> &'static str {
    "ok"
}

async fn qobuz_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let guard = state.qobuz.lock().await;
    Json(StatusResponse {
        status: "ok".to_string(),
        authenticated: guard.is_some(),
    })
}

async fn qobuz_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let query = params.q.clone().unwrap_or_default();
    if query.is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "Missing query parameter 'q'".into(),
        ));
    }

    let limit = params.limit.unwrap_or(10);
    info!("Searching Qobuz for: {}", query);

    let service = source.service();
    let query_for_search = query.clone();

    let result = tokio::task::spawn_blocking(move || {
        let service = service.blocking_lock();
        let albums = service.search_albums(&query_for_search, Some(limit), None)?;
        let tracks = service.search_tracks(&query_for_search, Some(limit), None)?;
        Ok::<_, qobuz_api::errors::QobuzApiError>((albums, tracks))
    })
    .await
    .map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task failed: {}", e),
        )
    })?
    .map_err(|e| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Search failed: {}", e),
        )
    })?;

    let (albums, tracks) = result;

    Ok(Json(serde_json::json!({
        "query": query,
        "albums": albums,
        "tracks": tracks
    })))
}

async fn qobuz_stream(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let format_id = params.format_id.unwrap_or(6);
    let track_id = params.track_id;
    info!(
        "Requesting stream URL for track {} (format {})",
        track_id, format_id
    );

    let service = source.service();

    let file_url = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        service.get_track_file_url(track_id, format_id)
    })
    .await
    .map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task failed: {}", e),
        )
    })?
    .map_err(|e| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Stream URL failed: {}", e),
        )
    })?;

    Ok(Json(serde_json::json!({
        "track_id": track_id,
        "format_id": format_id,
        "url": file_url.url,
        "duration": file_url.duration,
        "mime_type": file_url.mime_type
    })))
}

async fn index_downloaded_track(
    path: &str,
    title: &str,
    artist: &str,
    artist_id: Option<i32>,
    album: &str,
    track_number: Option<i32>,
    duration: Option<f64>,
    genre: Option<&str>,
) {
    if let Ok(library) = library::Library::new("cadence.db").await {
        let stream_url = format!(
            "http://localhost:3000/library/stream_local?path={}",
            urlencoding::encode(path)
        );
        let _ = library
            .upsert_track(
                path,
                Some(title),
                Some(artist),
                artist_id,
                Some(album),
                track_number,
                duration,
                "flac",
                Some(&stream_url),
                genre,
            )
            .await;
    }
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
            service.get_track_file_url(track_id, format_id)
        }
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())?;

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
            // qobuz-api Track has no genre field
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

            let rt = tokio::runtime::Handle::current();
            rt.block_on(index_downloaded_track(
                new_path.to_str().unwrap_or(""),
                &title,
                &artist,
                artist_id,
                &album,
                track.track_number,
                None,
                genre.as_deref(),
            ));

            Ok::<_, String>(new_path.display().to_string())
        }
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok(final_path)
}

async fn qobuz_download(
    State(state): State<AppState>,
    Query(params): Query<DownloadParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    use lofty::config::WriteOptions;
    use lofty::file::{AudioFile, TaggedFileExt};
    use lofty::tag::{Accessor, Tag, TagType};

    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let format_id = params.format_id.unwrap_or(6);
    let output_dir = std::path::PathBuf::from(
        std::env::var("LIBRARY_PATH").unwrap_or_else(|_| "library".into()),
    )
    .join("Qobuz");
    std::fs::create_dir_all(&output_dir).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create library dir: {}", e),
        )
    })?;

    let service = source.service();

    if let Some(album_id) = params.album_id {
        info!("Downloading album {}", album_id);
        let album_id_clone = album_id.clone();
        let output_dir_clone = output_dir.clone();
        let paths = tokio::task::spawn_blocking(move || {
            let mut service = service.blocking_lock();
            let album = service
                .get_album(&album_id_clone, Some("track_ids"))
                .map_err(|e| format!("get_album failed: {}", e))?;
            let mut saved = Vec::new();

            if let Some(track_ids) = album.track_ids {
                let artist = album
                    .artist
                    .as_ref()
                    .and_then(|a| a.name.clone())
                    .unwrap_or_else(|| "Unknown Artist".into());
                let album_title = album
                    .title
                    .clone()
                    .unwrap_or_else(|| "Unknown Album".into());
                let artist_id = album.artist.as_ref().and_then(|a| a.id);
                let year = album
                    .release_date_original
                    .as_ref()
                    .and_then(|d| d.split('-').next())
                    .unwrap_or("0000")
                    .to_string();

                let artist_folder = format!("{} - {}", artist_id.unwrap_or(0), sanitize(&artist));
                let album_folder = format!("{} - {}", album_id_clone, sanitize(&album_title));
                let dir = output_dir_clone
                    .join(&artist_folder)
                    .join(&year)
                    .join(&album_folder);
                let _ = std::fs::create_dir_all(&dir);

                if let Some(image) = &album.image {
                    if let Some(cover_url) = image.large.as_ref().or(image.small.as_ref()) {
                        if let Ok(resp) = reqwest::blocking::get(cover_url) {
                            if let Ok(bytes) = resp.bytes() {
                                let cover_path = dir.join("cover.jpg");
                                let _ = std::fs::write(&cover_path, &bytes);
                            }
                        }
                    }
                }

                for (idx, track_id) in track_ids.into_iter().enumerate() {
                    let track = service
                        .get_track(track_id)
                        .map_err(|e| format!("get_track {}: {}", track_id, e))?;

                    let file_url = service
                        .get_track_file_url(track_id, format_id)
                        .map_err(|e| format!("file url: {}", e))?;
                    let url = file_url.url.ok_or("no url")?;

                    let temp_path = output_dir_clone.join(format!("{}.flac", track_id));
                    let resp = reqwest::blocking::get(&url).map_err(|e| e.to_string())?;
                    let bytes = resp.bytes().map_err(|e| e.to_string())?;
                    std::fs::write(&temp_path, &bytes).map_err(|e| e.to_string())?;

                    let title = track.title.unwrap_or_else(|| format!("Track {}", track_id));
                    let track_artist = track
                        .performer
                        .as_ref()
                        .and_then(|p| p.name.clone())
                        .unwrap_or_else(|| artist.clone());
                    let track_artist_id = track.performer.as_ref().and_then(|p| p.id).or(artist_id);
                    let genre = album.genre.as_ref().and_then(|g| g.name.clone());
                    if let Ok(mut tagged) = lofty::read_from_path(&temp_path) {
                        let mut tag = tagged
                            .primary_tag_mut()
                            .cloned()
                            .unwrap_or_else(|| Tag::new(TagType::VorbisComments));
                        tag.set_title(title.clone());
                        tag.set_artist(track_artist.clone());
                        tag.set_album(album_title.clone());
                        if let Some(g) = &genre {
                            tag.set_genre(g.clone());
                        }
                        tagged.insert_tag(tag);
                        let _ = tagged.save_to_path(&temp_path, WriteOptions::default());
                    }

                    let track_num = track.track_number.unwrap_or((idx + 1) as i32);
                    let filename =
                        format!("{:02}. {} - {}.flac", track_num, track_id, sanitize(&title));
                    let final_path = dir.join(filename);
                    let _ = std::fs::rename(&temp_path, &final_path);

                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(index_downloaded_track(
                        final_path.to_str().unwrap_or(""),
                        &title,
                        &track_artist,
                        track_artist_id,
                        &album_title,
                        Some(track_num),
                        None,
                        genre.as_deref(),
                    ));

                    saved.push(final_path.display().to_string());
                }
            }
            Ok::<_, String>(saved)
        })
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?;

        return Ok(Json(serde_json::json!({
            "type": "album",
            "album_id": album_id,
            "format_id": format_id,
            "paths": paths
        })));
    }

    let track_id = params.track_id.ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "track_id or album_id required".into(),
    ))?;
    info!("Downloading track {} via stream URL", track_id);

    let path = download_track_to_library(source, track_id, format_id)
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?;

    Ok(Json(serde_json::json!({
        "type": "track",
        "track_id": track_id,
        "format_id": format_id,
        "path": path
    })))
}

async fn qobuz_album_id(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let query = params.q.clone().unwrap_or_default();
    if query.is_empty() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "Missing q".into()));
    }

    let service = source.service();
    let query_clone = query.clone();

    let album_id = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        let result = service
            .search_albums(&query_clone, Some(1), None)
            .map_err(|e| e.to_string())?;
        result
            .items
            .and_then(|items| items.into_iter().next())
            .and_then(|album| album.id)
            .ok_or_else(|| "No album found".to_string())
    })
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::NOT_FOUND, e))?;

    Ok(Json(serde_json::json!({
        "query": query,
        "album_id": album_id
    })))
}

async fn qobuz_queue_albums(
    State(state): State<AppState>,
    Query(params): Query<ArtistQueueParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let artist_id = params.artist_id;
    let limit = params.limit.unwrap_or(50);

    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let library = library::Library::new("cadence.db")
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let service = source.service();

    let albums = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        let result = service
            .get_release_list(artist_id, Some(limit), None)
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(result.items.unwrap_or_default())
    })
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?;

    let mut queued = 0;
    for album in albums {
        if let Some(album_id) = album.id {
            let title = album.title.unwrap_or_default();
            let artist = album
                .artist
                .as_ref()
                .and_then(|a| a.name.clone())
                .unwrap_or_default();

            let _ = sqlx::query(
                "INSERT OR IGNORE INTO album_queue (album_id, artist, artist_id, title) VALUES (?, ?, ?, ?)"
            )
            .bind(&album_id)
            .bind(&artist)
            .bind(artist_id)
            .bind(&title)
            .execute(&library.pool)
            .await;

            queued += 1;
        }
    }

    Ok(Json(serde_json::json!({
        "artist_id": artist_id,
        "limit": limit,
        "queued": queued
    })))
}

async fn qobuz_queue_artists(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let query = params.q.unwrap_or_default();
    if query.is_empty() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "Missing q".into()));
    }

    let limit = params.limit.unwrap_or(50);

    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let library = library::Library::new("cadence.db")
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let service = source.service();
    let query_clone = query.clone();

    let artists = tokio::task::spawn_blocking(move || {
        let mut service = service.blocking_lock();
        let result = service
            .search_artists(&query_clone, Some(limit), None)
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(result.items.unwrap_or_default())
    })
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?;

    let mut queued = 0;
    for artist in artists {
        if let (Some(id), Some(name)) = (artist.id, artist.name) {
            let _ = sqlx::query("INSERT OR IGNORE INTO artists (artist_id, name) VALUES (?, ?)")
                .bind(id)
                .bind(&name)
                .execute(&library.pool)
                .await;
            queued += 1;
        }
    }

    Ok(Json(serde_json::json!({
        "query": query,
        "limit": limit,
        "queued": queued
    })))
}

async fn qobuz_queue_albums_from_artists(
    State(state): State<AppState>,
    Query(params): Query<LimitParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let limit = params.limit.unwrap_or(50);

    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let library = library::Library::new("cadence.db")
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let artists: Vec<(i32, String)> = sqlx::query_as("SELECT artist_id, name FROM artists")
        .fetch_all(&library.pool)
        .await
        .unwrap_or_default();

    let artists_processed = artists.len();
    let mut total_queued = 0;

    for (artist_id, name) in artists {
        let already_queued: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM album_queue WHERE artist_id = ?")
                .bind(artist_id)
                .fetch_one(&library.pool)
                .await
                .unwrap_or(0);

        if already_queued > 0 {
            continue;
        }

        match download_albums_for_artist(source, artist_id, limit, 6).await {
            Ok(album_ids) => {
                for album_id in album_ids {
                    let result = sqlx::query(
                        "INSERT OR IGNORE INTO album_queue (album_id, artist, artist_id, title) VALUES (?, ?, ?, ?)"
                    )
                    .bind(&album_id)
                    .bind(&name)
                    .bind(artist_id)
                    .bind("")
                    .execute(&library.pool)
                    .await;

                    if let Ok(r) = result {
                        if r.rows_affected() > 0 {
                            total_queued += 1;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch albums for artist {}: {}", artist_id, e);
            }
        }
    }

    Ok(Json(serde_json::json!({
        "artists_processed": artists_processed,
        "limit_per_artist": limit,
        "new_albums_queued": total_queued
    })))
}

async fn download_albums_for_artist(
    source: &QobuzSource,
    artist_id: i32,
    limit: i32,
    _format_id: i32,
) -> Result<Vec<String>, String> {
    let service = source.service();
    let album_ids = tokio::task::spawn_blocking({
        let service = service.clone();
        move || {
            let mut service = service.blocking_lock();
            let result = service
                .get_release_list(artist_id, Some(limit), None)
                .map_err(|e| e.to_string())?;
            let ids: Vec<String> = result
                .items
                .unwrap_or_default()
                .into_iter()
                .filter_map(|a| a.id)
                .collect();
            Ok::<_, String>(ids)
        }
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok(album_ids)
}

async fn qobuz_download_artist_albums(
    State(state): State<AppState>,
    Query(params): Query<ArtistDownloadParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let guard = state.qobuz.lock().await;
    let source = guard.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not authenticated".into(),
    ))?;

    let ids = download_albums_for_artist(
        source,
        params.artist_id,
        params.limit.unwrap_or(10),
        params.format_id.unwrap_or(6),
    )
    .await
    .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?;

    Ok(Json(serde_json::json!({
        "artist_id": params.artist_id,
        "albums": ids
    })))
}

async fn library_search(
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let library = library::Library::new("cadence.db").await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("DB error: {}", e),
        )
    })?;

    let query = params.q.unwrap_or_default();
    let results = library.search(&query).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Search error: {}", e),
        )
    })?;

    Ok(Json(
        serde_json::json!({ "query": query, "tracks": results }),
    ))
}

async fn library_scan() -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let library = library::Library::new("cadence.db").await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("DB error: {}", e),
        )
    })?;

    let (added, updated, removed) =
        library::scan_directory(&library, std::path::Path::new("library"))
            .await
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Scan error: {}", e),
                )
            })?;

    Ok(Json(serde_json::json!({
        "added": added,
        "updated": updated,
        "removed": removed
    })))
}

async fn library_stream_local(
    Query(params): Query<LibraryStreamParams>,
) -> Result<impl axum::response::IntoResponse, (axum::http::StatusCode, String)> {
    let path = params
        .path
        .ok_or((axum::http::StatusCode::BAD_REQUEST, "Missing path".into()))?;

    let bytes = tokio::fs::read(&path).await.map_err(|e| {
        (
            axum::http::StatusCode::NOT_FOUND,
            format!("Failed to read file: {}", e),
        )
    })?;

    let mime = if path.ends_with(".flac") {
        "audio/flac"
    } else {
        "audio/wav"
    };

    Ok(([(axum::http::header::CONTENT_TYPE, mime)], bytes))
}

async fn library_stream_qobuz(
    State(state): State<AppState>,
    Query(params): Query<QobuzStreamParams>,
) -> Result<impl axum::response::IntoResponse, (axum::http::StatusCode, String)> {
    let track_id = params.track_id;
    let format_id = params.format_id.unwrap_or(6);

    let library = library::Library::new("cadence.db")
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let existing_path: Option<String> =
        sqlx::query_scalar("SELECT path FROM tracks WHERE path LIKE ? LIMIT 1")
            .bind(format!("%{} - %", track_id))
            .fetch_optional(&library.pool)
            .await
            .unwrap_or(None);

    let path = if let Some(p) = existing_path {
        if std::path::Path::new(&p).exists() {
            p
        } else {
            let guard = state.qobuz.lock().await;
            let source = guard.as_ref().ok_or((
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Qobuz not authenticated".into(),
            ))?;
            download_track_to_library(source, track_id, format_id)
                .await
                .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?
        }
    } else {
        let guard = state.qobuz.lock().await;
        let source = guard.as_ref().ok_or((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Qobuz not authenticated".into(),
        ))?;
        download_track_to_library(source, track_id, format_id)
            .await
            .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e))?
    };

    let bytes = tokio::fs::read(&path).await.map_err(|e| {
        (
            axum::http::StatusCode::NOT_FOUND,
            format!("Failed to read local file: {}", e),
        )
    })?;

    let mime = if path.ends_with(".flac") {
        "audio/flac"
    } else {
        "audio/wav"
    };

    Ok(([(axum::http::header::CONTENT_TYPE, mime)], bytes))
}

async fn library_ai_search(
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let query = params.q.unwrap_or_default();
    if query.is_empty() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "Missing q".into()));
    }

    let library = library::Library::new("cadence.db")
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let artists: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT artist FROM tracks WHERE artist IS NOT NULL")
            .fetch_all(&library.pool)
            .await
            .unwrap_or_default();

    let prompt = format!(
        "You are helping search a personal music library.\n\
         User query: \"{}\"\n\
         Artists in the library: {}.\n\
         Which of these artists match the query? \
         Reply with ONLY a JSON array of matching artist names, nothing else.\n\
         Example: [\"Miles Davis\"] or []\n\
         Do not use markdown.",
        query,
        artists.join(", ")
    );

    let api_key = std::env::var("GEMINI_API_KEY").map_err(|_| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "GEMINI_API_KEY not set".into(),
        )
    })?;

    let model_url = std::env::var("GEMINI_MODEL_URL")
        .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent".to_string());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}?key={}", model_url, api_key))
        .json(&serde_json::json!({
            "contents": [{
                "parts": [{ "text": prompt }]
            }]
        }))
        .send()
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e.to_string()))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, e.to_string()))?;

    let reply = json["candidates"][0]["content"]["parts"][0]["text"]
        .as_str()
        .unwrap_or("[]")
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim()
        .to_string();

    tracing::info!("Gemini cleaned reply: {}", reply);

    let matched_artists: Vec<String> = serde_json::from_str(&reply).unwrap_or_default();

    let mut all_tracks = Vec::new();
    for artist in matched_artists {
        if let Ok(tracks) = library.search(&artist).await {
            all_tracks.extend(tracks);
        }
    }

    Ok(Json(serde_json::json!({
        "query": query,
        "ai_reply": reply,
        "tracks": all_tracks
    })))
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
