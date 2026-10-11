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
    id: Option<i64>,
    path: Option<String>,
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

    // Initialize Qobuz before starting the Tokio runtime
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
                .route("/library/search", get(library_search))
                .route("/library/scan", get(library_scan))
                .route("/library/stream", get(library_stream))
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
    );
    std::fs::create_dir_all(&output_dir).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create library dir: {}", e),
        )
    })?;

    let track_id = params.track_id.ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "track_id required".into(),
    ))?;

    info!("Downloading track {} via stream URL", track_id);

    let service = source.service();
    let file_url = tokio::task::spawn_blocking({
        let service = service.clone();
        move || {
            let mut service = service.blocking_lock();
            service.get_track_file_url(track_id, format_id)
        }
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

    let url = file_url.url.ok_or((
        axum::http::StatusCode::BAD_GATEWAY,
        "No stream URL returned".into(),
    ))?;

    let temp_path = output_dir.join(format!("{}.flac", track_id));

    let client = reqwest::Client::new();
    let mut resp = client.get(url).send().await.map_err(|e| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Download request failed: {}", e),
        )
    })?;

    if !resp.status().is_success() {
        return Err((
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Download failed: HTTP {}", resp.status()),
        ));
    }

    let mut file = tokio::fs::File::create(&temp_path).await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create file: {}", e),
        )
    })?;

    while let Some(chunk) = resp.chunk().await.map_err(|e| {
        (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("Chunk error: {}", e),
        )
    })? {
        use tokio::io::AsyncWriteExt;
        file.write_all(&chunk).await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Write error: {}", e),
            )
        })?;
    }
    drop(file);

    let final_path = {
        let service = source.service();
        let temp_path_clone = temp_path.clone();
        let output_dir = output_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut service = service.blocking_lock();
            if let Ok(track) = service.get_track(track_id) {
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

                let mut tagged = lofty::read_from_path(&temp_path_clone)
                    .map_err(|e| format!("lofty read: {}", e))?;
                let mut tag = tagged
                    .primary_tag_mut()
                    .cloned()
                    .unwrap_or_else(|| Tag::new(TagType::VorbisComments));
                tag.set_title(title.clone());
                tag.set_artist(artist.clone());
                tag.set_album(album.clone());

                // Cover art
                if let Some(album_obj) = &track.album {
                    if let Some(image_url) = album_obj
                        .image
                        .as_ref()
                        .and_then(|img| img.large.as_ref().or(img.small.as_ref()))
                    {
                        if let Ok(resp) = reqwest::blocking::get(image_url) {
                            if let Ok(bytes) = resp.bytes() {
                                use lofty::picture::{MimeType, Picture, PictureType};
                                let picture = Picture::new_unchecked(
                                    PictureType::CoverFront,
                                    Some(MimeType::Jpeg),
                                    None,
                                    bytes.to_vec(),
                                );
                                tag.set_picture(0, picture);
                            }
                        }
                    }
                }

                tagged.insert_tag(tag);
                tagged
                    .save_to_path(&temp_path_clone, WriteOptions::default())
                    .map_err(|e| format!("lofty save: {}", e))?;

                // Structured path: artist/year/album/NN. title.flac
                let year = track
                    .album
                    .as_ref()
                    .and_then(|a| {
                        a.release_date_original
                            .as_ref()
                            .and_then(|d| d.split('-').next().map(|s| s.to_string()))
                            .or_else(|| {
                                a.released_at.map(|ts| {
                                    // rough year from unix timestamp
                                    let year = 1970 + (ts / 31_536_000);
                                    year.to_string()
                                })
                            })
                    })
                    .unwrap_or_else(|| "0000".to_string());
                let track_num = track.track_number.unwrap_or(0);
                let safe_artist = sanitize(&artist);
                let safe_album = sanitize(&album);
                let safe_title = sanitize(&title);

                let dir = output_dir.join(&safe_artist).join(&year).join(&safe_album);
                std::fs::create_dir_all(&dir).ok();

                let filename = format!("{:02}. {}.flac", track_num, safe_title);
                let new_path = dir.join(filename);

                std::fs::rename(&temp_path_clone, &new_path).ok();
                Ok::<_, String>(new_path)
            } else {
                tracing::info!("get_track FAILED for {}", track_id);
                Ok(temp_path_clone)
            }
        })
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Tagging task failed: {}", e),
            )
        })?
        .unwrap_or(temp_path)
    };

    Ok(Json(serde_json::json!({
        "type": "track",
        "track_id": track_id,
        "format_id": format_id,
        "path": final_path.display().to_string()
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

async fn library_stream(
    Query(params): Query<LibraryStreamParams>,
) -> Result<impl axum::response::IntoResponse, (axum::http::StatusCode, String)> {
    let library = library::Library::new("cadence.db").await.map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("DB error: {}", e),
        )
    })?;

    // Resolve path from id or direct path
    let file_path = if let Some(id) = params.id {
        // You may want a get_by_id method; for now use search or add one
        let rows = library
            .search("")
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        rows.into_iter()
            .find(|t| t["id"] == id)
            .and_then(|t| t["path"].as_str().map(|s| s.to_string()))
            .ok_or((axum::http::StatusCode::NOT_FOUND, "Track not found".into()))?
    } else if let Some(path) = params.path {
        path
    } else {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "Provide id or path".into(),
        ));
    };

    // Load the whole file into memory
    let bytes = tokio::fs::read(&file_path).await.map_err(|e| {
        (
            axum::http::StatusCode::NOT_FOUND,
            format!("Failed to read file: {}", e),
        )
    })?;

    let mime = if file_path.ends_with(".flac") {
        "audio/flac"
    } else if file_path.ends_with(".wav") {
        "audio/wav"
    } else {
        "application/octet-stream"
    };

    let body = axum::body::Body::from(bytes);
    let response = axum::response::Response::builder()
        .header("Content-Type", mime)
        .header("Accept-Ranges", "bytes")
        .body(body)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(response)
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

    // Get a short list of artists in the library
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
     Example replies:\n\
     [\"Miles Davis\"]\n\
     []\n\
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

    tracing::info!("Artists sent to Gemini: {:?}", artists);
    tracing::info!("Prompt: {}", prompt);

    let client = reqwest::Client::new();
    let model_url = std::env::var("GEMINI_MODEL_URL").map_err(|_| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "GEMINI_MODEL_URL not set".into(),
        )
    })?;

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

    tracing::info!("Full Gemini response: {}", json);

    // Extract the text reply
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

    let matched_artists: Vec<String> = serde_json::from_str(&reply).unwrap_or_else(|_| {
        tracing::warn!("Failed to parse Gemini reply as JSON array");
        vec![]
    });

    // Now search the library for those artists
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
