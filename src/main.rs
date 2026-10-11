use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

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
    let source = guard
        .as_ref()
        .ok_or((axum::http::StatusCode::SERVICE_UNAVAILABLE, "Qobuz not authenticated".into()))?;

    let query = params.q.clone().unwrap_or_default();
    if query.is_empty() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "Missing query parameter 'q'".into()));
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
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
    .map_err(|e| (axum::http::StatusCode::BAD_GATEWAY, format!("Search failed: {}", e)))?;

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
