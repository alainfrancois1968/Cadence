//! Playback queue handlers.
//!
//! stream_from_track loads the album queue (or reuses it) and streams the
//! requested track via the existing stream_local path.
//! next_stream / previous_stream advance the queue and stream the new track.
//!
//! Auto-advance is client-driven: when the current track ends the client
//! opens /queue/next_stream. This keeps Range/seeking working.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::library::Library;
use crate::qobuz::QobuzSource;
use crate::queue::{PlayQueue, QueueTrack};

/// Minimal state the handlers need. Your AppState already has qobuz + library;
/// add `queue: Arc<tokio::sync::Mutex<PlayQueue>>`.
pub struct QueueState {
    pub qobuz: Arc<tokio::sync::Mutex<Option<QobuzSource>>>,
    pub library: Arc<Library>,
    pub queue: Arc<tokio::sync::Mutex<PlayQueue>>,
}

#[derive(Deserialize)]
pub struct StreamFromTrackParams {
    pub track_id: i32,
    pub format_id: Option<i32>,
}

/// GET /stream_from_track?track_id=...
pub async fn stream_from_track(
    State(state): State<crate::AppState>,
    Query(params): Query<StreamFromTrackParams>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let format_id = params.format_id.unwrap_or(6);

    // Resolve album and tracks.
    let (album_id, tracks) = load_album_tracks(&state, params.track_id, format_id).await?;

    let start_index = tracks
        .iter()
        .position(|t| t.track_id == params.track_id)
        .unwrap_or(0);

    {
        let mut q = state.queue.lock().await;
        if q.album_id.as_deref() != Some(album_id.as_str()) {
            q.clear();
            q.album_id = Some(album_id.clone());
            q.tracks = tracks;
        } else if q.tracks.is_empty() {
            q.tracks = tracks;
            q.album_id = Some(album_id);
        }
        q.current = start_index;
    }

    let current = {
        let q = state.queue.lock().await;
        q.current_track().cloned()
    }
    .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "queue empty".into()))?;

    // Stream via existing stream_local.
    let mut qparams = std::collections::HashMap::new();
    qparams.insert("path".to_string(), current.path.clone());
    let mut resp = crate::stream_local(State(state.clone()), Query(qparams), headers).await?;

    let (next_id, idx, len) = {
        let q = state.queue.lock().await;
        let next = q.tracks.get(q.current + 1).map(|t| t.track_id);
        (next, q.current, q.tracks.len())
    };
    let h = resp.headers_mut();
    h.insert("X-Queue-Index", idx.to_string().parse().unwrap());
    h.insert("X-Queue-Length", len.to_string().parse().unwrap());
    h.insert(
        "X-Current-Track-Id",
        current.track_id.to_string().parse().unwrap(),
    );
    if let Some(nid) = next_id {
        h.insert("X-Next-Track-Id", nid.to_string().parse().unwrap());
    }
    Ok(resp)
}

/// GET /stream_from_track_gapless?track_id=...&format_id=6
///
/// Loads all tracks from the requested one to the end of the album into memory,
/// then streams them as one continuous response.
/// Prefers the upsampled copy of each track when it exists.
pub async fn stream_from_track_gapless(
    State(state): State<crate::AppState>,
    Query(params): Query<StreamFromTrackParams>,
) -> Result<Response, (StatusCode, String)> {
    let format_id = params.format_id.unwrap_or(6);

    let (album_id, tracks) = load_album_tracks(&state, params.track_id, format_id).await?;

    let start_index = tracks
        .iter()
        .position(|t| t.track_id == params.track_id)
        .unwrap_or(0);

    {
        let mut q = state.queue.lock().await;
        if q.album_id.as_deref() != Some(album_id.as_str()) || q.tracks.is_empty() {
            q.clear();
            q.album_id = Some(album_id);
            q.tracks = tracks.clone();
        }
        q.current = start_index;
    }

    // Prefer upsampled version of each track, fall back to original.
    let paths: Vec<String> = tracks[start_index..]
        .iter()
        .map(|t| crate::prefer_upsampled(&t.path))
        .collect();

    if paths.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no tracks to stream".into()));
    }

    // Load every remaining track fully into memory.
    let mut all_bytes = Vec::new();
    for path in &paths {
        let bytes = tokio::fs::read(path).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read {}: {}", path, e),
            )
        })?;
        all_bytes.extend_from_slice(&bytes);
    }

    let body = Body::from(all_bytes);

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "audio/flac")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Gapless", "1")
        .body(body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (idx, len, next_id) = {
        let q = state.queue.lock().await;
        let next = q.tracks.get(q.current + 1).map(|t| t.track_id);
        (q.current, q.tracks.len(), next)
    };
    let h = resp.headers_mut();
    h.insert("X-Queue-Index", idx.to_string().parse().unwrap());
    h.insert("X-Queue-Length", len.to_string().parse().unwrap());
    if let Some(nid) = next_id {
        h.insert("X-Next-Track-Id", nid.to_string().parse().unwrap());
    }

    Ok(resp)
}

/// GET /queue
pub async fn queue_status(State(state): State<crate::AppState>) -> Json<serde_json::Value> {
    let q = state.queue.lock().await;
    Json(serde_json::to_value(q.status()).unwrap_or(serde_json::json!({})))
}

/// POST /queue/next  (JSON, does not stream)
pub async fn queue_next(
    State(state): State<crate::AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut q = state.queue.lock().await;
    if q.next().is_none() {
        return Err((StatusCode::NOT_FOUND, "end of queue".into()));
    }
    let status = q.status();
    Ok(Json(serde_json::json!({
        "action": "next",
        "current_index": status.current_index,
        "current": status.current,
        "stream_url": status.current.as_ref().map(|t| format!("/stream_from_track?track_id={}", t.track_id))
    })))
}

/// POST /queue/previous
pub async fn queue_previous(
    State(state): State<crate::AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut q = state.queue.lock().await;
    if q.previous().is_none() {
        return Err((StatusCode::NOT_FOUND, "start of queue".into()));
    }
    let status = q.status();
    Ok(Json(serde_json::json!({
        "action": "previous",
        "current_index": status.current_index,
        "current": status.current,
        "stream_url": status.current.as_ref().map(|t| format!("/stream_from_track?track_id={}", t.track_id))
    })))
}

/// GET /queue/next_stream  — advance and stream
pub async fn queue_next_stream(
    State(state): State<crate::AppState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let track_id = {
        let mut q = state.queue.lock().await;
        q.next()
            .map(|t| t.track_id)
            .ok_or((StatusCode::NOT_FOUND, "end of queue".into()))?
    };
    stream_from_track(
        State(state),
        Query(StreamFromTrackParams {
            track_id,
            format_id: None,
        }),
        headers,
    )
    .await
}

/// GET /queue/previous_stream
pub async fn queue_previous_stream(
    State(state): State<crate::AppState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let track_id = {
        let mut q = state.queue.lock().await;
        q.previous()
            .map(|t| t.track_id)
            .ok_or((StatusCode::NOT_FOUND, "start of queue".into()))?
    };
    stream_from_track(
        State(state),
        Query(StreamFromTrackParams {
            track_id,
            format_id: None,
        }),
        headers,
    )
    .await
}

/// POST /queue/clear
pub async fn queue_clear(State(state): State<crate::AppState>) -> Json<serde_json::Value> {
    state.queue.lock().await.clear();
    Json(serde_json::json!({ "status": "cleared" }))
}

/// Load every track on the album that contains `track_id`.
/// Downloads missing tracks into the library so they have a local path.
async fn load_album_tracks(
    state: &crate::AppState,
    track_id: i32,
    format_id: i32,
) -> Result<(String, Vec<QueueTrack>), (StatusCode, String)> {
    let qobuz = state.qobuz.lock().await;
    let source = qobuz.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "Qobuz not available".into(),
    ))?;
    let service = source.service();

    // Get the track so we know its album.
    let album_id = {
        let service = service.clone();
        tokio::task::spawn_blocking(move || {
            let service = service.blocking_lock();
            let track = service.get_track(track_id).map_err(|e| e.to_string())?;
            track
                .album
                .as_ref()
                .and_then(|a| a.id.clone())
                .ok_or_else(|| "track has no album".to_string())
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
    };

    // Get the album with its track list.
    let album = {
        let service = service.clone();
        let album_id = album_id.clone();
        tokio::task::spawn_blocking(move || {
            let service = service.blocking_lock();
            service
                .get_album(&album_id, Some("track_ids"))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
    };

    let track_ids = album.track_ids.unwrap_or_default();
    let album_title = album.title.clone().unwrap_or_default();
    let artist_name = album
        .artist
        .as_ref()
        .and_then(|a| a.name.clone())
        .unwrap_or_default();

    let mut tracks = Vec::new();
    for (idx, tid) in track_ids.iter().enumerate() {
        // Ensure the file is local.
        let path = if let Ok(Some(p)) = state.library.track_path(*tid).await {
            if std::path::Path::new(&p).exists() {
                p
            } else {
                crate::download_track_to_library(
                    source,
                    *tid,
                    format_id,
                    state.library.clone(),
                    None,
                    Some(idx + 1),
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
            }
        } else {
            crate::download_track_to_library(
                source,
                *tid,
                format_id,
                state.library.clone(),
                None,
                Some(idx + 1),
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
        };

        // Look up DR and waveform already stored for this path
        let (dr_score, waveform) = match state.library.get_dr_and_waveform(&path).await {
            Ok((dr, wf)) => (dr, wf),
            Err(_) => (None, None),
        };

        tracks.push(QueueTrack {
            track_id: *tid,
            title: String::new(),
            artist: artist_name.clone(),
            album: album_title.clone(),
            album_id: album_id.clone(),
            path,
            duration: 0.0,
            track_number: Some((idx + 1) as i32),
            dr_score,
            waveform,
        });
    }

    Ok((album_id, tracks))
}
