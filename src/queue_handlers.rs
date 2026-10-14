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
use std::path::PathBuf;
use std::sync::Arc;

use flacenc::component::BitRepr;
use flacenc::error::Verify;
use rubato::audioadapter::Adapter;

use audioadapter_buffers::owned::InterleavedOwned;
use rubato::{Fft, FixedSync, Resampler};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

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
    let mut resp = crate::stream_local(Query(qparams), headers).await?;

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

    let paths: Vec<String> = tracks[start_index..]
        .iter()
        .map(|t| t.path.clone())
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

/// GET /stream_from_track_upsampled?track_id=...&target_rate=96000
///
/// Loads tracks from the requested one to the end of the album,
/// upsamples them, re-encodes to a single FLAC, caches it on disk,
/// and streams the cached file.
pub async fn stream_from_track_upsampled(
    State(state): State<crate::AppState>,
    Query(params): Query<UpsampleParams>,
) -> Result<Response, (StatusCode, String)> {
    let format_id = params.format_id.unwrap_or(6);
    let target_rate = params.target_rate.unwrap_or(96000);

    let (album_id, tracks) = load_album_tracks(&state, params.track_id, format_id).await?;

    let start_index = tracks
        .iter()
        .position(|t| t.track_id == params.track_id)
        .unwrap_or(0);

    {
        let mut q = state.queue.lock().await;
        if q.album_id.as_deref() != Some(album_id.as_str()) || q.tracks.is_empty() {
            q.clear();
            q.album_id = Some(album_id.clone());
            q.tracks = tracks.clone();
        }
        q.current = start_index;
    }

    let paths: Vec<String> = tracks[start_index..]
        .iter()
        .map(|t| t.path.clone())
        .collect();

    if paths.is_empty() {
        return Err((StatusCode::NOT_FOUND, "no tracks to stream".into()));
    }

    // Cache key: album + start track + target rate
    let cache_dir = PathBuf::from("cache/upsampled");
    tokio::fs::create_dir_all(&cache_dir).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cache dir: {}", e),
        )
    })?;

    let cache_path = cache_dir.join(format!(
        "{}_{}_{}.flac",
        album_id, params.track_id, target_rate
    ));

    // Serve from cache if it already exists
    if cache_path.exists() {
        let bytes = tokio::fs::read(&cache_path).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("read cache: {}", e),
            )
        })?;
        return build_flac_response(bytes, &state).await;
    }

    // Otherwise decode → upsample → encode → cache
    let (pcm, _rate, channels) = upsample_tracks(&paths, target_rate).await?;
    let flac_bytes = encode_flac(&pcm, target_rate, channels).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("flac encode: {}", e),
        )
    })?;

    tokio::fs::write(&cache_path, &flac_bytes)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("write cache: {}", e),
            )
        })?;

    build_flac_response(flac_bytes, &state).await
}

#[derive(Deserialize)]
pub struct UpsampleParams {
    pub track_id: i32,
    pub format_id: Option<i32>,
    pub target_rate: Option<usize>,
}

async fn build_flac_response(
    bytes: Vec<u8>,
    state: &crate::AppState,
) -> Result<Response, (StatusCode, String)> {
    let body = Body::from(bytes);

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "audio/flac")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("X-Upsampled", "1")
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

/// Decode every track, upsample, and concatenate PCM.
async fn upsample_tracks(
    paths: &[String],
    target_rate: usize,
) -> Result<(Vec<f32>, usize, usize), (StatusCode, String)> {
    let mut all_pcm = Vec::new();
    let mut out_channels = 2;

    for path in paths {
        let (pcm, rate, channels) = decode_flac(path).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("decode {}: {}", path, e),
            )
        })?;

        let upsampled = if rate == target_rate {
            pcm
        } else {
            upsample_pcm(&pcm, channels, rate, target_rate).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("upsample {}: {}", path, e),
                )
            })?
        };

        out_channels = channels;
        all_pcm.extend(upsampled);
    }

    Ok((all_pcm, target_rate, out_channels))
}

async fn decode_flac(path: &str) -> Result<(Vec<f32>, usize, usize), String> {
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

/// Encode interleaved f32 PCM to a 16-bit FLAC byte buffer.
fn encode_flac(pcm: &[f32], sample_rate: usize, channels: usize) -> Result<Vec<u8>, String> {
    let samples: Vec<i32> = pcm
        .iter()
        .map(|&s| {
            let clamped = s.clamp(-1.0, 1.0);
            (clamped * 32767.0).round() as i32
        })
        .collect();

    let config = flacenc::config::Encoder::default()
        .into_verified()
        .map_err(|e| format!("{:?}", e))?;

    let source = flacenc::source::MemSource::from_samples(&samples, channels, 16, sample_rate);

    let flac_stream = flacenc::encode_with_fixed_block_size(&config, source, config.block_size)
        .map_err(|e| format!("{:?}", e))?;

    let mut sink = flacenc::bitsink::ByteSink::new();
    flac_stream
        .write(&mut sink)
        .map_err(|e| format!("{:?}", e))?;

    Ok(sink.as_slice().to_vec())
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
