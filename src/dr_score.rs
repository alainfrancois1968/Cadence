//! Dynamic range scoring (crest factor, similar to DR Meter).
//! Call `compute_dynamic_range` after a track is written to disk.
use std::path::Path;

use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

/// Returns the crest factor in dB (higher = more dynamic).
/// Typical range: ~6 (heavily compressed) to ~14+ (very dynamic).
pub fn compute_dynamic_range(path: &Path) -> Result<f64, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open: {}", e))?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());

    let mut hint = Hint::new();
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    }

    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            mss,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|e| format!("probe: {}", e))?;

    let mut format = probed.format;
    let track = format.default_track().ok_or("no audio track")?;

    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|e| format!("decoder: {}", e))?;

    let mut peak = 0.0f64;
    let mut sum_sq = 0.0f64;
    let mut count = 0u64;

    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(Error::ResetRequired) => continue,
            Err(Error::IoError(_)) | Err(Error::DecodeError(_)) => break,
            Err(_) => break,
        };

        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = *decoded.spec();
                let mut buf = SampleBuffer::<f32>::new(decoded.capacity() as u64, spec);
                buf.copy_interleaved_ref(decoded);

                for &sample in buf.samples() {
                    let abs = (sample as f64).abs();
                    if abs > peak {
                        peak = abs;
                    }
                    sum_sq += abs * abs;
                    count += 1;
                }
            }
            Err(Error::DecodeError(_)) => continue,
            Err(_) => break,
        }
    }

    if count == 0 {
        return Err("no samples decoded".into());
    }

    let rms = (sum_sq / count as f64).sqrt();
    if rms <= 0.0 {
        return Ok(0.0);
    }

    // Crest factor in dB
    Ok(20.0 * (peak / rms).log10())
}

/// Convenience wrapper that logs the score and returns it as Option.
/// Use this at the end of a download.
pub fn score_and_log(path: &Path, track_id: i32) -> Option<f64> {
    match compute_dynamic_range(path) {
        Ok(score) => {
            tracing::info!("DR score for track {}: {:.1} dB", track_id, score);
            Some(score)
        }
        Err(e) => {
            tracing::warn!("DR score failed for track {} ({}): {}", track_id, path.display(), e);
            None
        }
    }
}
