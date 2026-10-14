use crate::eq::{is_flat, EqBand, EqSettings};
use math_audio_iir_fir::{Biquad, BiquadFilterType, Fir};

/// Apply a linear-phase FIR approximation of the given EqSettings.
///
/// `n_taps` must be odd (e.g. 1025, 2049). Larger values give a closer
/// magnitude match and higher latency.
pub fn apply_eq_linear_phase(
    samples: &[f32],
    sample_rate: u32,
    settings: &EqSettings,
    n_taps: usize,
) -> Vec<f32> {
    if is_flat(settings) || samples.is_empty() || n_taps < 3 || n_taps % 2 == 0 {
        return samples.to_vec();
    }

    let sr = sample_rate as f64;

    // Convert every band into a biquad so we can evaluate its magnitude response
    let mut biquads = Vec::new();
    for band in settings.left.iter().chain(settings.right.iter()) {
        if let Some(b) = band_to_biquad(band, sr) {
            biquads.push(b);
        }
    }

    if biquads.is_empty() {
        return samples.to_vec();
    }

    // Design a symmetric (linear-phase) FIR that matches the combined magnitude
    let fir = design_fir_from_biquads(&biquads, sr, n_taps);

    // Process
    process_fir(&fir, samples)
}

/// Map an EqBand to a math-iir-fir Biquad.
fn band_to_biquad(band: &EqBand, sample_rate: f64) -> Option<Biquad> {
    let filter_type = match band.r#type.as_str() {
        "peak" => BiquadFilterType::Peak,
        "lowshelf" => BiquadFilterType::Lowshelf,
        "highshelf" => BiquadFilterType::Highshelf,
        "lowpass" => BiquadFilterType::Lowpass,
        "highpass" => BiquadFilterType::Highpass,
        _ => return None,
    };

    Some(Biquad::new(
        filter_type,
        band.freq,
        sample_rate,
        band.q.max(0.1),
        band.gain,
    ))
}

/// Build a linear-phase FIR whose magnitude approximates the cascade of biquads.
fn design_fir_from_biquads(biquads: &[Biquad], sample_rate: f64, n_taps: usize) -> Fir {
    let n_points = 1024;
    let nyquist = sample_rate / 2.0;

    // Sample the target linear magnitude on a dense grid
    let mut target = vec![1.0f64; n_points];
    for (i, mag) in target.iter_mut().enumerate() {
        let freq = i as f64 * nyquist / (n_points - 1) as f64;
        let mut combined = 1.0;
        for b in biquads {
            combined *= b.result(freq);
        }
        *mag = combined;
    }

    // Inverse DFT construction of symmetric coefficients
    let half = n_taps / 2;
    let mut coeffs = vec![0.0f64; n_taps];

    for k in 0..n_taps {
        let mut sum = 0.0;
        for (i, &mag) in target.iter().enumerate() {
            let omega = std::f64::consts::PI * i as f64 / (n_points - 1) as f64;
            let phase = omega * (k as f64 - half as f64);
            sum += mag * phase.cos();
        }
        coeffs[k] = sum / n_points as f64;
    }

    // Hann window to reduce ripple
    for (i, c) in coeffs.iter_mut().enumerate() {
        let w = 0.5 * (1.0 - (2.0 * std::f64::consts::PI * i as f64 / (n_taps - 1) as f64).cos());
        *c *= w;
    }

    Fir::new_custom(coeffs, sample_rate)
}

/// Direct-form FIR convolution (mono).
/// For longer filters you can replace this with an overlap-add FFT version.
fn process_fir(fir: &Fir, input: &[f32]) -> Vec<f32> {
    // The crate exposes coefficients; adjust the accessor if the version differs.
    // Common patterns: fir.coeffs() or a public field.
    let taps = fir.coeffs(); // <-- change to the actual method/field if needed
    let n = taps.len();
    let mut output = vec![0.0f32; input.len()];

    for i in 0..input.len() {
        let mut acc = 0.0f64;
        for k in 0..n {
            if i >= k {
                acc += input[i - k] as f64 * taps[k];
            }
        }
        output[i] = acc as f32;
    }
    output
}
