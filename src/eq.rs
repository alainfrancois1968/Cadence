use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqBand {
    pub freq: f64,
    pub gain: f64, // dB
    pub q: f64,
    pub r#type: String, // peak, lowshelf, highshelf, lowpass, highpass
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqSettings {
    #[serde(default)]
    pub linear_phase: bool,
    pub left: Vec<EqBand>,
    pub right: Vec<EqBand>,
}

pub fn is_flat(settings: &EqSettings) -> bool {
    settings
        .left
        .iter()
        .chain(settings.right.iter())
        .all(|b| b.gain.abs() < 0.01)
}

/// Simple biquad. Coefficients computed from RBJ cookbook.
struct Biquad {
    b0: f64,
    b1: f64,
    b2: f64,
    a1: f64,
    a2: f64,
    x1: f64,
    x2: f64,
    y1: f64,
    y2: f64,
}

impl Biquad {
    fn from_band(band: &EqBand, sample_rate: f64) -> Self {
        let w0 = 2.0 * std::f64::consts::PI * band.freq / sample_rate;
        let alpha = w0.sin() / (2.0 * band.q.max(0.1));
        let a = 10.0_f64.powf(band.gain / 40.0);

        let (b0, b1, b2, a0, a1, a2) = match band.r#type.as_str() {
            "lowshelf" => {
                let cos = w0.cos();
                let sqrt_a = a.sqrt();
                (
                    a * ((a + 1.0) - (a - 1.0) * cos + 2.0 * sqrt_a * alpha),
                    2.0 * a * ((a - 1.0) - (a + 1.0) * cos),
                    a * ((a + 1.0) - (a - 1.0) * cos - 2.0 * sqrt_a * alpha),
                    (a + 1.0) + (a - 1.0) * cos + 2.0 * sqrt_a * alpha,
                    -2.0 * ((a - 1.0) + (a + 1.0) * cos),
                    (a + 1.0) + (a - 1.0) * cos - 2.0 * sqrt_a * alpha,
                )
            }
            "highshelf" => {
                let cos = w0.cos();
                let sqrt_a = a.sqrt();
                (
                    a * ((a + 1.0) + (a - 1.0) * cos + 2.0 * sqrt_a * alpha),
                    -2.0 * a * ((a - 1.0) + (a + 1.0) * cos),
                    a * ((a + 1.0) + (a - 1.0) * cos - 2.0 * sqrt_a * alpha),
                    (a + 1.0) - (a - 1.0) * cos + 2.0 * sqrt_a * alpha,
                    2.0 * ((a - 1.0) - (a + 1.0) * cos),
                    (a + 1.0) - (a - 1.0) * cos - 2.0 * sqrt_a * alpha,
                )
            }
            "lowpass" => {
                let cos = w0.cos();
                (
                    (1.0 - cos) / 2.0,
                    1.0 - cos,
                    (1.0 - cos) / 2.0,
                    1.0 + alpha,
                    -2.0 * cos,
                    1.0 - alpha,
                )
            }
            "highpass" => {
                let cos = w0.cos();
                (
                    (1.0 + cos) / 2.0,
                    -(1.0 + cos),
                    (1.0 + cos) / 2.0,
                    1.0 + alpha,
                    -2.0 * cos,
                    1.0 - alpha,
                )
            }
            _ => {
                // peak
                let cos = w0.cos();
                (
                    1.0 + alpha * a,
                    -2.0 * cos,
                    1.0 - alpha * a,
                    1.0 + alpha / a,
                    -2.0 * cos,
                    1.0 - alpha / a,
                )
            }
        };

        Self {
            b0: b0 / a0,
            b1: b1 / a0,
            b2: b2 / a0,
            a1: a1 / a0,
            a2: a2 / a0,
            x1: 0.0,
            x2: 0.0,
            y1: 0.0,
            y2: 0.0,
        }
    }

    fn process(&mut self, x: f64) -> f64 {
        let y = self.b0 * x + self.b1 * self.x1 + self.b2 * self.x2
            - self.a1 * self.y1
            - self.a2 * self.y2;
        self.x2 = self.x1;
        self.x1 = x;
        self.y2 = self.y1;
        self.y1 = y;
        y
    }
}

/// Apply EQ to interleaved stereo f32 samples. Returns new Vec.
pub fn apply_eq(samples: &[f32], sample_rate: u32, settings: &EqSettings) -> Vec<f32> {
    if is_flat(settings) {
        return samples.to_vec();
    }

    let mut left_filters: Vec<Biquad> = settings
        .left
        .iter()
        .map(|b| Biquad::from_band(b, sample_rate as f64))
        .collect();
    let mut right_filters: Vec<Biquad> = settings
        .right
        .iter()
        .map(|b| Biquad::from_band(b, sample_rate as f64))
        .collect();

    let mut out = Vec::with_capacity(samples.len());
    let mut i = 0;
    while i + 1 < samples.len() {
        let mut l = samples[i] as f64;
        let mut r = samples[i + 1] as f64;
        for f in &mut left_filters {
            l = f.process(l);
        }
        for f in &mut right_filters {
            r = f.process(r);
        }
        out.push(l.clamp(-1.0, 1.0) as f32);
        out.push(r.clamp(-1.0, 1.0) as f32);
        i += 2;
    }
    out
}

/// Build a minimal 16-bit stereo WAV from f32 samples.
pub fn to_wav(samples: &[f32], sample_rate: u32, bit_depth: u32) -> Vec<u8> {
    let bit_depth = if bit_depth == 24 { 24 } else { 16 }; // only 16 or 24 supported
    let bytes_per_sample = bit_depth / 8;
    let num_samples = samples.len() as u32;
    let block_align = 2 * bytes_per_sample; // stereo
    let byte_rate = sample_rate * block_align;
    let data_size = num_samples * bytes_per_sample;
    let file_size = 36 + data_size;

    let mut wav = Vec::with_capacity(44 + data_size as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&file_size.to_le_bytes());
    wav.extend_from_slice(b"WAVE");
    wav.extend_from_slice(b"fmt ");
    wav.extend_from_slice(&16u32.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
    wav.extend_from_slice(&2u16.to_le_bytes()); // stereo
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&(block_align as u16).to_le_bytes());
    wav.extend_from_slice(&(bit_depth as u16).to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_size.to_le_bytes());

    for s in samples {
        let clamped = s.clamp(-1.0, 1.0);
        if bit_depth == 24 {
            let v = (clamped * 8_388_607.0) as i32;
            wav.extend_from_slice(&v.to_le_bytes()[..3]);
        } else {
            let v = (clamped * 32_767.0) as i16;
            wav.extend_from_slice(&v.to_le_bytes());
        }
    }
    wav
}
