use crate::dr_score;
use lofty::file::{AudioFile, TaggedFileExt};
use lofty::tag::Accessor;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use walkdir::WalkDir;

use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

pub struct Library {
    pool: SqlitePool,
}

impl Library {
    pub async fn new(db_path: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite://{}?mode=rwc", db_path))
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL,
                title TEXT,
                artist TEXT,
                artist_id INTEGER,
                album TEXT,
                album_id TEXT,
                track_id INTEGER,
                track_number INTEGER,
                duration REAL,
                format TEXT,
                stream_url TEXT,
                genre TEXT,
                dr_score REAL,
                waveform TEXT,
                updated_at TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Add columns if the table already existed without them
        let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN dr_score REAL")
            .execute(&pool)
            .await;
        let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN waveform TEXT")
            .execute(&pool)
            .await;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS genres (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS track_genres (
                track_id INTEGER NOT NULL,
                genre_id INTEGER NOT NULL,
                PRIMARY KEY (track_id, genre_id)
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS artists (
                artist_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS album_queue (
                album_id TEXT PRIMARY KEY,
                artist TEXT,
                artist_id INTEGER,
                title TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn upsert_track(
        &self,
        path: &str,
        title: Option<&str>,
        artist: Option<&str>,
        artist_id: Option<i32>,
        album: Option<&str>,
        album_id: Option<&str>,
        track_id: Option<i32>,
        track_number: Option<i32>,
        duration: Option<f64>,
        format: Option<&str>,
        stream_url: Option<&str>,
        genre: Option<&str>,
        waveform: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO tracks (path, title, artist, artist_id, album, album_id, track_id, track_number, duration, format, stream_url, genre, waveform, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(path) DO UPDATE SET
                title = excluded.title,
                artist = excluded.artist,
                artist_id = COALESCE(excluded.artist_id, tracks.artist_id),
                album = excluded.album,
                album_id = COALESCE(excluded.album_id, tracks.album_id),
                track_id = COALESCE(excluded.track_id, tracks.track_id),
                track_number = excluded.track_number,
                duration = excluded.duration,
                format = excluded.format,
                stream_url = excluded.stream_url,
                genre = excluded.genre,
                waveform = COALESCE(excluded.waveform, tracks.waveform),
                updated_at = datetime('now')
            "#,
        )
        .bind(path)
        .bind(title)
        .bind(artist)
        .bind(artist_id)
        .bind(album)
        .bind(album_id)
        .bind(track_id)
        .bind(track_number)
        .bind(duration)
        .bind(format)
        .bind(stream_url)
        .bind(genre)
        .bind(waveform)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn search(&self, query: &str) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let like = format!("%{}%", query);
        let rows = sqlx::query(
            r#"
            SELECT path, title, artist, album, track_number, duration, format, stream_url, genre, dr_score, waveform, updated_at
            FROM tracks
            WHERE title LIKE ?1 OR artist LIKE ?1 OR album LIKE ?1 OR genre LIKE ?1
            ORDER BY artist, album, track_number
            "#,
        )
        .bind(&like)
        .fetch_all(&self.pool)
        .await?;

        let tracks = rows
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "path": r.try_get::<String, _>("path").ok(),
                    "title": r.try_get::<String, _>("title").ok(),
                    "artist": r.try_get::<String, _>("artist").ok(),
                    "album": r.try_get::<String, _>("album").ok(),
                    "track_number": r.try_get::<i32, _>("track_number").ok(),
                    "duration": r.try_get::<f64, _>("duration").ok(),
                    "format": r.try_get::<String, _>("format").ok(),
                    "stream_url": r.try_get::<String, _>("stream_url").ok(),
                    "genre": r.try_get::<String, _>("genre").ok(),
                    "dr_score": r.try_get::<f64, _>("dr_score").ok(),
                    "waveform": r.try_get::<String, _>("waveform").ok(),
                    "updated_at": r.try_get::<String, _>("updated_at").ok(),
                })
            })
            .collect();

        Ok(tracks)
    }

    pub async fn scan_directory(&self, dir: &str) -> Result<i64, sqlx::Error> {
        let mut count = 0i64;
        for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            // Skip anything under an "upsampled" directory
            if path.components().any(|c| c.as_os_str() == "upsampled") {
                continue;
            }

            if !path.is_file() {
                continue;
            }
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();
            if !matches!(ext.as_str(), "flac" | "wav" | "mp3" | "m4a" | "ogg") {
                continue;
            }

            let path_str = path.to_string_lossy().to_string();
            let mut title = None;
            let mut artist = None;
            let mut album = None;
            let mut track_number = None;
            let mut duration = None;
            let mut genre = None;

            if let Ok(tagged) = lofty::read_from_path(path) {
                if let Some(tag) = tagged.primary_tag() {
                    title = tag.title().map(|s| s.to_string());
                    artist = tag.artist().map(|s| s.to_string());
                    album = tag.album().map(|s| s.to_string());
                    track_number = tag.track().map(|n| n as i32);
                    genre = tag.genre().map(|s| s.to_string());
                }
                duration = Some(tagged.properties().duration().as_secs_f64());
            }

            let format = Some(ext.as_str());
            let stream_url = Some(format!(
                "/library/stream_local?path={}",
                urlencoding::encode(&path_str)
            ));

            // Compute stereo waveform only if not already stored
            let waveform = if self.get_waveform(&path_str).await.ok().flatten().is_none() {
                compute_stereo_waveform(&path_str, 150).ok()
            } else {
                None
            };

            self.upsert_track(
                &path_str,
                title.as_deref(),
                artist.as_deref(),
                None, // artist_id
                album.as_deref(),
                None, // album_id
                None, // track_id
                track_number,
                duration,
                format,
                stream_url.as_deref(),
                genre.as_deref(),
                waveform.as_deref(),
            )
            .await?;

            // Dynamic range – only if not already stored
            if self.get_dr_score(&path_str).await.ok().flatten().is_none() {
                if let Some(score) = dr_score::score_and_log(path, 0) {
                    self.set_dr_score(&path_str, score).await.ok();
                }
            }

            count += 1;
        }
        // Delete rows from the tracks table when the file is gone,
        // but keep the row if its track_id is unique.
        let rows = sqlx::query("SELECT path, track_id FROM tracks")
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
            let path: String = row.try_get("path").unwrap_or_default();
            if std::path::Path::new(&path).exists() {
                continue;
            }

            let track_id: Option<i32> = row.try_get("track_id").ok().flatten();

            // Keep the row if this track_id appears only once
            if let Some(tid) = track_id {
                let count: (i64,) =
                    sqlx::query_as("SELECT COUNT(*) FROM tracks WHERE track_id = ?")
                        .bind(tid)
                        .fetch_one(&self.pool)
                        .await?;

                if count.0 == 1 {
                    tracing::info!("Keeping unique track_id {} (file missing): {}", tid, path);
                    continue;
                }
            }

            // File is gone and track_id is missing or duplicated → delete the row only
            sqlx::query("DELETE FROM tracks WHERE path = ?")
                .bind(&path)
                .execute(&self.pool)
                .await?;
            tracing::info!("Deleted row for missing file: {}", path);
        }
        Ok(count)
    }

    pub async fn set_dr_score(&self, path: &str, score: f64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE tracks SET dr_score = ? WHERE path = ?")
            .bind(score)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Returns the existing DR score for a path, if any.
    pub async fn get_dr_score(&self, path: &str) -> Result<Option<f64>, sqlx::Error> {
        let row: Option<(Option<f64>,)> =
            sqlx::query_as("SELECT dr_score FROM tracks WHERE path = ?")
                .bind(path)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(s,)| s))
    }

    /// Returns the existing waveform JSON for a path, if any.
    pub async fn get_waveform(&self, path: &str) -> Result<Option<String>, sqlx::Error> {
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT waveform FROM tracks WHERE path = ?")
                .bind(path)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(s,)| s))
    }

    pub async fn find_path_by_track_id(
        &self,
        track_id: i32,
    ) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query("SELECT path FROM tracks WHERE path LIKE ?")
            .bind(format!("%{}.flac", track_id))
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.and_then(|r| r.try_get("path").ok()))
    }

    pub async fn track_exists(&self, track_id: i32) -> Result<bool, sqlx::Error> {
        let row = sqlx::query("SELECT 1 FROM tracks WHERE track_id = ? LIMIT 1")
            .bind(track_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    pub async fn track_path(&self, track_id: i32) -> Result<Option<String>, sqlx::Error> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT path FROM tracks WHERE track_id = ? LIMIT 1")
                .bind(track_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(p,)| p))
    }
    /// Returns (dr_score, waveform) for a path, if present.
    pub async fn get_dr_and_waveform(
        &self,
        path: &str,
    ) -> Result<(Option<f64>, Option<String>), sqlx::Error> {
        let row: Option<(Option<f64>, Option<String>)> =
            sqlx::query_as("SELECT dr_score, waveform FROM tracks WHERE path = ?")
                .bind(path)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.unwrap_or((None, None)))
    }
}

/// Compute stereo peak arrays and return JSON {"l":[...],"r":[...]}.
/// `points` is the number of peaks per channel (e.g. 150).
fn compute_stereo_waveform(path: &str, points: usize) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());

    let mut hint = Hint::new();
    if let Some(ext) = std::path::Path::new(path).extension() {
        hint.with_extension(ext.to_string_lossy().as_ref());
    }

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
        .find(|t| t.codec_params.sample_rate.is_some())
        .ok_or("no audio track")?;

    let track_id = track.id;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(|e| e.to_string())?;

    let channels = track.codec_params.channels.map(|c| c.count()).unwrap_or(2);
    let sample_rate = track.codec_params.sample_rate.unwrap_or(44100) as usize;
    let duration_secs = track
        .codec_params
        .n_frames
        .and_then(|n| {
            track
                .codec_params
                .sample_rate
                .map(|sr| n as f64 / sr as f64)
        })
        .unwrap_or(180.0);
    let estimated_frames = (duration_secs * sample_rate as f64) as usize;
    let frames_per_point = (estimated_frames / points).max(1);

    let mut left_peaks = vec![0.0f32; points];
    let mut right_peaks = vec![0.0f32; points];
    let mut bin = 0usize;
    let mut left_max = 0.0f32;
    let mut right_max = 0.0f32;
    let mut count = 0usize;

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
        let samples = buf.samples();

        let mut i = 0;
        while i + 1 < samples.len() {
            let l = samples[i].abs();
            let r = if channels > 1 {
                samples[i + 1].abs()
            } else {
                l
            };
            if l > left_max {
                left_max = l;
            }
            if r > right_max {
                right_max = r;
            }
            count += 1;
            i += channels;

            if count >= frames_per_point && bin < points {
                left_peaks[bin] = left_max;
                right_peaks[bin] = right_max;
                bin += 1;
                left_max = 0.0;
                right_max = 0.0;
                count = 0;
            }
        }
    }

    // Normalize each channel independently to 0.0–1.0
    let lmax = left_peaks
        .iter()
        .cloned()
        .fold(0.0f32, f32::max)
        .max(0.0001);
    let rmax = right_peaks
        .iter()
        .cloned()
        .fold(0.0f32, f32::max)
        .max(0.0001);
    for p in &mut left_peaks {
        *p = (*p / lmax).clamp(0.0, 1.0);
    }
    for p in &mut right_peaks {
        *p = (*p / rmax).clamp(0.0, 1.0);
    }

    Ok(serde_json::json!({ "l": left_peaks, "r": right_peaks }).to_string())
}
