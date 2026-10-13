use crate::dr_score;
use lofty::file::{AudioFile, TaggedFileExt};
use lofty::tag::Accessor;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use walkdir::WalkDir;

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
                updated_at TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Add column if the table already existed without it
        let _ = sqlx::query("ALTER TABLE tracks ADD COLUMN dr_score REAL")
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
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO tracks (path, title, artist, artist_id, album, album_id, track_id, track_number, duration, format, stream_url, genre, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn search(&self, query: &str) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let like = format!("%{}%", query);
        let rows = sqlx::query(
            r#"
            SELECT path, title, artist, album, track_number, duration, format, stream_url, genre, dr_score, updated_at
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
            )
            .await?;

            // Dynamic range – computed on scan only
            if let Some(score) = dr_score::score_and_log(path, 0) {
                self.set_dr_score(&path_str, score).await.ok();
            }

            count += 1;
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
}
