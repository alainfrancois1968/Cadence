//! In-memory playback queue.
//!
//! Starting a track fills the queue with every track from the same album.
//! Starting a track from a different album clears the queue and rebuilds it.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct QueueTrack {
    pub track_id: i32,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub album_id: String,
    pub path: String,
    pub duration: f64,
    pub track_number: Option<i32>,
    pub dr_score: Option<f64>,
    pub waveform: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PlayQueue {
    pub tracks: Vec<QueueTrack>,
    pub current: usize,
    pub album_id: Option<String>,
}

impl PlayQueue {
    pub fn clear(&mut self) {
        self.tracks.clear();
        self.current = 0;
        self.album_id = None;
    }

    pub fn current_track(&self) -> Option<&QueueTrack> {
        self.tracks.get(self.current)
    }

    pub fn next(&mut self) -> Option<&QueueTrack> {
        if self.current + 1 < self.tracks.len() {
            self.current += 1;
            self.tracks.get(self.current)
        } else {
            None
        }
    }

    pub fn previous(&mut self) -> Option<&QueueTrack> {
        if self.current > 0 {
            self.current -= 1;
            self.tracks.get(self.current)
        } else {
            None
        }
    }

    pub fn status(&self) -> QueueStatus {
        QueueStatus {
            album_id: self.album_id.clone(),
            current_index: self.current,
            length: self.tracks.len(),
            current: self.current_track().cloned(),
            tracks: self.tracks.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct QueueStatus {
    pub album_id: Option<String>,
    pub current_index: usize,
    pub length: usize,
    pub current: Option<QueueTrack>,
    pub tracks: Vec<QueueTrack>,
}
