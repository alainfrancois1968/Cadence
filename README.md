# Cadence Music Server

A cross-platform Rust music server for local files + Qobuz.

## Status

Minimal skeleton with:
- Axum HTTP server
- Qobuz authentication via `qobuz-api`
- Placeholder search endpoint
- Modular structure for future local library + additional sources

## Prerequisites

- Rust (stable)
- A Qobuz account (Studio or Sublime recommended for hi-res)

## Setup

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Fill in your Qobuz credentials in `.env`.

3. Build and run:
   ```bash
   cargo run
   ```

4. Test endpoints:
   - `GET http://localhost:3000/health`
   - `GET http://localhost:3000/qobuz/status`
   - `GET http://localhost:3000/qobuz/search?q=miles+davis`

## Next steps

- Wire real `QobuzApiService` search / get_album / get_track / download methods
- Add local library scanner (`lofty` + SQLite)
- Add stream URL endpoint
- Add download-to-library endpoint
- Build a simple frontend

## Notes

- `qobuz-api` is unofficial. The API surface may evolve.
- Streaming URLs are time-limited.
- Downloads write tagged FLAC/MP3 files into a local folder that can later be scanned.


curl "http://localhost:3000/qobuz/download?track_id=13176086&format_id=6"

curl http://localhost:3000/library/scan

sqlite3 ~/Projects/Cadence/cadence.db "SELECT id, title, artist, album, path FROM tracks;"

curl "http://localhost:3000/library/search?q=blues"


http://localhost:3000/library/stream?path=library/Miles%20Davis/1959/Kind%20Of%20Blue/04.%20All%20Blues.flac

AQ.Ab8RN6J1dyIfPYHBP1tGd8rCmgeZcQ_FsHpEG48LN6YHB-nLTg

https://aistudio.google.com/app/api-keys




album id search
curl "http://localhost:3000/qobuz/album-id?q=kind+of+blue"

id:5099749522428

curl "http://localhost:3000/qobuz/download?album_id=5099749522428&format_id=6"


curl "http://localhost:3000/qobuz/search?q=A%20Night%20of%20Miles%20Davis&limit=5"


curl "http://localhost:3000/qobuz/album-id?q=A%20Night%20of%20Miles%20Davis"
id:dvre8bsckjwmm

curl "http://localhost:3000/qobuz/download?album_id=dvre8bsckjwmm&format_id=6"


curl "http://localhost:3000/qobuz/album-id?q=equinox"
id:oewcjd3wc0ksa

curl "http://localhost:3000/qobuz/download?album_id=oewcjd3wc0ksa&format_id=6"