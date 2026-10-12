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


lsof -ti :3000 | xargs kill -9                                                                                                                                                                                                                                                                                                                              
cargo run



curl "http://localhost:3000/qobuz/download?track_id=13176086&format_id=6"

curl http://localhost:3000/library/scan

sqlite3 ~/Projects/Cadence/cadence.db "SELECT id, title, artist, artist_id, album, path FROM tracks;"

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


download album_ids for artist
curl "http://localhost:3000/qobuz/queue-albums?artist_id=6760&limit=50"

sqlite3 ~/Projects/Cadence/cadence.db "SELECT album_id, artist, artist_id, title FROM album_queue LIMIT 5;"


sqlite3 ~/Projects/Cadence/cadence.db "SELECT title, artist, artist_id FROM tracks WHERE artist_id IS NOT NULL LIMIT 5;"

curl "http://localhost:3000/qobuz/queue-artists?q=Miles%20Davis&limit=200"

sqlite3 ~/Projects/Cadence/cadence.db "SELECT * FROM artists"

curl "http://localhost:3000/qobuz/download-artist-albums?artist_id=4900089&limit=5&format_id=6"







## API Endpoints

| Route                                   | Purpose                                                         | Files written                                       | SQL written                             | Notes                                            | Example                                                                              |
|-----------------------------------------|-----------------------------------------------------------------|-----------------------------------------------------|-----------------------------------------|--------------------------------------------------|--------------------------------------------------------------------------------------|
| `GET /health`                           | Health check                                                    | —                                                   | —                                       | Returns `"ok"`                                   | `curl http://localhost:3000/health`                                                  |
| `GET /qobuz/status`                     | Qobuz authentication status                                     | —                                                   | —                                       | Shows if the token is valid                      | `curl http://localhost:3000/qobuz/status`                                            |
| `GET /qobuz/search`                     | Search Qobuz albums and tracks                                  | —                                                   | —                                       | Returns albums and tracks                        | `curl "http://localhost:3000/qobuz/search?q=Miles%20Davis&limit=10"`                 |
| `GET /qobuz/stream`                     | Get a temporary stream URL                                      | —                                                   | —                                       | URL expires after a short time                   | `curl "http://localhost:3000/qobuz/stream?track_id=13176086&format_id=6"`            |
| `GET /qobuz/download`                   | Download a track or a full album                                | FLAC files and `cover.jpg` under `library/Qobuz/…`  | Upserts rows in `tracks`                | Uses ID-prefixed folders                         | `curl "http://localhost:3000/qobuz/download?album_id=5099749522428&format_id=6"`     |
| `GET /qobuz/album-id`                   | Get the first album ID for a search                             | —                                                   | —                                       | Helper for downloads                             | `curl "http://localhost:3000/qobuz/album-id?q=kind+of+blue"`                         |
| `GET /qobuz/queue-albums`               | Queue albums for an artist ID                                   | —                                                   | Inserts into `album_queue`              | Uses `get_release_list`                          | `curl "http://localhost:3000/qobuz/queue-albums?artist_id=6760&limit=50"`            |
| `GET /qobuz/queue-artists`              | Queue artists matching a name                                   | —                                                   | Inserts into `artists`                  | Search-based                                     | `curl "http://localhost:3000/qobuz/queue-artists?q=Miles%20Davis&limit=100"`         |
| `GET /qobuz/queue-albums-from-artists`  | Queue albums for every artist in the artists table              | —                                                   | Inserts into `album_queue`              | Skips artists that already have albums queued    | `curl "http://localhost:3000/qobuz/queue-albums-from-artists?limit=20"`              |
| `GET /qobuz/download-artist-albums`     | List album IDs for an artist                                    | —                                                   | —                                       | Does not download files                          | `curl "http://localhost:3000/qobuz/download-artist-albums?artist_id=6760&limit=10"`  |
| `GET /library/search`                   | Fuzzy search the local library                                  | —                                                   | Reads from `tracks`                     | Handles typos                                    | `curl "http://localhost:3000/library/search?q=miles%20davids"`                       |
| `GET /library/scan`                     | Synchronize the library folder with SQLite                      | —                                                   | Upserts and deletes rows in `tracks`    | Preserves existing `artist_id` values            | `curl http://localhost:3000/library/scan`                                            |
| `GET /library/stream`                   | Stream a local file                                             | —                                                   | —                                       | Loads the whole file into memory                 | `curl "http://localhost:3000/library/stream?path=library/Qobuz/…"`                   |
| `GET /library/ai-search`                | LLM-powered semantic search                                     | —                                                   | Reads artist names from `tracks`        | Uses Gemini                                      | `curl "http://localhost:3000/library/ai-search?q=jazz%20legends"`                    |
