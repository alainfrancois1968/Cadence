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