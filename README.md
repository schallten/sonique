# Sonique

A music recognition app inspired by **Shazam**
Currently under development

## Overview

Sonique will identify songs by listening to short audio samples and matching them to a database

## Status

- backend routes completed (including /feedback)
- frontend completed
- complete pipeline ready for processing and indexing new songs
- song metadata cached in DB (no more Spotify API calls on every lookup)
- rate limiting on feedback endpoint

## Stack

- **Backend:** FastAPI (python)
- **Frontend:** React Native (Expo)
- **Database:** SQLite (multi-db, auto-splits at 50MB)

## Usage

### Index a YouTube playlist

```bash
cd backend
uv run python -m pipeline.load "https://www.youtube.com/playlist?list=PLAYLIST_ID" --workers 3
```

- `--workers N` — parallel download/processing threads (default: 3, max: 8)
- Already-indexed songs are automatically skipped
- DB auto-rotates to a new file when the current one exceeds 50MB

### Download remote databases (optional)

Set `DB_URLS` env variable — databases auto-download on startup if the `database/` folder is empty:

```bash
DB_URLS='["https://example.com/db1.db","https://example.com/db2.db"]' uv run uvicorn main:app
```

Or manually:

```bash
cd backend
DB_URLS='["https://example.com/db1.db","https://example.com/db2.db"]' uv run python db_downloader.py
```

## TODO

- frontend: handle /match response to show detected song (Detected component)
- frontend: add feedback UI (confirm/deny match)

## Contributors

- [docot04](https://github.com/docot04)
- [schallten](https://github.com/schallten)
