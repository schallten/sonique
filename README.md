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
- **Database:** SQLite

## TODO

- frontend: handle /match response to show detected song (Detected component)
- frontend: add feedback UI (confirm/deny match)

## Contributors

- [docot04](https://github.com/docot04)
- [schallten](https://github.com/schallten)
