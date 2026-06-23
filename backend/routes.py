"""
ROUTE               TYPE    ACTION

/load               POST    takes Spotify track/album/playlist ID(s) and runs batch pipeline on multiple workers
/dashboard          GET     queries the entire DB for the dashboard
/dashboard          POST    queries the DB for specific entries and returns spotify metadata
/match              POST    takes user's mp3, runs pipeline to create hash, queries DB for matches, returns matches with %
/feedback           POST    after match, asks user if match was correct
"""

import time
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, UploadFile, File, Request
from concurrent.futures import ThreadPoolExecutor, as_completed
from engine.spotify_parser import extract_spotify_ids
from pipeline.load import process_spotify_track
from pipeline.db import get_dashboard, get_song, save_feedback, check_rate_limit
from pipeline.match import process_audio_sample

router = APIRouter()
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB upload size for /match


class LoadRequest(BaseModel):
    track_id: list[str] = []
    album_id: list[str] = []
    playlist_id: list[str] = []


class DashboardRequest(BaseModel):
    spotify_id: str


class FeedbackRequest(BaseModel):
    spotify_id: str
    is_correct: bool


@router.post("/load")
async def load_tracks(req: LoadRequest, max_workers: int = 5):
    all_track_ids = []

    # collect all track IDs before batch processing (tracks, albums, playlists)
    all_track_ids.extend(req.track_id or [])
    for album_id in req.album_id or []:
        try:
            album_tracks = extract_spotify_ids(album_id, "album")
            all_track_ids.extend(album_tracks)
        except Exception as e:
            print(f"[ERROR] Album {album_id} failed: {e}")
    for playlist_id in req.playlist_id or []:
        try:
            playlist_tracks = extract_spotify_ids(playlist_id, "playlist")
            all_track_ids.extend(playlist_tracks)
        except Exception as e:
            print(f"[ERROR] Playlist {playlist_id} failed: {e}")

    # remove duplicates before processing
    all_track_ids = list(dict.fromkeys(all_track_ids))

    if not all_track_ids:
        raise HTTPException(status_code=400, detail="No track IDs available to process")

    start_time = time.time()
    processed_count = 0
    total_tracks = len(all_track_ids)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_spotify_track, tid): tid for tid in all_track_ids
        }

        for future in as_completed(futures):
            tid = futures[future]
            try:
                success = future.result()
                if success:
                    processed_count += 1
            except Exception as e:
                print(f"[ERROR] Track {tid} failed: {e}")

    skipped_count = total_tracks - processed_count
    duration = round(time.time() - start_time, 2)
    average = round(duration / total_tracks, 2) if total_tracks else 0

    return {
        "message": "Processing complete",
        "details": {
            "processed": processed_count,
            "skipped": skipped_count,
            "duration": duration,
            "average": average,
        },
    }


@router.get("/dashboard")
async def dashboard():
    return {"data": get_dashboard()}


@router.post("/dashboard")
async def dashboard_post(req: DashboardRequest):
    if not req.spotify_id:
        raise HTTPException(status_code=400, detail="No Spotify ID provided")

    song = get_song(req.spotify_id)
    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    return {
        "spotify_ID": song.get("spotify_ID", ""),
        "youtube_ID": song.get("youtube_ID", ""),
        "title": song.get("title", ""),
        "artists": song.get("artists", ""),
        "cover": song.get("cover", ""),
        "album_name": song.get("album_name", ""),
        "release_date": song.get("release_date", ""),
        "duration_ms": song.get("duration_ms", 0),
    }


@router.post("/match")
async def match(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".mp3"):
        raise HTTPException(status_code=400, detail="Only MP3 files are supported")
    audio_bytes = await file.read()

    if len(audio_bytes) > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413, detail="File too large. Max size is 10 MB."
        )

    result = process_audio_sample(audio_bytes)

    if not result:
        return {"status": "no_match", "result": []}

    return {"status": "success", "result": result}


@router.post("/feedback")
async def feedback(req: FeedbackRequest, request: Request):
    # get user IP for rate limiting
    user_ip = request.client.host if request.client else "unknown"

    # rate limit: max 10 feedback requests per minute
    if not check_rate_limit(user_ip, "feedback", max_requests=10, window_seconds=60):
        raise HTTPException(
            status_code=429, detail="Too many feedback requests. Please wait a moment."
        )

    if not req.spotify_id:
        raise HTTPException(status_code=400, detail="No Spotify ID provided")

    save_feedback(req.spotify_id, user_ip, req.is_correct)
    return {"status": "ok", "message": "Feedback saved"}
