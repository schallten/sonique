import os
from yt_dlp import YoutubeDL
from pipeline.db import save_fingerprints_batch, save_song_metadata, song_exists, FingerprintDict
from engine.spotify_parser import spotify_parser, TrackMetadata
from engine.yt_scraper import yt_downloader
from engine.preprocessor import preprocessor
from engine.spectrogram import audio_to_spectrogram
from engine.peak_maker import extract_peaks
from engine.fingerprinting import generate_hashes


def extract_yt_playlist_ids(playlist_url: str) -> list[str]:
    """extract all video IDs from a YouTube playlist URL
    **PARAMS:** playlist_url (YouTube playlist URL)
    **RETURN:** list of YouTube video IDs"""
    opts = {"quiet": True, "extract_flat": True, "noplaylist": False}
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(playlist_url, download=False)
    return [entry["id"] for entry in info.get("entries", [])]


def yt_downloader_direct(youtube_id: str, filename: str) -> tuple[str, str]:
    """download audio from a specific YouTube video ID"""
    opts = {
        "format": "bestaudio/best",
        "postprocessors": [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "64"}
        ],
        "noplaylist": True,
        "cachedir": False,
        "quiet": True,
        "outtmpl": os.path.join("downloads", f"{filename}.%(ext)s"),
    }
    url = f"https://www.youtube.com/watch?v={youtube_id}"
    with YoutubeDL(opts) as ydl:
        ydl.download([url])
    return os.path.join("downloads", f"{filename}.mp3"), youtube_id


def process_youtube_track(youtube_id: str) -> bool:
    """runs complete pipeline for a YouTube video ID (no Spotify keys needed)
    **PIPELINE:** check if exists > YT metadata > download > preprocessing > spectrogram > fingerprinting > save to DB > cleanup > return
    **PARAMS:** youtube_id (YouTube video ID)
    **RETURN:** True if processed successfully, False otherwise"""
    print(f"\n[START] Processing YouTube ID: {youtube_id}")

    try:
        # 1: check if song already exists (check by youtube_ID)
        if song_exists(youtube_id):
            print(f"[WARN] Skipping track {youtube_id}: Song already exists")
            return False

        # 2: get metadata from YouTube
        meta_opts = {"quiet": True, "extract_flat": False, "noplaylist": True}
        with YoutubeDL(meta_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={youtube_id}", download=False)

        title = info.get("title", "")
        artists = info.get("uploader") or info.get("channel") or ""
        thumbnails = info.get("thumbnails", [])
        cover_url = thumbnails[-1]["url"] if thumbnails else None

        if not title:
            print(f"[WARN] Invalid metadata for {youtube_id}, skipping...")
            return False

        print(f"[INFO] Track: {title} - {artists}")

        # 3: download audio
        safe_filename = f"{youtube_id}"
        audio_path, _ = yt_downloader_direct(youtube_id, safe_filename)
        print(f"[INFO] Downloaded audio: {audio_path}")

        # 4: preprocessing
        processed_path = preprocessor(audio_path)
        print(f"[INFO] Preprocessed audio: {processed_path}")

        # 5: generate spectrogram
        S_db = audio_to_spectrogram(processed_path)
        print(f"[INFO] Spectrogram shape: {S_db.shape}")

        peaks = extract_peaks(S_db)
        print(f"[INFO] Extracted {len(peaks)} peaks from spectrogram")

        # 6: fingerprinting
        fingerprints_tuple, _ = generate_hashes(peaks, youtube_id)
        fingerprints: list[FingerprintDict] = [
            {
                "spotify_ID": youtube_id,
                "youtube_ID": youtube_id,
                "hash_value": h,
                "hash_time": t,
            }
            for h, t in fingerprints_tuple
        ]
        print(f"[INFO] Generated {len(fingerprints)} fingerprints")

        # 7: save fingerprints to DB
        save_fingerprints_batch(fingerprints)

        # 8: save metadata to DB
        metadata: TrackMetadata = {
            "title": title,
            "artists": artists,
            "album_name": None,
            "cover": cover_url,
            "release_date": None,
            "duration_ms": None,
        }
        save_song_metadata(youtube_id, youtube_id, metadata)

        # 9: cleanup temp files
        for path in [audio_path, processed_path]:
            if os.path.exists(path):
                os.remove(path)

        print(f"[DONE] Finished processing {youtube_id}\n")
        return True

    except Exception as e:
        print(f"[ERROR] Failed to process {youtube_id}: {e}")
        return False


def process_spotify_track(track_id: str) -> bool:
    """runs complete pipeline for a spotify ID
    **PIPELINE:** check if exists > Spotify metadata > YT search & download > preprocessing > spectrogram > fingerprinting > save to DB > cleanup > return
    **PARAMS:** track_id (spotify)
    **RETURN:** True if processed successfully, False otherwise"""
    print(f"\n[START] Processing Spotify ID: {track_id}")

    try:
        # 1: check if song already exists
        if song_exists(track_id):
            print(f"[WARN] Skipping track {track_id}: Song already exists")
            return False

        # 2: Spotify metadata
        try:
            info: TrackMetadata = spotify_parser(track_id)
        except ValueError as e:
            print(f"[WARN] Skipping track {track_id}: {e}")
            return False

        title: str = info.get("title")
        artists: str = info.get("artists")
        if not title or not artists:
            print(f"[WARN] Invalid metadata for {track_id}, skipping...")
            return False

        print(f"[INFO] Track: {title} - {artists}")

        # 3: YT search & download
        query = f"{title} {artists}"
        safe_filename = f"{track_id}"

        try:
            audio_path, youtube_id = yt_downloader(query, safe_filename)
        except ValueError as e:
            print(f"[WARN] Skipping track {track_id}: {e}")
            return False

        print(f"[INFO] Downloaded audio: {audio_path}")
        print(f"[INFO] YouTube ID: {youtube_id}")

        # 4: preprocessing
        processed_path = preprocessor(audio_path)
        print(f"[INFO] Preprocessed audio: {processed_path}")

        # 5: generate spectrogram
        S_db = audio_to_spectrogram(processed_path)
        print(f"[INFO] Spectrogram shape: {S_db.shape}")

        peaks = extract_peaks(S_db)
        print(f"[INFO] Extracted {len(peaks)} peaks from spectrogram")

        # 6: fingerprinting
        fingerprints_tuple, _ = generate_hashes(peaks, track_id)
        fingerprints: list[FingerprintDict] = [
            {
                "spotify_ID": track_id,
                "youtube_ID": youtube_id,
                "hash_value": h,
                "hash_time": t,
            }
            for h, t in fingerprints_tuple
        ]
        print(f"[INFO] Generated {len(fingerprints)} fingerprints")

        # 7: save fingerprints to DB
        save_fingerprints_batch(fingerprints)

        # 8: save metadata to DB (so we dont need to call spotify API every time)
        save_song_metadata(track_id, youtube_id, info)

        # 9: cleanup temp files
        for path in [audio_path, processed_path]:
            if os.path.exists(path):
                os.remove(path)

        print(f"[DONE] Finished processing {track_id}\n")
        return True

    except Exception as e:
        print(f"[ERROR] Failed to process {track_id}: {e}")
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m pipeline.load <youtube_url>      # YouTube playlist or video URL")
        print("  python -m pipeline.load <spotify_id>       # Spotify track ID")
        sys.exit(1)

    target = sys.argv[1]

    if "youtube.com" in target or "youtu.be" in target:
        # YouTube URL - extract track IDs and process each
        track_ids = extract_yt_playlist_ids(target)
        print(f"[INFO] Found {len(track_ids)} tracks in playlist")
        for tid in track_ids:
            process_youtube_track(tid)
    else:
        # assume Spotify track ID
        process_spotify_track(target)