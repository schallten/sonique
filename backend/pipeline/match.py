import os
import uuid
from collections import Counter
from engine.preprocessor import preprocessor
from engine.spectrogram import audio_to_spectrogram
from engine.fingerprinting import generate_hashes
from engine.peak_maker import extract_peaks
from pipeline.db import get_db_files, get_fingerprints_from_db, get_song
from typing import TypedDict, Optional

TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)


class SongDetails(TypedDict):
    spotify_ID: str
    youtube_ID: str
    title: str
    artists: str
    cover: str
    album_name: str
    release_date: str
    duration_ms: int


def process_audio_sample(audio_bytes: bytes) -> list[dict[str, object]]:
    """saves the received file in a temp directory and returns the match results"""
    filename = f"{uuid.uuid4()}.mp3"
    file_path = os.path.join(TEMP_DIR, filename)

    with open(file_path, "wb") as f:
        f.write(audio_bytes)

    result = match(file_path)
    print(f"[BACKEND LOG] Match result: {result}")

    if os.path.exists(file_path):
        os.remove(file_path)

    return result


def get_song_details(spotify_id: str) -> Optional[SongDetails]:
    """gets song details from the database (cached metadata)"""
    song_data = get_song(spotify_id)
    if not song_data:
        return None

    return SongDetails(
        spotify_ID=song_data.get("spotify_ID", ""),
        youtube_ID=song_data.get("youtube_ID", ""),
        title=song_data.get("title", ""),
        artists=song_data.get("artists", ""),
        cover=song_data.get("cover", ""),
        album_name=song_data.get("album_name", ""),
        release_date=song_data.get("release_date", ""),
        duration_ms=song_data.get("duration_ms", 0),
    )


def match(file_path: str) -> list[dict[str, object]]:
    """
    matches an audio file against the fingerprint database.
    loads each DB one by one to avoid memory spikes.

    Args:
        file_path (str): path to the audio file

    Returns:
        list: list of dicts with 'song_details' and 'confidence', sorted by confidence
    """
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return []

    processed_path: Optional[str] = None
    try:
        processed_path = preprocessor(file_path)
        s_db = audio_to_spectrogram(processed_path)
        peaks = extract_peaks(s_db)
        input_fingerprints, _ = generate_hashes(peaks, None)
    except Exception as e:
        print(f"[ERROR] Could not generate fingerprints for {file_path}: {e}")
        return []
    finally:
        if processed_path and os.path.exists(processed_path):
            os.remove(processed_path)

    if not input_fingerprints:
        print("[WARN] No fingerprints generated from input audio")
        return []

    print(f"[BACKEND LOG] Generated {len(input_fingerprints)} fingerprints from the input audio.")

    input_fingerprints_with_time: dict[str, float] = {h: t for h, t in input_fingerprints}
    results: list[dict[str, object]] = []
    seen_songs: dict[str, int] = {}

    db_files = get_db_files()
    print(f"[BACKEND LOG] Searching across {len(db_files)} database(s)...")

    for db_path in db_files:
        db_name = os.path.basename(db_path)
        db_fingerprints = get_fingerprints_from_db(db_path)
        if not db_fingerprints:
            print(f"[BACKEND LOG] {db_name}: empty, skipping")
            continue

        print(f"[BACKEND LOG] {db_name}: {len(db_fingerprints)} fingerprints")

        db_fingerprints_by_song: dict[str, list[tuple[str, float]]] = {}
        for db_fp in db_fingerprints:
            spotify_id = db_fp["spotify_ID"]
            if spotify_id not in db_fingerprints_by_song:
                db_fingerprints_by_song[spotify_id] = []
            db_fingerprints_by_song[spotify_id].append((db_fp["hash_value"], db_fp["hash_time"]))

        for spotify_id, db_fps in db_fingerprints_by_song.items():
            time_offsets: list[float] = []
            for db_hash, db_time in db_fps:
                if db_hash in input_fingerprints_with_time:
                    input_time = input_fingerprints_with_time[db_hash]
                    time_offsets.append(db_time - input_time)

            if time_offsets:
                _, num_matches = Counter(time_offsets).most_common(1)[0]
                confidence: float = (num_matches / len(input_fingerprints)) * 100 if len(input_fingerprints) > 0 else 0
                print(f"[BACKEND LOG] Song {spotify_id}: {num_matches} matches, confidence {confidence}%")

                if spotify_id in seen_songs:
                    idx = seen_songs[spotify_id]
                    if confidence > results[idx]["confidence"]:
                        results[idx]["confidence"] = round(confidence, 2)
                else:
                    song_details = get_song_details(spotify_id)
                    if song_details:
                        seen_songs[spotify_id] = len(results)
                        results.append({
                            "song_details": song_details,
                            "confidence": round(confidence, 2),
                        })

        del db_fingerprints

    results.sort(key=lambda x: x["confidence"], reverse=True)  # type: ignore

    return results
