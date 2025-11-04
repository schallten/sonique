import os
import uuid
from collections import Counter
from engine.preprocessor import preprocessor
from engine.spectrogram import audio_to_spectrogram
from engine.fingerprinting import generate_hashes
from engine.peak_maker import extract_peaks
from engine.spotify_parser import spotify_parser
from pipeline.db import get_all_fingerprints, get_song

TEMP_DIR = "temp"
os.makedirs(TEMP_DIR, exist_ok=True)

def process_audio_sample(audio_bytes: bytes) -> list:
    """
    Saves the received file in a **temp** directory and returns the match results.
    """
    filename = f"{uuid.uuid4()}.mp3"
    file_path = os.path.join(TEMP_DIR, filename)

    with open(file_path, "wb") as f:
        f.write(audio_bytes)

    result = match(file_path)
    print(f"[BACKEND LOG] Match result: {result}")
    # Clean up the temporary file
    if os.path.exists(file_path):
        os.remove(file_path)
    return result

def get_song_details(spotify_id):
    """Gets song details from the database."""
    song_data = get_song(spotify_id)
    if not song_data:
        return None
    
    return {
        "spotify_ID": song_data[0],
        "youtube_ID": song_data[1],
        "title": song_data[2],
        "artists": song_data[3],
        "cover": song_data[4],
        "album_name": song_data[5],
        "release_date": song_data[6],
        "duration_ms": song_data[7],
    }

def match(file_path: str):
    """
    Matches an audio file against the fingerprint database.

    Args:
        file_path (str): Path to the audio file.

    Returns:
        list: A list of dictionaries, where each dictionary contains
              'song_details' and 'confidence'. The list is sorted by
              confidence in descending order.
    """
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return []

    db_fingerprints = get_all_fingerprints()
    if not db_fingerprints:
        print("[ERROR] Database is empty or could not be read.")
        return []
    print(f"[BACKEND LOG] Fetched {len(db_fingerprints)} fingerprints from the database.")

    # Generate fingerprints for the input audio file
    processed_path = None
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
    
    print(f"[BACKEND LOG] Generated {len(input_fingerprints)} fingerprints from the input audio.")

    input_fingerprints_with_time = {h: t for h, t in input_fingerprints}
    print(f"[BACKEND LOG] Sample of input hashes: {list(input_fingerprints_with_time.keys())[:5]}")

    # Group database fingerprints by spotify_ID
    db_fingerprints_by_song = {}
    for db_fp in db_fingerprints:
        spotify_id = db_fp["spotify_ID"]
        if spotify_id not in db_fingerprints_by_song:
            db_fingerprints_by_song[spotify_id] = []
        db_fingerprints_by_song[spotify_id].append((db_fp["hash_value"], db_fp["hash_time"]))

    # Print a sample of database hashes
    if db_fingerprints_by_song:
        first_song_id = list(db_fingerprints_by_song.keys())[0]
        sample_db_hashes = [fp[0] for fp in db_fingerprints_by_song[first_song_id][:5]]
        print(f"[BACKEND LOG] Sample of database hashes for song {first_song_id}: {sample_db_hashes}")

    # Find potential matches and calculate confidence
    results = []
    for spotify_id, db_fps in db_fingerprints_by_song.items():
        time_offsets = []
        for db_hash, db_time in db_fps:
            if db_hash in input_fingerprints_with_time:
                input_time = input_fingerprints_with_time[db_hash]
                time_offsets.append(db_time - input_time)

        if time_offsets:
            _, num_matches = Counter(time_offsets).most_common(1)[0]
            confidence = (num_matches / len(input_fingerprints)) * 100 if len(input_fingerprints) > 0 else 0
            print(f"[BACKEND LOG] Song {spotify_id}: Found {num_matches} matching hashes. Confidence: {confidence}%")

            song_details = get_song_details(spotify_id)
            if song_details:
                results.append({
                    "song_details": song_details,
                    "confidence": round(confidence, 2)
                })

    results.sort(key=lambda x: x["confidence"], reverse=True)

    return results