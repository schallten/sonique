import os
from pipeline.db import save_fingerprints_batch, song_exists
from engine.spotify_parser import spotify_parser
from engine.yt_scraper import yt_downloader
from engine.preprocessor import preprocessor
from engine.spectrogram import audio_to_spectrogram
from engine.peak_maker import extract_peaks
from engine.fingerprinting import generate_hashes

def process_spotify_track(track_id: str):
    """runs complete pipeline for a spotify ID
    **PIPELINE:** check if exists > Spotify metadata > YT search & download > preprocessing > spectrogram > fingerprinting > save to DB > cleanup > return
    **PARAMS:** track_id (spotify)
    **RETURN:** list of dicts [{spotify_ID, youtube_ID, hash, time}]"""
    print(f"\n[START] Processing Spotify ID: {track_id}")

    try:
        # 1: check if song already exists
        if song_exists(track_id):
            print(f"[WARN] Skipping track {track_id}: Song already exists")
            return [], True

        # 2: Spotify metadata
        try:
            info = spotify_parser(track_id)
        except ValueError as e:
            print(f"[WARN] Skipping track {track_id}: {e}")
            return [], True

        title = info.get("title")
        artists = info.get("artists")
        if not title or not artists:
            print(f"[WARN] Invalid metadata for {track_id}, skipping...")
            return [], True

        print(f"[INFO] Track: {title} - {artists}")

        # 3: YT search & download
        query = f"{title} {artists}"
        safe_filename = f"{track_id}"

        try:
            audio_path, youtube_id = yt_downloader(query, safe_filename)
        except ValueError as e:
            print(f"[WARN] Skipping track {track_id}: {e}")
            return [], True

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
        fingerprints = [
            {
                "spotify_ID": track_id,
                "youtube_ID": youtube_id,
                "hash_value": h,
                "hash_time": t,
            }
            for h, t in fingerprints_tuple
        ]
        print(f"[INFO] Generated {len(fingerprints)} fingerprints")

        # 7: save to DB
        save_fingerprints_batch(fingerprints)

        # 8: cleanup
        for path in [audio_path, processed_path]:
            if os.path.exists(path):
                os.remove(path)

        print(f"[DONE] Finished processing {track_id}\n")

    except Exception as e:
        print(f"[ERROR] Failed to process {track_id}: {e}")
        return [], True

if __name__ == "__main__":
    ids = [
        "4yMMsS5QaEcsHZkKwdEyLv",
        "4VbpKAZKVxzY7JpGQ34zMj",
        "3lcRYiY6NbuxNTf5hjbyyu",
        "2yR2sziCF4WEs3klW1F38d",
        "7mykoq6R3BArsSpNDjFQTm",
    ]

    for track_id in ids:
        process_spotify_track(track_id)
