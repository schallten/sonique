"""
DB Downloader & Merger

Downloads database files from URLs specified in the DB_URLS env variable.
If no URLs are set, relies entirely on local databases.

Env var format (JSON array):
    DB_URLS=["https://example.com/db1.db","https://example.com/db2.db"]

Usage:
    python db_downloader.py          # download dbs from DB_URLS
    python db_downloader.py --merge  # download and merge into local dbs
"""

import os
import sys
import json
import shutil
import sqlite3
import tempfile
import urllib.request
from typing import Optional

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database")


def _get_existing_db_count() -> int:
    """count existing db files to avoid naming collisions"""
    if not os.path.exists(DB_DIR):
        return 0
    return len([f for f in os.listdir(DB_DIR) if f.endswith(".db")])


def download_databases() -> list[str]:
    """download databases from URLs in DB_URLS env var.

    Returns list of downloaded file paths."""
    db_urls_raw: str = os.environ.get("DB_URLS", "")
    if not db_urls_raw:
        print("[DB DOWNLOADER] No DB_URLS set, skipping download")
        return []

    try:
        urls: list[str] = json.loads(db_urls_raw)
    except json.JSONDecodeError as e:
        print(f"[DB DOWNLOADER] Failed to parse DB_URLS: {e}")
        return []

    if not urls:
        print("[DB DOWNLOADER] DB_URLS is empty, skipping download")
        return []

    os.makedirs(DB_DIR, exist_ok=True)
    downloaded: list[str] = []
    counter = _get_existing_db_count()

    for url in urls:
        counter += 1
        filename = f"db{counter}.db"
        filepath = os.path.join(DB_DIR, filename)

        try:
            print(f"[DB DOWNLOADER] Downloading {url} -> {filename}")
            urllib.request.urlretrieve(url, filepath)

            conn: Optional[sqlite3.Connection] = None
            try:
                conn = sqlite3.connect(filepath)
                conn.execute("SELECT 1 FROM sqlite_master")
                print(f"[DB DOWNLOADER] Verified {filename}")
            except Exception as e:
                print(f"[DB DOWNLOADER] Invalid DB file {filename}, removing: {e}")
                os.remove(filepath)
                counter -= 1
                continue
            finally:
                if conn:
                    conn.close()

            downloaded.append(filepath)
            print(f"[DB DOWNLOADER] Downloaded {filename} ({os.path.getsize(filepath) / (1024*1024):.1f}MB)")
        except Exception as e:
            print(f"[DB DOWNLOADER] Failed to download {url}: {e}")

    print(f"[DB DOWNLOADER] Downloaded {len(downloaded)}/{len(urls)} database(s)")
    return downloaded


def merge_databases(source_paths: list[str]) -> bool:
    """merge multiple db files into a single local db.

    Creates a new db file and copies all data from source dbs."""
    if not source_paths:
        return False

    os.makedirs(DB_DIR, exist_ok=True)
    counter = _get_existing_db_count()
    merged_name = f"db{counter + 1}.db"
    merged_path = os.path.join(DB_DIR, merged_name)

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(merged_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spotify_ID VARCHAR(25) NOT NULL,
                youtube_ID VARCHAR(15) NOT NULL,
                hash_time FLOAT NOT NULL,
                hash_value VARCHAR(50) NOT NULL
            )
        """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_hash_value ON Songs (hash_value)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_hash_time ON Songs (hash_time)")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS SongMetadata (
                spotify_ID VARCHAR(25) PRIMARY KEY,
                youtube_ID VARCHAR(15),
                title TEXT,
                artists TEXT,
                cover TEXT,
                album_name TEXT,
                release_date TEXT,
                duration_ms INTEGER
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spotify_ID VARCHAR(25) NOT NULL,
                user_ip TEXT,
                is_correct INTEGER NOT NULL,
                audio_path TEXT,
                created_at FLOAT NOT NULL
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS RateLimits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_ip TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                timestamp FLOAT NOT NULL
            )
        """
        )
        conn.commit()

        total_songs = 0
        total_metadata = 0

        for source_path in source_paths:
            if not os.path.exists(source_path):
                print(f"[DB MERGE] Source not found: {source_path}")
                continue

            source_conn: Optional[sqlite3.Connection] = None
            try:
                source_conn = sqlite3.connect(source_path)
                source_cursor = source_conn.cursor()

                source_cursor.execute("SELECT spotify_ID, youtube_ID, hash_time, hash_value FROM Songs")
                rows = source_cursor.fetchall()
                if rows:
                    cursor.executemany(
                        "INSERT INTO Songs (spotify_ID, youtube_ID, hash_time, hash_value) VALUES (?, ?, ?, ?)",
                        rows,
                    )
                    total_songs += len(rows)

                source_cursor.execute("SELECT * FROM SongMetadata")
                rows = source_cursor.fetchall()
                if rows:
                    cursor.executemany(
                        "INSERT OR IGNORE INTO SongMetadata (spotify_ID, youtube_ID, title, artists, cover, album_name, release_date, duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        rows,
                    )
                    total_metadata += len(rows)

                conn.commit()
                print(f"[DB MERGE] Merged {os.path.basename(source_path)}: {len(rows)} metadata rows")
            except sqlite3.Error as e:
                print(f"[DB MERGE] Error reading {source_path}: {e}")
            finally:
                if source_conn:
                    source_conn.close()

        print(f"[DB MERGE] Done: {total_songs} fingerprints, {total_metadata} metadata entries -> {merged_name}")
        return True

    except sqlite3.Error as e:
        print(f"[DB MERGE] Failed to create merged db: {e}")
        if os.path.exists(merged_path):
            os.remove(merged_path)
        return False
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    if "--merge" in sys.argv:
        downloaded = download_databases()
        if downloaded:
            merge_databases(downloaded)
    else:
        download_databases()
