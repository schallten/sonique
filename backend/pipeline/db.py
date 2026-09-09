import sqlite3
import time
import os
import json
import urllib.request
from typing import Any, Optional, TypedDict, cast
from engine.spotify_parser import spotify_parser, TrackMetadata

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database")
MAX_DB_SIZE_MB = 50
_databases_checked = False


class SongMetadata(TypedDict):
    spotify_ID: str
    youtube_ID: str
    title: str
    artists: str
    cover: str
    album_name: str
    release_date: str
    duration_ms: int


class SongRow(TypedDict):
    spotify_ID: str
    youtube_ID: str
    hash_time: float
    hash_value: str


class FingerprintDict(TypedDict):
    spotify_ID: str
    youtube_ID: str
    hash_time: float
    hash_value: str


def _sort_key(filename: str) -> int:
    name = os.path.basename(filename)
    name = name.removesuffix(".db")
    try:
        return int(name.removeprefix("db"))
    except ValueError:
        return 0


def ensure_databases() -> None:
    """downloads databases from DB_URLS if the database/ folder is empty.
    runs once on startup, subsequent calls are no-ops."""
    global _databases_checked
    if _databases_checked:
        return
    _databases_checked = True

    os.makedirs(DB_DIR, exist_ok=True)
    existing = [f for f in os.listdir(DB_DIR) if f.endswith(".db")]
    if existing:
        print(f"[DB] Found {len(existing)} existing database(s), skipping download")
        return

    db_urls_raw: str = os.environ.get("DB_URLS", "")
    if not db_urls_raw:
        print("[DB] No databases found and DB_URLS not set, starting with empty db")
        conn = sqlite3.connect(os.path.join(DB_DIR, "db1.db"))
        create_db(conn)
        conn.close()
        return

    try:
        urls: list[str] = json.loads(db_urls_raw)
    except json.JSONDecodeError as e:
        print(f"[DB] Failed to parse DB_URLS: {e}, starting with empty db")
        conn = sqlite3.connect(os.path.join(DB_DIR, "db1.db"))
        create_db(conn)
        conn.close()
        return

    if not urls:
        print("[DB] DB_URLS is empty, starting with empty db")
        conn = sqlite3.connect(os.path.join(DB_DIR, "db1.db"))
        create_db(conn)
        conn.close()
        return

    print(f"[DB] No databases found, downloading {len(urls)} from DB_URLS...")
    downloaded = 0
    for i, url in enumerate(urls, 1):
        filename = f"db{i}.db"
        filepath = os.path.join(DB_DIR, filename)
        try:
            print(f"[DB] Downloading {url} -> {filename}")
            urllib.request.urlretrieve(url, filepath)
            conn: Optional[sqlite3.Connection] = None
            try:
                conn = sqlite3.connect(filepath)
                conn.execute("SELECT 1 FROM sqlite_master")
                downloaded += 1
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"[DB] Verified {filename} ({size_mb:.1f}MB)")
            except Exception as e:
                print(f"[DB] Invalid DB file {filename}, removing: {e}")
                os.remove(filepath)
            finally:
                if conn:
                    conn.close()
        except Exception as e:
            print(f"[DB] Failed to download {url}: {e}")

    if downloaded == 0:
        print("[DB] No databases downloaded, starting with empty db")
        conn = sqlite3.connect(os.path.join(DB_DIR, "db1.db"))
        create_db(conn)
        conn.close()
    else:
        print(f"[DB] Downloaded {downloaded}/{len(urls)} database(s)")


def get_db_files() -> list[str]:
    """returns sorted list of all db files in the database/ folder.
    matches files named db1.db, db2.db, db10.db, etc."""
    os.makedirs(DB_DIR, exist_ok=True)
    files = [f for f in os.listdir(DB_DIR) if f.endswith(".db")]
    files.sort(key=_sort_key)
    return [os.path.join(DB_DIR, f) for f in files]


def get_latest_db() -> str:
    """returns path to the latest db file (creates db1 if none exist)"""
    db_files = get_db_files()
    if not db_files:
        first_db = os.path.join(DB_DIR, "db1.db")
        conn = sqlite3.connect(first_db)
        create_db(conn)
        conn.close()
        return first_db
    return db_files[-1]


def _rotate_db_if_needed() -> str:
    """checks if current db exceeds MAX_DB_SIZE_MB, creates new one if needed.
    returns path to the latest db to write to."""
    db_path = get_latest_db()
    try:
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
    except OSError:
        return db_path

    if size_mb > MAX_DB_SIZE_MB:
        db_files = get_db_files()
        last_num = len(db_files)
        new_db = os.path.join(DB_DIR, f"db{last_num + 1}.db")
        conn = sqlite3.connect(new_db)
        create_db(conn)
        conn.close()
        print(f"[DB] Rotated to new database: db{last_num + 1} ({size_mb:.1f}MB exceeded {MAX_DB_SIZE_MB}MB)")
        return new_db

    return db_path


def create_db(conn: sqlite3.Connection) -> None:
    """create db tables if they dont exist"""
    try:
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
    except sqlite3.Error as e:
        print(f"[DB ERROR] create_db failed: {e}")


def get_connection() -> sqlite3.Connection:
    """returns connection to the latest db for writes"""
    db_path = _rotate_db_if_needed()
    try:
        conn = sqlite3.connect(db_path)
        create_db(conn)
        return conn
    except sqlite3.Error as e:
        print(f"[DB ERROR] Connection failed: {e}")
        raise


def song_exists(track_id: str) -> bool:
    """checks if spotifyID already in any DB"""
    for db_path in get_db_files():
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM Songs WHERE spotify_ID = ?) AS spotify_exists",
                (track_id,),
            )
            result = cursor.fetchone()
            if result and result["spotify_exists"] == 1:
                return True
        except sqlite3.Error as e:
            print(f"[DB ERROR] song_exists failed for {os.path.basename(db_path)}: {e}")
        finally:
            if conn:
                conn.close()
    return False


def save_fingerprints_batch(fingerprints: list[FingerprintDict]) -> int:
    """save fingerprints to the latest DB, splitting if needed"""
    if not fingerprints:
        return 0

    query = """
        INSERT INTO Songs (spotify_ID, youtube_ID, hash_time, hash_value)
        VALUES (?, ?, ?, ?)
    """
    data = [
        (fp["spotify_ID"], fp["youtube_ID"], fp["hash_time"], fp["hash_value"])
        for fp in fingerprints
    ]

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        print(f"[DB] Inserted {len(data)} fingerprints.")
        return len(data)
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to insert fingerprints: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            conn.close()


def save_song_metadata(spotify_id: str, youtube_id: str, metadata: TrackMetadata) -> None:
    """save song metadata to the latest DB's SongMetadata table"""
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO SongMetadata
            (spotify_ID, youtube_ID, title, artists, cover, album_name, release_date, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                spotify_id,
                youtube_id,
                metadata.get("title", ""),
                metadata.get("artists", ""),
                metadata.get("cover", ""),
                metadata.get("album_name", ""),
                metadata.get("release_date", ""),
                metadata.get("duration_ms", 0),
            ),
        )
        conn.commit()
        print(f"[DB] Saved metadata for {spotify_id}")
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to save metadata: {e}")
    finally:
        if conn:
            conn.close()


def get_song_metadata(spotify_id: str) -> Optional[SongMetadata]:
    """get song metadata by searching each DB one by one"""
    for db_path in get_db_files():
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM SongMetadata WHERE spotify_ID = ? LIMIT 1",
                (spotify_id,),
            )
            row = cursor.fetchone()
            if row:
                return dict(row)  # type: ignore[return-value]
        except sqlite3.Error as e:
            print(f"[DB ERROR] get_song_metadata failed for {os.path.basename(db_path)}: {e}")
        finally:
            if conn:
                conn.close()
    return None


def get_fingerprints_from_db(db_path: str) -> list[FingerprintDict]:
    """returns fingerprints from a single db file"""
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT spotify_ID, hash_value, hash_time FROM Songs")
        results: list[dict[str, Any]] = [dict(row) for row in cursor.fetchall()]
        return results  # type: ignore[return-value]
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to fetch fingerprints from {os.path.basename(db_path)}: {e}")
        return []
    finally:
        if conn:
            conn.close()


def get_dashboard() -> list[SongRow]:
    """returns aggregated dashboard data from all DBs"""
    all_results: list[dict[str, Any]] = []

    for db_path in get_db_files():
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT S.spotify_ID, S.youtube_ID, COUNT(*) AS entry_count,
                       M.title, M.artists, M.cover, M.album_name, M.release_date, M.duration_ms
                FROM Songs S
                LEFT JOIN SongMetadata M ON S.spotify_ID = M.spotify_ID
                GROUP BY S.spotify_ID, S.youtube_ID
                ORDER BY entry_count DESC
            """
            )
            results = [dict(row) for row in cursor.fetchall()]
            all_results.extend(results)
        except sqlite3.Error as e:
            print(f"[DB ERROR] Failed to fetch dashboard data from {os.path.basename(db_path)}: {e}")
        finally:
            if conn:
                conn.close()

    seen: dict[str, dict[str, Any]] = {}
    for result in all_results:
        spotify_id = result["spotify_ID"]
        if spotify_id not in seen:
            seen[spotify_id] = result
        else:
            seen[spotify_id]["entry_count"] += result["entry_count"]

    deduped = list(seen.values())
    deduped.sort(key=lambda x: x["entry_count"], reverse=True)
    return deduped  # type: ignore[return-value]


def get_song(spotify_id: str) -> Optional[SongMetadata]:
    """fetch a single song's details by searching each DB one by one"""
    metadata = get_song_metadata(spotify_id)
    if metadata:
        return metadata

    for db_path in get_db_files():
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT spotify_ID, youtube_ID FROM Songs WHERE spotify_ID = ? LIMIT 1",
                (spotify_id,),
            )
            row = cursor.fetchone()
            if not row:
                continue

            try:
                meta = spotify_parser(row[0])
                save_song_metadata(row[0], row[1], meta)
                return cast(SongMetadata, {
                    "spotify_ID": row[0],
                    "youtube_ID": row[1],
                    "title": meta.get("title", ""),
                    "artists": meta.get("artists", ""),
                    "cover": meta.get("cover", ""),
                    "album_name": meta.get("album_name", ""),
                    "release_date": meta.get("release_date", ""),
                    "duration_ms": meta.get("duration_ms", 0),
                })
            except Exception as e:
                print(f"[ERROR] spotify_parser failed for {row[0]}: {e}")
                return cast(SongMetadata, {
                    "spotify_ID": row[0],
                    "youtube_ID": row[1],
                    "title": "",
                    "artists": "",
                    "cover": "",
                    "album_name": "",
                    "release_date": "",
                    "duration_ms": 0,
                })
        except sqlite3.Error as e:
            print(f"[DB ERROR] get_song failed for {os.path.basename(db_path)}: {e}")
        finally:
            if conn:
                conn.close()

    return None


def save_feedback(spotify_id: str, user_ip: str, is_correct: bool, audio_path: Optional[str] = None) -> None:
    """save user feedback to the latest DB"""
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO Feedback (spotify_ID, user_ip, is_correct, audio_path, created_at)
            VALUES (?, ?, ?, ?, ?)
        """,
            (spotify_id, user_ip, 1 if is_correct else 0, audio_path, time.time()),
        )
        conn.commit()
        print(f"[DB] Saved feedback for {spotify_id}: correct={is_correct}")
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to save feedback: {e}")
    finally:
        if conn:
            conn.close()


def check_rate_limit(
    user_ip: str, endpoint: str, max_requests: int = 10, window_seconds: int = 60
) -> bool:
    """check if user has exceeded rate limit (uses the latest DB)"""
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cutoff = time.time() - window_seconds
        cursor.execute(
            """
            SELECT COUNT(*) as cnt FROM RateLimits
            WHERE user_ip = ? AND endpoint = ? AND timestamp > ?
        """,
            (user_ip, endpoint, cutoff),
        )
        row = cursor.fetchone()
        count = row[0] if row else 0

        if count >= max_requests:
            return False

        cursor.execute(
            "INSERT INTO RateLimits (user_ip, endpoint, timestamp) VALUES (?, ?, ?)",
            (user_ip, endpoint, time.time()),
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR] Rate limit check failed: {e}")
        return True
    finally:
        if conn:
            conn.close()
