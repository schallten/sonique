import sqlite3
import time
from engine.spotify_parser import spotify_parser

DB_PATH = "sonique.db"


def create_db(conn: sqlite3.Connection):
    """create db tables if they dont exist"""
    try:
        cursor = conn.cursor()

        # fingerprints table (stores audio hashes)
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

        # song metadata table (title, artists, cover, etc.)
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

        # feedback table (tracks correct/incorrect matches)
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

        # rate limiting table (prevents spam)
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


def get_connection() -> sqlite3.Connection | None:
    try:
        conn = sqlite3.connect(DB_PATH)
        create_db(conn)
        return conn
    except sqlite3.Error as e:
        print(f"[DB ERROR] Connection failed: {e}")
        return None


def song_exists(track_id: str) -> int:
    """checks if spotifyID already in DB\n
    **PARAMS:** track_id\n
    **RETURN:** boolean"""
    query = "SELECT EXISTS(SELECT 1 FROM Songs WHERE spotify_ID = ?) AS spotify_exists"

    conn = get_connection()
    if not conn:
        print("[DB ERROR] Could not connect to DB for song_exists")
        return 0

    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, (track_id,))
        result = cursor.fetchone()
        return result["spotify_exists"] if result else 0
    except sqlite3.Error as e:
        print(f"[DB ERROR] song_exists failed: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()


def save_fingerprints_batch(fingerprints: list[dict]):
    """save fingerprints to 'Songs' table\n
    **PARAMS:** fingerprints (list of dicts/tuples containing: spotify_ID, youtube_ID, hash_time, hash_value)
    """
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

    conn = get_connection()
    if not conn:
        print("[DB ERROR] Cannot insert fingerprints: no connection")
        return 0

    try:
        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        print(f"[DB] Inserted {len(data)} fingerprints.")
        return len(data)
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to insert fingerprints: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


def save_song_metadata(spotify_id, youtube_id, metadata):
    """save song metadata to SongMetadata table"""
    conn = get_connection()
    if not conn:
        print("[DB ERROR] Cannot save metadata: no connection")
        return

    try:
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
        cursor.close()
        conn.close()


def get_song_metadata(spotify_id: str):
    """get song metadata from SongMetadata table (cached)"""
    conn = get_connection()
    if not conn:
        return None

    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM SongMetadata WHERE spotify_ID = ? LIMIT 1",
            (spotify_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        print(f"[DB ERROR] get_song_metadata failed: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_dashboard() -> list[dict]:
    """returns list of unique songs in DB with metadata"""
    conn = get_connection()
    if not conn:
        return []

    try:
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
        return results
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to fetch dashboard data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_song(spotify_id: str):
    """fetch a single song's details\n
    **PARAMS:** spotify_id (str)\n
    **RETURN:** dict with song details or None
    """
    # first try to get from cached metadata
    metadata = get_song_metadata(spotify_id)
    if metadata:
        return metadata

    # if not cached, get basic info from Songs table
    conn = get_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT spotify_ID, youtube_ID FROM Songs WHERE spotify_ID = ? LIMIT 1",
            (spotify_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        # fetch from spotify and cache it
        try:
            meta = spotify_parser(row[0])
            save_song_metadata(row[0], row[1], meta)
            return {
                "spotify_ID": row[0],
                "youtube_ID": row[1],
                "title": meta.get("title", ""),
                "artists": meta.get("artists", ""),
                "cover": meta.get("cover", ""),
                "album_name": meta.get("album_name", ""),
                "release_date": meta.get("release_date", ""),
                "duration_ms": meta.get("duration_ms", 0),
            }
        except Exception as e:
            print(f"[ERROR] spotify_parser failed for {row[0]}: {e}")
            return {
                "spotify_ID": row[0],
                "youtube_ID": row[1],
                "title": "",
                "artists": "",
                "cover": "",
                "album_name": "",
                "release_date": "",
                "duration_ms": 0,
            }

    except sqlite3.Error as e:
        print(f"[DB ERROR] get_song failed: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def get_all_fingerprints() -> list[dict]:
    """returns list of all fingerprints in DB"""
    query = "SELECT spotify_ID, hash_value, hash_time FROM Songs"

    conn = get_connection()
    if not conn:
        return []

    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        results = [dict(row) for row in cursor.fetchall()]
        return results
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to fetch fingerprints: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def save_feedback(spotify_id, user_ip, is_correct, audio_path=None):
    """save user feedback (correct/incorrect match)"""
    conn = get_connection()
    if not conn:
        print("[DB ERROR] Cannot save feedback: no connection")
        return

    try:
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
        cursor.close()
        conn.close()


def check_rate_limit(user_ip, endpoint, max_requests=10, window_seconds=60):
    """check if user has exceeded rate limit
    **RETURN:** True if allowed, False if rate limited"""
    conn = get_connection()
    if not conn:
        return True  # let it through if we cant check

    try:
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

        # log this request
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
        cursor.close()
        conn.close()
