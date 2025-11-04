import sqlite3
from engine.spotify_parser import spotify_parser

DB_PATH = "sonique.db"


def create_db(conn: sqlite3.Connection):
    """create db if it does not exist"""
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


def get_dashboard() -> list[dict]:
    """returns list of unique songs in DB"""
    query = """
    SELECT spotify_ID, youtube_ID, COUNT(*) AS entry_count
    FROM Songs
    GROUP BY spotify_ID, youtube_ID
    ORDER BY entry_count DESC
    """

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
        print(f"[DB ERROR] Failed to fetch dashboard data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def get_song(spotify_id: str) -> tuple[str, str, str, str, str] | None:
    """fetch a single song from DB and find its details\n
    **PARAMS:** spotify_id (str)\n
    **RETURN:** tuple (spotify_ID, youtube_ID) or None if not found
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT spotify_ID, youtube_ID FROM Songs WHERE spotify_ID = ? LIMIT 1",
            (spotify_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        metadata = {}
        try:
            metadata = spotify_parser(row[0])
        except Exception as e:
            print(f"[ERROR] spotify_parser failed for {row[0]}: {e}")

        return (
            row[0],
            row[1],
            metadata.get("title", ""),
            metadata.get("artists", ""),
            metadata.get("cover", ""),
            metadata.get("album_name", ""),
            metadata.get("release_date", ""),
            metadata.get("duration_ms", ""),
        )

    except sqlite3.Error as e:
        print(f"[DB ERROR] get_song failed: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
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
