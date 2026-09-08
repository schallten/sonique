import os, base64, time, requests
from dotenv import load_dotenv
from yt_dlp import YoutubeDL
from typing import TypedDict, Optional

load_dotenv()
CLIENT_ID: Optional[str] = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET: Optional[str] = os.getenv("SPOTIFY_CLIENT_SECRET")

# simple token cache so we dont request a new one every call
_cached_token: Optional[str] = None
_token_expiry: float = 0


class TrackMetadata(TypedDict):
    title: str
    artists: str
    album_name: Optional[str]
    cover: Optional[str]
    release_date: Optional[str]
    duration_ms: Optional[int]


def _get_token() -> str:
    """get a spotify access token, reuses cached one if still valid"""
    global _cached_token, _token_expiry

    if _cached_token and time.time() < _token_expiry:
        return _cached_token or ""

    token_resp = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={
            "Authorization": f"Basic {base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()}"
        },
        data={"grant_type": "client_credentials"},
        timeout=10,
    )

    if token_resp.status_code != 200:
        raise Exception("Failed to get Spotify access token")

    token: str = token_resp.json().get("access_token")
    if not token:
        raise Exception("Spotify access token missing")

    # spotify tokens last 3600 seconds, refresh a bit early to be safe
    _cached_token = token
    _token_expiry = time.time() + 3500

    return token


def _yt_dlp_metadata(search_query: str) -> TrackMetadata:
    """fetch metadata from YouTube using yt-dlp when Spotify keys are unavailable"""
    ydl_opts = {
        "format": "bestaudio/best",
        "noplaylist": True,
        "quiet": True,
        "extract_flat": False,
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytsearch1:{search_query}", download=False)
        entry = info["entries"][0] if "entries" in info else info

        title = entry.get("title", "")
        artists = entry.get("uploader") or entry.get("channel") or ""
        cover_url: Optional[str] = None
        thumbnails = entry.get("thumbnails", [])
        if thumbnails:
            cover_url = thumbnails[-1].get("url")

        return TrackMetadata(
            title=title,
            artists=artists,
            album_name=None,
            cover=cover_url,
            release_date=None,
            duration_ms=None,
        )
    except Exception:
        # fallback minimal metadata
        return TrackMetadata(title=search_query, artists="", album_name=None, cover=None, release_date=None, duration_ms=None)


def spotify_parser(track_id: str) -> TrackMetadata:
    """returns metadata for a song
    **PARAMS:** track_id (spotify songID) OR search query (if Spotify keys missing)
    **RETURN:** {title, artists, album_name, cover (link), release_date, duration_ms}"""
    # if Spotify keys not set, fall back to yt-dlp
    if not CLIENT_ID or not CLIENT_SECRET:
        # if it looks like a Spotify ID (22 chars alphanumeric), try extracting info
        if len(track_id) == 22 and track_id.isalnum():
            search_query = f"track:{track_id}"
        else:
            search_query = track_id
        return _yt_dlp_metadata(search_query)

    token = _get_token()

    resp = requests.get(
        f"https://api.spotify.com/v1/tracks/{track_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )

    if resp.status_code != 200:
        raise ValueError(f"Spotify track ID {track_id} is invalid or not found")

    data = resp.json()

    album = data.get("album", {})
    images = album.get("images", [])
    cover_url: Optional[str] = images[0]["url"] if images else None

    return TrackMetadata(
        title=data.get("name"),
        artists=", ".join(a.get("name", "") for a in data.get("artists", [])),
        album_name=album.get("name"),
        cover=cover_url,
        release_date=album.get("release_date"),
        duration_ms=data.get("duration_ms"),
    )


def extract_spotify_ids(item_id: str, item_type: str) -> list[str]:
    """extracts all track IDs from a Spotify album/playlist
    **PARAMS:** item_id (spotify album/playlist ID), item_type ("album" / "playlist")
    **RETURN:**list of track IDs
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        # if no Spotify keys, treat item_id as a single track ID
        return [item_id]

    token = _get_token()
    headers = {"Authorization": f"Bearer {token}"}

    track_ids: list[str] = []

    if item_type == "album":
        resp = requests.get(
            f"https://api.spotify.com/v1/albums/{item_id}/tracks",
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            raise ValueError(f"Album ID {item_id} is invalid or not found")
        tracks = resp.json().get("items", [])
        track_ids = [t["id"] for t in tracks]

    elif item_type == "playlist":
        url = f"https://api.spotify.com/v1/playlists/{item_id}/tracks"
        while url:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                raise ValueError(f"Playlist ID {item_id} is invalid or not found")
            data = resp.json()
            track_ids.extend(
                [
                    item["track"]["id"]
                    for item in data.get("items", [])
                    if item.get("track")
                ]
            )
            url = data.get("next")

    else:
        raise ValueError('item_type must be either "album" or "playlist"')

    return track_ids
