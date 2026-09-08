import os
from yt_dlp import YoutubeDL

output_dir = "downloads"
os.makedirs(output_dir, exist_ok=True)

ydl_opts_template = {
    "format": "bestaudio/best",
    "postprocessors": [
        {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "64"}
    ],
    "noplaylist": True,
    "cachedir": False,
    "quiet": True,
}


def yt_downloader(query: str, filename: str) -> tuple[str, str]:
    """downloads audio from YouTube
    **PARAMS:** query (search query), filename (downloaded file)
    **RETURN:** audio path, youtube id (for DB storage)"""
    opts = ydl_opts_template.copy()
    opts["outtmpl"] = os.path.join(output_dir, f"{filename}.%(ext)s")

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=True)

        # Check if search returned results
        if "entries" in info and info["entries"]:
            info = info["entries"][0]
        elif "id" in info:
            pass  # single result returned directly
        else:
            raise ValueError(f"No YouTube results found for query: '{query}'")

        youtube_id: str = info.get("id")
        if not youtube_id:
            raise ValueError(f"Failed to retrieve YouTube ID for query: '{query}'")

        audio_path: str = os.path.join(output_dir, f"{filename}.mp3")
        return audio_path, youtube_id

    except Exception as e:
        raise ValueError(f"Failed to download YouTube video for query '{query}': {e}")