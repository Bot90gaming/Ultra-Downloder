import asyncio
import logging
import subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp

# 1. Setup Logging Format
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("ytm_app")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Enable log output in yt-dlp
YTDL_OPTIONS = {
    'format': 'bestaudio/best',
    'noplaylist': True,
    'quiet': False,  # Changed to False to output info to terminal
    'extract_flat': False,
}

@app.get("/search")
def search_ytm(q: str = Query(..., description="Song name or artist")):
    logger.info(f"Searching YTM for: '{q}'")
    ydl_opts = {
        'format': 'bestaudio/best',
        'default_search': 'ytsearch5',
        'quiet': True,
        'extract_flat': True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(f"ytmsearch:{q}", download=False)
            results = []
            if 'entries' in info:
                for entry in info['entries']:
                    results.append({
                        'id': entry.get('id'),
                        'title': entry.get('title'),
                        'uploader': entry.get('uploader'),
                        'duration': entry.get('duration'),
                        'thumbnail': entry.get('thumbnails', [{}])[-1].get('url') if entry.get('thumbnails') else None
                    })
            logger.info(f"Found {len(results)} search results for '{q}'")
            return {"results": results}
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/{video_id}")
async def stream_audio(video_id: str, request: Request):
    logger.info(f"Stream request received for video_id: {video_id}")
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    loop = asyncio.get_event_loop()
    
    def get_stream_url():
        with yt_dlp.YoutubeDL(YTDL_OPTIONS) as ydl:
            logger.info(f"Extracting raw stream URL via yt-dlp for {video_id}...")
            info = ydl.extract_info(url, download=False)
            return info.get('url'), info.get('title')

    try:
        audio_url, title = await loop.run_in_executor(None, get_stream_url)
        logger.info(f"Extracted audio URL successfully for: '{title}'")
    except Exception as e:
        logger.error(f"yt-dlp extraction failed for {video_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to extract audio: {str(e)}")

    if not audio_url:
        logger.error(f"No audio stream found for {video_id}")
        raise HTTPException(status_code=404, detail="Audio stream not found")

    cmd = [
        'ffmpeg',
        '-headers', 'User-Agent: Mozilla/5.0\r\n',
        '-i', audio_url,
        '-f', 'mp3',
        '-acodec', 'libmp3lame',
        '-ab', '128k',
        '-vn',
        'pipe:1'
    ]

    logger.info(f"Spawning FFmpeg process for video_id: {video_id}")

    try:
        # 3. Capture stderr to capture FFmpeg process logs instead of DEVNULL
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except FileNotFoundError:
        logger.error("FFmpeg execution failed: 'ffmpeg' binary not found in PATH.")
        raise HTTPException(
            status_code=500, 
            detail="FFmpeg is not installed or not found in system PATH."
        )

    # Background task to print FFmpeg logs to terminal asynchronously
    async def log_ffmpeg_errors():
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            logger.debug(f"[FFmpeg] {line.decode().strip()}")

    asyncio.create_task(log_ffmpeg_errors())

    async def iter_stream():
        bytes_sent = 0
        try:
            while True:
                chunk = await process.stdout.read(64 * 1024)
                if not chunk:
                    logger.info(f"Finished streaming {video_id}. Total sent: {bytes_sent} bytes")
                    break
                bytes_sent += len(chunk)
                yield chunk
        except Exception as e:
            logger.error(f"Stream interrupted for {video_id}: {e}")
        finally:
            if process.returncode is None:
                logger.info(f"Terminating FFmpeg process for {video_id}")
                process.kill()

    return StreamingResponse(
        iter_stream(),
        media_type="audio/mpeg",
        headers={
            "Accept-Ranges": "none",
            "Content-Type": "audio/mpeg",
        }
    )