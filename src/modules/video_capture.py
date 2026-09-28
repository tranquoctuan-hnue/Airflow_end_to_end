import traceback
import cv2
import time
from typing import Optional
from yt_dlp import YoutubeDL
from .logger import default_logger as logger
from .rtsp_credentials import redact


class VideoCapture:
    """Handles video capture and URL resolution for streams."""
    
    def resolve_youtube_url(self, youtube_url: str) -> Optional[str]:
        """
        Resolve YouTube URL to direct stream URL via yt-dlp.
        
        Args:
            youtube_url: YouTube URL string
            
        Returns:
            Direct stream URL or None if resolution fails
        """
        try:
            ydl_opts = {
                "format": f"best[height<=1280][ext=mp4]/best[height<=1280]",
                'quiet': True,
                'skip_download': True,
            }
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                direct_url = info.get('url')
                if not direct_url:
                    for f in (info.get("formats") or []):
                        if f.get("url"):
                            direct_url = f.get("url")
                if not direct_url:
                    logger.error(f"Could not resolve YouTube URL: {youtube_url}")
                    return None
                if direct_url:
                    return direct_url

               
        except Exception as e:
            logger.error(f"YouTube URL resolution failed: {str(e)}")
            return None
    
    def capture_frame(self, stream_url: str) -> Optional[tuple]:
        """
        Capture a single frame from a stream.
        
        Args:
            stream_url: RTSP or direct video stream URL
            
        Returns:
            Tuple of (frame, success_flag) or (None, False) if capture fails
        """
        cap = None
        try:
            cap = cv2.VideoCapture(stream_url)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # Minimize latency for live streams
            
            ret, frame = cap.read()
            if not ret or frame is None:
                logger.warning(f"Failed to read frame from: {redact(stream_url)}")
                return None, False
            
            logger.info(f"Successfully captured frame from stream")
            return frame, True
        except Exception as e:
            logger.error(f"Frame capture error: {str(e)}\n{traceback.format_exc()}")
            return None, False
        finally:
            if cap:
                cap.release()