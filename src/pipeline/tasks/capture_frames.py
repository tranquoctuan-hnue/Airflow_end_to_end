import time
from typing import Optional
from src.modules.video_capture import VideoCapture
from src.modules.config import INITIAL_SLEEP, MAX_SLEEP
from src.modules.logger import default_logger as logger
from src.modules.rtsp_credentials import redact

def capture_from_rtsp(rtsp_url: str) -> Optional:
    """
    Capture a frame from a single RTSP/YouTube URL with exponential backoff retry.
    
    Retry logic: starts at 10min, doubles on failure, resets at 12h.
    
    Args:
        rtsp_url: RTSP or YouTube URL
        
    Returns:
        Captured frame (OpenCV format) or None if failed
    """
    vc = VideoCapture()
    resolved_url = rtsp_url
  
    # Try to resolve YouTube URLs
    if 'youtube' in rtsp_url.lower() or 'youtu.be' in rtsp_url.lower():
        resolved_url = vc.resolve_youtube_url(rtsp_url)
        if not resolved_url:
            logger.warning(f"Failed to resolve YouTube URL: {rtsp_url}")
            return None
    
    start_time = time.time()
    current_sleep = INITIAL_SLEEP
    
    # Retry loop with exponential backoff
    while True:
        try:
            frame, success = vc.capture_frame(resolved_url)
            if success:
                logger.info(f"Successfully captured frame from {redact(rtsp_url)}")
                return frame
            else:
                # Failed to capture, apply backoff
                elapsed = time.time() - start_time
                if elapsed >= MAX_SLEEP:
                    # Reset after 12 hours
                    current_sleep = INITIAL_SLEEP
                    start_time = time.time()
                    logger.info("Retry timer reset after 12 hours")
                
                logger.warning(f"Retrying in {current_sleep}s...")
                time.sleep(current_sleep)
                current_sleep = min(current_sleep * 2, MAX_SLEEP)
        except Exception as e:
            logger.error(f"Unexpected error for {redact(rtsp_url)}: {str(e)}")
            return None


