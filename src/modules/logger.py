"""
Logging configuration for the video processing pipeline.
"""

import logging
import os
from .config import LOGS_DIR
from .rtsp_credentials import RedactRtspFilter

def setup_logger(name: str, log_file: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Setup a logger with console and file handlers.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        level: Logging level
    
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(RedactRtspFilter())   # không ghi mật khẩu camera ra log
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(RedactRtspFilter())
        logger.addHandler(file_handler)
    
    return logger

# Default logger
default_logger = setup_logger("video_pipeline", os.path.join(LOGS_DIR, "pipeline.log"))