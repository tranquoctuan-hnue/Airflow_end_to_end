"""
Image saving utilities for the pipeline.
"""

import os
import cv2
from PIL import Image
from typing import Optional
from .logger import default_logger as logger

def save_image_cv2(image, output_path: str) -> Optional[str]:
    """Save image using OpenCV."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        cv2.imwrite(output_path, image)
        return output_path
    except Exception as e:
        logger.error(f"Error saving image with cv2: {e}")
        return None

def save_image_pil(image, output_path: str, format: str = 'JPEG') -> Optional[str]:
    """Save image using PIL."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        image.save(output_path, format.upper())
        return output_path
    except Exception as e:
        logger.error(f"Error saving image with PIL: {e}")
        return None

def convert_bgr_to_rgb_and_save(frame, output_path: str) -> Optional[str]:
    """Convert OpenCV BGR frame to RGB and save using PIL."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        pil_image.save(output_path)
        return output_path
    except Exception as e:
        logger.error(f"Error converting and saving image: {e}")
        return None