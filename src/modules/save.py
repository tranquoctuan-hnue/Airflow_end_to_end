import os
from pathlib import Path
import time
from datetime import datetime, timedelta

from typing import Optional, List
from .logger import default_logger as logger
from .file_ops import create_directory
from .img_io import save_image_cv2
from .file_ops import create_directory, get_filename_without_extension,get_unique_filename
from .config import DATE_FORMAT, get_timestamp


def save_img_crop(cropped_image, image_path: str, output_dir: str) -> Optional[str]:
    """Save cropped image.
    
    Args:
        cropped_image: Cropped image (OpenCV format)
        image_path: Original image path
        output_dir: Output directory for cropped images
        
    Returns:
        Path to saved cropped image or None if failed
    """
    try:
        create_directory(output_dir)
        base_name = get_filename_without_extension(image_path)
        crop_filename = f"{base_name}_crop"
        crop_filename = get_unique_filename(output_dir, crop_filename)

        crop_path = os.path.join(output_dir, crop_filename)
        return save_image_cv2(cropped_image, crop_path)
    except Exception as e:
        logger.error(f"Error saving cropped image: {e}")
        return None

def save_frame(frame, out_dir: str, cam_id: str) -> Optional[str]:
    """Save frame to file with date-based folder and timestamp.
    
    Args:
        frame: OpenCV frame (BGR)
        out_dir: Output directory
        cam_id: Camera ID
        
    Returns:
        Path to saved frame or None if failed
    """
    # base_path.mkdir(parents=True, exist_ok=True)
    try:
        # date_folder = DATE_FORMAT
        cap_dir = Path(out_dir) / "data_hetmet" / "images"
        cap_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"{cam_id}_{get_timestamp()}"
        unique_path = get_unique_filename(str(cap_dir), base_name, ".jpg")
        return save_image_cv2(frame, unique_path)
    except Exception as e:
        logger.error(f"Error saving frame: {e}")
        return None

def save_labels(boxes: List[tuple], output_path: str) -> Optional[str]:
    """Save YOLO format labels to file.
    
    Args:
        boxes: List of (cls, x_center, y_center, width, height) tuples
        output_path: Path to save labels file
        
    Returns:
        Path to saved labels file or None if failed
    """
    try:
        
        create_directory(os.path.dirname(output_path))
        with open(output_path, 'w') as f:
            for box in boxes:
                cls, x_center, y_center, width, height = box
                f.write(f"{cls} {x_center} {y_center} {width} {height}\n")
        return output_path
    except Exception as e:
        logger.error(f"Error saving labels: {e}")
        return None
  