import cv2
from PIL import Image
import numpy as np
import os
from typing import Optional
from .logger import default_logger as logger
from .validate import validate_image_path
from .img_io import save_image_cv2, save_image_pil

class ImageUtils:
    def __init__(self):
        pass
    
    def get_rotate_crop_image(img, points):
        points = np.array(points, dtype=np.float32)
        text_regions = []
        assert len(points) == 4, "shape of points must be 4*2"
        img_crop_width = int(
            max(
                np.linalg.norm(points[0] - points[1]), np.linalg.norm(points[2] - points[3])
            )
        )
        img_crop_height = int(
            max(
                np.linalg.norm(points[0] - points[3]), np.linalg.norm(points[1] - points[2])
            )
        )
        pts_std = np.float32(
            [
                [0, 0],
                [img_crop_width, 0],
                [img_crop_width, img_crop_height],
                [0, img_crop_height],
            ]
        )
        M = cv2.getPerspectiveTransform(points, pts_std)
        dst_img = cv2.warpPerspective(
            img,
            M,
            (img_crop_width, img_crop_height),
            borderMode=cv2.BORDER_REPLICATE,
            flags=cv2.INTER_CUBIC,
        )
        dst_img_height, dst_img_width = dst_img.shape[0:2]
        if dst_img_height * 1.0 / dst_img_width >= 1.5:
            dst_img = np.rot90(dst_img)
        text_regions.append(dst_img)
        return text_regions

    def resize_image(self, image_path: str, output_path: str, width: int, height: int) -> Optional[str]:
        """Resize image to specified dimensions."""
        if not validate_image_path(image_path):
            return None
        
        image = cv2.imread(image_path)
        if image is None:
            logger.error(f"Cannot read image: {image_path}")
            return None
        
        resized = cv2.resize(image, (width, height))
        return save_image_cv2(resized, output_path)
    
    def convert_format(self, image_path: str, output_path: str, format: str = 'PNG') -> Optional[str]:
        """Convert image to different format."""
        if not validate_image_path(image_path):
            return None
        
        try:
            img = Image.open(image_path)
            return save_image_pil(img, output_path, format)
        except Exception as e:
            logger.error(f"Error converting image: {e}")
            return None
    
    def get_image_info(self, image_path: str) -> Optional[dict]:
        """Get image dimensions and other info."""
        if not validate_image_path(image_path):
            return None
        
        image = cv2.imread(image_path)
        if image is None:
            return None
        
        height, width = image.shape[:2]
        return {
            'width': width,
            'height': height,
            'channels': image.shape[2] if len(image.shape) > 2 else 1
        }