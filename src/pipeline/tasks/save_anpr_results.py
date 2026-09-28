import os
import cv2
import json
import numpy as np
import math
from typing import List, Optional, Dict, Any, Tuple
from src.modules.logger import default_logger as logger
from src.modules.file_ops import create_directory, get_unique_filename
from src.modules.image_utils import ImageUtils
from src.modules.config import DATE_FORMAT, get_timestamp
from src.modules.img_io import save_image_cv2
from pathlib import Path

def scale_box_coordinates(
    box_points: List[List[float]],
    original_size: Tuple[int, int],
    resized_size: Tuple[int, int] = (96, 64)
) -> List[List[float]]:
    """
    Scale bounding box coordinates from resized image (96x64) back to original image size.
    Uses ceil for fractional parts >= 0.2, int otherwise.
    
    Args:
        box_points: List of [x, y] coordinates from resized image
        original_size: Tuple of (width, height) of original image
        resized_size: Tuple of (width, height) of resized image, default (96, 64)
        
    Returns:
        List of scaled [x, y] coordinates for original image
    """
    orig_w, orig_h = original_size
    resized_w, resized_h = resized_size
    
    scale_x = float(orig_w) / float(resized_w)
    scale_y = float(orig_h) / float(resized_h)
    
    scaled_points = []
    for point in box_points:
        scaled_x_val = point[0] * scale_x
        scaled_y_val = point[1] * scale_y
        
        # Use ceil if fractional part >= 0.2, else use int
        scaled_x = math.ceil(scaled_x_val) if (scaled_x_val % 1) >= 0.2 else int(scaled_x_val)
        scaled_y = math.ceil(scaled_y_val) if (scaled_y_val % 1) >= 0.2 else int(scaled_y_val)
        
        scaled_points.append([scaled_x, scaled_y])
    
    return scaled_points


def save_anpr_results(
    img : np.ndarray,
    ocr_result: Optional[List],
    output_dir: str,
    cam_id: str,
) -> Optional[Dict[str, str]]:
    """
    Save ANPR (Automatic Number Plate Recognition) results including:
    - Detection labels (YYYYMMDD_det.txt) with bounding boxes
    - Recognition labels (YYYYMMDD_rec.txt) with text and confidence
    - Cropped images for each detected text region
    
    Args:
        img: Original OpenCV image (before resizing to 96x64)
        ocr_result: OCR detection result from PaddleOCR with structure:
                   [[[box_points], (text, confidence)], ...]
                   Note: box_points are coordinates from 96x64 resized image
        output_dir: Base output directory
        cam_id: Camera ID
        base_name: Optional base filename (e.g., cam_2_20260130_152154_crop). If None, generated from timestamp
        
    Returns:
        Dictionary with paths to saved files, or None if error
    """
    if ocr_result is None or img is None:
        logger.warning(f"Invalid OCR result or image for {cam_id}")
        return None
    
    try:
        # Create output subdirectories
        det_dir = os.path.join(output_dir, 'det')
        rec_dir = os.path.join(output_dir, 'rec')
        
        for dir_path in [det_dir, rec_dir]:
            create_directory(dir_path)
        
        # Generate base filename with timestamp if not provided
        
        base_name = f"{cam_id}_{get_timestamp()}_crop"
                
        # Original image size (this is the ảnh gốc)
        original_h, original_w = img.shape[:2]
        
        # Resized image size used for OCR (96x64)
        resized_w, resized_h = 96, 64
        
        # Process OCR results
        det_labels = []
        rec_labels_list = []
        rec_counter = 0
        
        # Save resized image (96x64) to det folder
        try:
            # det_image_name = f"{base_name}.jpg"
            # det_image_name = get_unique_filename(det_dir, base_name)
            det_image_path = get_unique_filename(det_dir, base_name, ".jpg")
            # det_image_path = os.path.join(det_dir, det_image_name)
            if cv2.imwrite(det_image_path, img):
                logger.info(f"Saved resized 96x64 image to det: {det_image_path}")
            else:
                logger.warning(f"Failed to write resized image: {det_image_path}")
        except Exception as e:
            logger.warning(f"Error saving resized image: {str(e)}")

        for detection in ocr_result:
            if isinstance(detection, list) and len(detection) > 0:
                for det_box, (rec_text, confidence) in detection:
                    # Round points for det_labels
                    pts = [[round(float(p[0]), 1), round(float(p[1]), 1)] for p in det_box]
                    det_labels.append({
                        "transcription": rec_text,
                        "points": pts
                    })
                    
                    # Calculate scaled box for cropping
                    scale_y = float(original_h) / float(resized_h)
                    scale_x = float(original_w) / float(resized_w)

                    scaled_box = [
                        [
                            math.ceil(point[0] * scale_x) if (point[0] * scale_x) % 1 >= 0.2 else int(
                                point[0] * scale_x),
                            math.ceil(point[1] * scale_y) if (point[1] * scale_y) % 1 >= 0.2 else int(
                                point[1] * scale_y)
                        ]
                        for point in det_box
                    ]

                    # Crop and save image
                    try:
                        logger.info(f"Cropping with scaled_box: {scaled_box}")
                        crop_result = ImageUtils.get_rotate_crop_image(img, np.array(scaled_box, dtype=np.float32))
                        
                        # get_rotate_crop_image returns a list [cropped_img], extract the image
                        if crop_result is None or not isinstance(crop_result, list) or len(crop_result) == 0:
                            logger.warning(f"Failed to crop image for text '{rec_text}': get_rotate_crop_image returned invalid result")
                            continue
                        
                        image_dec = crop_result[0]  # Extract ndarray from list
                        
                        if not isinstance(image_dec, np.ndarray):
                            logger.warning(f"Failed to crop image for text '{rec_text}': result item is not ndarray, got {type(image_dec)}")
                            continue
                        
                        logger.info(f"Cropped image shape: {image_dec.shape}, dtype: {image_dec.dtype}")
                        
                        rec_counter += 1
                        name = Path(det_image_name).stem
                        rec_image_name = f"{name}_{rec_counter:02d}.jpg"
                        rec_image_path = os.path.join(rec_dir, rec_image_name)
                        
                        # Ensure image data is uint8
                        if image_dec.dtype != np.uint8:
                            image_dec = (image_dec * 255).astype(np.uint8)
                            logger.info(f"Converted image dtype to uint8")
                        
                        # Ensure image is numpy array and writable
                        if not cv2.imwrite(rec_image_path, image_dec):
                            logger.warning(f"Failed to write image: {rec_image_path}")
                            rec_counter -= 1  # Rollback counter if write failed
                            continue
                        
                        logger.info(f"Saved crop image: {rec_image_path}")
                        
                        # Add to rec_labels: filename text confidence
                        confidence_str = f"{confidence:.4f}"
                        rec_labels_list.append({
                            'text': rec_text,
                            'confidence': confidence_str
                        })
                        
                    except Exception as crop_err:
                        logger.warning(f"Error cropping/saving image for text '{rec_text}': {str(crop_err)}")
                        import traceback
                        traceback.print_exc()
                        continue
        # Save detection label file (YYYYMMDD_det.txt)
        det_label_filename = f"{DATE_FORMAT}_det.txt"
        det_label_path = os.path.join(det_dir, det_label_filename)
        mode = 'a' if os.path.exists(det_label_path) else 'w'
        with open(det_label_path, mode, encoding='utf-8') as f:
            detection_json = json.dumps(det_labels, ensure_ascii=False)
            f.write(f"{det_image_name} {detection_json}\n")
        logger.info(f"Saved detection labels: {det_label_path}")


        # Save recognition label file (YYYYMMDD_rec.txt)
        rec_label_filename = f"{DATE_FORMAT}_rec.txt"
        rec_label_path = os.path.join(rec_dir, rec_label_filename)
        mode = 'a' if os.path.exists(rec_label_path) else 'w'

        with open(rec_label_path, mode, encoding='utf-8') as f:
            for item in rec_labels_list:
                text = item['text']
                confidence = item['confidence']
                f.write(f"{rec_image_path} {text} {confidence}\n")
        logger.info(f"Saved recognition labels: {rec_label_path}")
        
        return {
            "det_label_path": det_label_path,
            "rec_label_path": rec_label_path,
            "rec_count": rec_counter
        }
    except Exception as e:
        logger.error(f"Error saving ANPR results for {cam_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None



