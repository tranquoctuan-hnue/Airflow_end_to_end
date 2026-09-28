import math
import os
import re
import shutil
import numpy as np
from paddleocr import PaddleOCR
import cv2
from typing import Dict, Any, Optional, List


def is_license_plate(plate):
    car_patterns = [
        r'^[1-9][0-9][A-Z][0-9]{3,5}$',  # FNCNNNN hoặc FNCNNNNN
        r'^[1-9][0-9][A-Z][1-9][0-9]{3,5}$',  # FNCFNNNN hoặc FNCFNNNNN
        r'^[1-9][0-9][A-Z]{2}[0-9]{3,5}$',  # FNCCNNN hoặc FNCCNNNNN
        r'^[1-9][0-9](NG|QT|CV|NN)[0-9]{5}$',
        r'^[1-9][0-9]{3}(NG|QT|CV|NN)[0-9]{2}$',
        r'^[A-Z]{2-3}[0-9]{4}$',
        r'^[A-Z]{2}[0-9]{3}[A-Z]{2}$',
    ]
    for pattern in car_patterns:
        if re.match(pattern, plate):
            return True
    return False

def ocr_on_image(img , ocr_config: Dict[str, Any]) -> Optional[List]:
    """
    Run OCR on image to detect license plate.
    
    Args:
        img: OpenCV image (original image)
        ocr_config: PaddleOCR configuration dictionary
        
    Returns:
        OCR detection results with bounding boxes from 96x64 resized image
        (coordinates will be scaled back to original size in save_anpr_results)
        Returns None if no valid license plate found
    """
    global rec_text
    
    # Resize image for OCR
    img_rz = cv2.resize(img, (96, 64))
    
    if img_rz is None:
        print("Resized image is None.")
        return None
    
    # Extract PaddleOCR parameters with GPU enabled
    paddle_params = {
        'det_model_dir': ocr_config.get('det_model_dir'),
        'rec_model_dir': ocr_config.get('rec_model_dir'),
        'rec_char_dict_path': ocr_config.get('rec_char_dict_path'),
        'cls_model_dir': ocr_config.get('cls_model_dir'),
        'use_angle_cls': ocr_config.get('use_angle_cls', False),
        'lang': ocr_config.get('lang', 'en'),
        'use_gpu': True,
    }
    # Remove None values
    # paddle_params = {k: v for k, v in paddle_params.items() if v is not None}
    ocr = PaddleOCR(**paddle_params)
    result = ocr.ocr(img_rz)
    
    # Get confidence threshold from config (app-specific parameter)
    conf_threshold = ocr_config.get('conf_threshold_rec', 0.9)
    if result and isinstance(result, list) and len(result) > 0:
        plate_lines = []  
        for detection in result:
            if isinstance(detection, list) and len(detection) > 0:
                for det_box, (rec_text, confidence) in detection:
                    if confidence >= conf_threshold:  
                        plate_lines.append(rec_text.strip())

        full_plate = "".join(plate_lines).replace(" ", "")
        if full_plate is not None:
            if is_license_plate(full_plate):
                print(f"✓ Valid license plate detected: {full_plate}")
                return result
            else:
                print(f"⚠ Text detected but not a valid license plate: {full_plate}")
                return result
        else:
            print("No text detected after filtering by confidence")
            return None
    else:
        print("No result from OCR or result format is invalid.")
    return None