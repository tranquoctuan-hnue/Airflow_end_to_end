"""
Save helmet and attributes detection results.
Saves crops, labels, and results metadata.
"""

import os
import json
from typing import Dict, List, Optional, Tuple
import numpy as np
from pathlib import Path

from src.modules.logger import default_logger as logger
from src.modules.file_ops import create_directory, get_unique_filename
from src.modules.img_io import save_image_cv2
from src.modules.config import DATE_FORMAT, get_timestamp


def save_helmet_results(
    crop_image: np.ndarray,
    detections_by_class: Dict[str, List[Tuple[float, float, float, float, float]]],
    # frame_path: str,
    output_dir: str,
    cam_id: str,
    crop_idx: int,
) -> Optional[Dict[str, str]]:
    """Save helmet detection results including crops, labels, and metadata.
    
    Args:
        crop_image: Cropped image (numpy array)
        detections_by_class: Dictionary {class_name: [(x, y, w, h, conf), ...]}
        output_dir: Output directory base path
        cam_id: Camera ID
        crop_idx: Index of crop in the frame
        
    Returns:
        Dictionary with saved paths or None if error
    """
    try:
        # Check if crop contains helmet detection, skip if so
        if 'Helmet' in detections_by_class and detections_by_class['Helmet']:
            logger.info("Crop contains helmet detection, skipping save.")
            return None
        
        # Create directory structure: output_dir/helmet/{cam_id}/crops, labels, metadata
        cam_dir = os.path.join(output_dir, DATE_FORMAT)
        crops_dir = os.path.join(cam_dir, 'images')
        labels_dir = os.path.join(cam_dir, 'labels')
        metadata_dir = os.path.join(cam_dir, 'metadata')
        
        create_directory(crops_dir)
        create_directory(labels_dir)
        create_directory(metadata_dir)
        
        # Generate unique filename for crop
        crop_filename = f"{cam_id}_crop_{get_timestamp()}"
        # crop_path = get_unique_filename(crops_dir, crop_filename)
        crop_path = get_unique_filename(crops_dir, crop_filename, ".jpg")
        # crop_path = os.path.join(crops_dir, crop_filename)
        
        # Save crop image
        if not save_image_cv2(crop_image, crop_path):
            logger.error(f"Failed to save crop image: {crop_path}")
            return None
        
        # Save labels file (YOLO format: class_id x y w h)
        crop_basename = os.path.splitext(os.path.basename(crop_path))[0]
        label_filename = f"{crop_basename}.txt"
        label_path = os.path.join(labels_dir, label_filename)
        
        label_lines = []
        class_indices = {
            'Helmet': 0,
            'Hat': 1,
            'Hood': 2,
            'Head': 3,
            'smart phone': 4,
            'goods carried on a motorbike': 5,
        }
        
        # Prepare metadata
        metadata = {
            'crop_path': crop_path,
            'crop_filename': crop_filename,
            'cam_id': cam_id,
            'crop_idx': crop_idx,
            'timestamp': get_timestamp(),
            'detections': []
        }
        
        # Write detection results
        for class_name, detections in detections_by_class.items():
            if not detections:
                continue
            
            class_id = class_indices.get(class_name, -1)
            if class_id < 0:
                logger.warning(f"Unknown class: {class_name}")
                continue
            
            for x, y, w, h, conf in detections:
                # Write to label file: class_id x y w h
                label_line = f"{class_id} {x:.6f} {y:.6f} {w:.6f} {h:.6f}"
                label_lines.append(label_line)
                
                # Add to metadata
                metadata['detections'].append({
                    'class': class_name,
                    'class_id': class_id,
                    'x': float(x),
                    'y': float(y),
                    'w': float(w),
                    'h': float(h),
                    'confidence': float(conf)
                })
        
        # Save label file if there are detections
        if label_lines:
            with open(label_path, 'w') as f:
                for line in label_lines:
                    f.write(line + '\n')
        else:
            # Create empty label file
            open(label_path, 'w').close()
        
        # Save metadata as JSON
        metadata_filename = f"results.json"
        metadata_path = os.path.join(metadata_dir, metadata_filename)
        mode = 'a' if os.path.exists(metadata_path) else 'w'
        with open(metadata_path, mode) as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(
            f"Saved helmet detection results: "
            f"crop={crop_path}, labels={label_path}, metadata={metadata_path} "
            f"({len(metadata['detections'])} detections)"
        )
        
        return {
            'crop_path': crop_path,
            'label_path': label_path,
            'metadata_path': metadata_path,
        }
        
    except Exception as e:
        logger.error(f"Error saving helmet results: {e}")
        import traceback
        traceback.print_exc()
        return None


# def save_helmet_results_batch(
#     frame_crops: List[np.ndarray],
#     all_detections: List[Dict[str, List[Tuple[float, float, float, float, float]]]],
#     frame_path: str,
#     output_dir: str,
#     cam_id: str,
# ) -> List[Optional[Dict[str, str]]]:
#     """Save results for multiple crops from a single frame.
    
#     Args:
#         frame_crops: List of cropped images
#         all_detections: List of detections for each crop
#         frame_path: Path to original frame
#         output_dir: Output directory
#         cam_id: Camera ID
        
#     Returns:
#         List of result dictionaries for each crop
#     """
#     results = []
#     for crop_idx, (crop, detections) in enumerate(zip(frame_crops, all_detections)):
#         result = save_helmet_results(
#             crop, 
#             detections, 
#             frame_path, 
#             output_dir, 
#             cam_id, 
#             crop_idx
#         )
#         results.append(result)
    
#     return results


def append_helmet_results_csv(
    crop_path: str,
    detections_by_class: Dict[str, List[Tuple[float, float, float, float, float]]],
    output_csv: str,
) -> bool:
    """Append helmet detection results to CSV file for batch processing analysis.
    
    Args:
        crop_path: Path to crop image
        detections_by_class: Dictionary of detections
        output_csv: Output CSV file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create CSV header if file doesn't exist
        if not os.path.exists(output_csv):
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            with open(output_csv, 'w') as f:
                f.write("crop_path,class_name,x,y,w,h,confidence\n")
        
        # Append detection results
        with open(output_csv, 'a') as f:
            for class_name, detections in detections_by_class.items():
                for x, y, w, h, conf in detections:
                    line = f"{crop_path},{class_name},{x:.6f},{y:.6f},{w:.6f},{h:.6f},{conf:.4f}\n"
                    f.write(line)
        
        return True
        
    except Exception as e:
        logger.error(f"Error appending to CSV: {e}")
        return False
