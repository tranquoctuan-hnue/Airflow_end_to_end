"""
Task for predicting person attributes using PaddleClas
"""

import os
import cv2
import numpy as np
from typing import Optional, Dict, Any, List
from pathlib import Path
from paddleclas import PaddleClas

from src.modules.logger import default_logger as logger
from src.modules.file_ops import create_directory, get_unique_filename
from src.modules.config import DATE_FORMAT, get_timestamp


def predict_person_attributes(
    img_crop: np.ndarray,
    attr_config: Dict[str, Any],
    output_dir: str,
    cam_id: str
) -> Optional[Dict[str, Any]]:
    """
    Predict person attributes from cropped image using PaddleClas.
    
    Args:
        img_crop: Cropped image as numpy array (BGR format)
        attr_config: Dictionary containing PaddleClas configuration parameters
        class_id_map_file: Path to class_id_map.txt file
        output_dir: Output directory to save results
        cam_id: Camera identifier (e.g., 'cam_1', 'cam_2')
        
    Returns:
        Dictionary containing prediction results, or None if failed
        
    Example output:
        {
            'class_ids': [7, 0, 9, 5, 21],
            'scores': [0.84433, 0.80821, 0.78387, 0.62333, 0.43693],
            'label_names': ['LowerLong', 'Female', 'Pants', 'UpperLong', 'LowerBlack'],
            'filename': crop_filename,
        }
    """
    try:
        # Create output directory
        attr_dir = os.path.join(output_dir,DATE_FORMAT )
        create_directory(attr_dir)
        
        base_name = f"{cam_id}_{get_timestamp()}_crop"
        image_path = get_unique_filename(attr_dir, base_name, ".jpg")
        
        # Convert from numpy array to uint8 if needed
        if img_crop.dtype != np.uint8:
            if img_crop.max() <= 1.0:
                img_crop = (img_crop * 255).astype(np.uint8)
            else:
                img_crop = img_crop.astype(np.uint8)
        
        success = cv2.imwrite(image_path, img_crop)
        if not success:
            logger.error(f"Failed to save image: {image_path}")
            return None
        
        logger.info(f"Saved crop image: {image_path}")
        
        # Initialize PaddleClas model
        logger.info("Initializing PaddleClas model...")
        threshold = attr_config.get('conf_threshold', 0.8)
        model = PaddleClas(
            inference_model_dir = attr_config.get('inference_model_dir'),
            class_id_map_file = attr_config.get('class_id_map_file'),
            use_gpu =  attr_config.get('use_gpu', True),
            topk = attr_config.get('topk', 15),
        )
               
        result_gen = model.predict(image_path)
        
        # Process results
        for result in result_gen:
            if isinstance(result, list) and len(result) > 0:
                result = result[0]  # Take first result if list is returned
                class_ids = result.get('class_ids', [])
                scores = result.get('scores', [])
                label_names = result.get('label_names', [])
            
                if not label_names:
                    logger.warning("No labels found in prediction result")
                    return None
            
            # Save to label file
            label_filename = f"{DATE_FORMAT}_attr.txt"
            label_path = os.path.join(attr_dir, label_filename)
            
            # Format: filename label1 score1 label2 score2 ...
            label_line = crop_filename
            for label_name, score in zip(label_names, scores):
                if score > threshold:  # Only include labels with confidence >= threshold
                    label_line += f" {label_name} {score}"
            
            # Append to label file
            try:
                with open(label_path, 'a') as f:
                    f.write(label_line + '\n')
                logger.info(f"Saved labels to: {label_path}")
                logger.info(f"Label content: {label_line}")
            except Exception as e:
                logger.error(f"Error writing to label file {label_path}: {e}")
                return None
            
            # Prepare result dictionary
            result_dict = {
                'class_ids': class_ids,
                'scores': scores,
                'label_names': label_names,
                'filename': crop_filename,
                'image_path': image_path,
                'label_path': label_path,
                'cam_id': cam_id,
            }
            
            return result_dict
        
        logger.warning("No results generated from model prediction")
        return None
        
    except Exception as e:
        logger.error(f"Error in predict_person_attributes: {e}")
        import traceback
        traceback.print_exc()
        return None
