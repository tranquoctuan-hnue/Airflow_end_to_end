"""
SAM3-based helmet and attributes detection on cropped vehicle images.
Detects: Helmet, Hat, Hood, Head, smart phone, headphone
"""

import os
import numpy as np
import torch
from typing import Dict, List, Optional, Tuple
from PIL import Image

from src.modules.logger import default_logger as logger


def from_sam(sam_result) -> Dict:
    """Convert SAM3 result to standard detection format (xyxy, confidence).
    
    Args:
        sam_result: SAM3 model output dictionary
        
    Returns:
        Dictionary with 'xyxy' (N, 4) and 'confidence' (N,) arrays
    """
    if sam_result is None:
        return {'xyxy': np.zeros((0, 4)), 'confidence': np.array([])}
    
    boxes = sam_result.get("boxes", None)
    scores = sam_result.get("scores", None)
    
    if boxes is None or scores is None:
        return {'xyxy': np.zeros((0, 4)), 'confidence': np.array([])}

    xyxy = boxes.to(torch.float32).cpu().numpy()
    confidence = scores.to(torch.float32).cpu().numpy()
    
    return {'xyxy': xyxy, 'confidence': confidence}


def filter_by_conf(detections: Dict, threshold: float) -> Dict:
    """Filter detections by confidence threshold.
    
    Args:
        detections: Dictionary with 'xyxy' and 'confidence'
        threshold: Confidence threshold
        
    Returns:
        Filtered detections dictionary
    """
    if len(detections['confidence']) == 0:
        return {'xyxy': np.zeros((0, 4)), 'confidence': np.array([])}
    
    idx = np.where(np.array(detections['confidence']) > threshold)[0]
    if idx.size == 0:
        return {'xyxy': np.zeros((0, 4)), 'confidence': np.array([])}
    
    xyxy = np.asarray(detections['xyxy'])[idx]
    conf = np.asarray(detections['confidence'])[idx]
    
    return {'xyxy': xyxy, 'confidence': conf}


def xyxy_to_xywh_normalized(xyxy: np.ndarray, img_h: int, img_w: int) -> Tuple[float, float, float, float]:
    """Convert from xyxy (x1,y1,x2,y2) to xywh normalized coordinates.
    
    Args:
        xyxy: Array [x1, y1, x2, y2]
        img_h: Image height
        img_w: Image width
        
    Returns:
        Tuple (x_center_norm, y_center_norm, w_norm, h_norm) in range [0, 1]
    """
    x1, y1, x2, y2 = xyxy
    x_center = (x1 + x2) / 2 / img_w
    y_center = (y1 + y2) / 2 / img_h
    w = (x2 - x1) / img_w
    h = (y2 - y1) / img_h
    
    return x_center, y_center, w, h


def iou_boxes(b1: List[float], b2: List[float]) -> float:
    """Calculate IoU between two boxes in xyxy format.
    
    Args:
        b1: Box 1 [x1, y1, x2, y2]
        b2: Box 2 [x1, y1, x2, y2]
        
    Returns:
        IoU value between 0 and 1
    """
    x1_1, y1_1, x2_1, y2_1 = b1
    x1_2, y1_2, x2_2, y2_2 = b2

    x1_i = max(x1_1, x1_2)
    y1_i = max(y1_1, y1_2)
    x2_i = min(x2_1, x2_2)
    y2_i = min(y2_1, y2_2)
    
    if x2_i <= x1_i or y2_i <= y1_i:
        return 0.0
    
    area_i = (x2_i - x1_i) * (y2_i - y1_i)
    area1 = (x2_1 - x1_1) * (y2_1 - y1_1)
    area2 = (x2_2 - x1_2) * (y2_2 - y1_2)
    area_u = area1 + area2 - area_i
    
    return area_i / area_u if area_u > 0 else 0.0

def get_class_priority(class_name: str) -> int:
    """Get priority of class for filtering overlapping detections.
    Higher priority = helmet > hat/hood > head > others
    
    Args:
        class_name: Class name string
        
    Returns:
        Priority value (higher = more important)
    """
    priorities = {
        'smart phone': 3,
        'Helmet': 3,
        'Hat': 3,
        'Hood': 3,
        'Head': 2,
        'goods carried on a motorbike': 1,
        
    }
    return priorities.get(class_name, 0)


def filter_best_detections(
    detections_by_class: Dict[str, List[Tuple[float, float, float, float, float]]],
    iou_threshold: float = 0.5
) -> Dict[str, List[Tuple[float, float, float, float, float]]]:
    """Filter detections to handle overlaps, prioritizing helmet > head > others.
    
    Args:
        detections_by_class: Dict {class_name: [(x, y, w, h, conf), ...]}
        iou_threshold: IoU threshold for considering boxes as overlapping
        
    Returns:
        Filtered detections with best boxes only
    """
    all_detections = []
    
    # Gather all detections with class info
    for class_idx, class_name in enumerate(detections_by_class.keys()):
        detections = detections_by_class.get(class_name, [])
        for x, y, w, h, conf in detections:
            all_detections.append({
                'class_name': class_name,
                'class_idx': class_idx,
                'x': x, 'y': y, 'w': w, 'h': h,
                'conf': conf,
                'box': [x - w/2, y - h/2, x + w/2, y + h/2]  # xyxy
            })
    
    # Sort by confidence descending
    all_detections.sort(key=lambda d: d['conf'], reverse=True)
    
    selected = []
    for det in all_detections:
        is_overlapping = False
        
        for sel in selected:
            iou = iou_boxes(det['box'], sel['box'])
            if iou > iou_threshold:
                is_overlapping = True
                existing_det = sel
                break
        
        if not is_overlapping:
            selected.append(det)
        else:
            # Overlapping - apply priority rules
            current_priority = get_class_priority(det['class_name'])
            existing_priority = get_class_priority(existing_det['class_name'])
            
            if current_priority > existing_priority:
                selected.remove(existing_det)
                selected.append(det)
            elif current_priority == existing_priority and det['conf'] > existing_det['conf']:
                selected.remove(existing_det)
                selected.append(det)
    
    # Group by class
    filtered_detections = {}
    for det in selected:
        class_name = det['class_name']
        if class_name not in filtered_detections:
            filtered_detections[class_name] = []
        filtered_detections[class_name].append((det['x'], det['y'], det['w'], det['h'], det['conf']))
    
    return filtered_detections


def detect_helmet_attributes_sam3(
    crop_image,
    sam3_processor,
    sam3_config: Dict,
    target_classes: Optional[List[str]] = None
) -> Dict[str, List[Tuple[float, float, float, float, float]]]:
    """Run SAM3 detection on cropped vehicle image to detect helmet attributes.
    
    Args:
        crop_image: Image (numpy array or PIL Image)
        sam3_processor: SAM3 processor instance
        sam3_config: SAM3 configuration dictionary
        target_classes: List of class names to detect (uses config default if None)
        
    Returns:
        Dictionary {class_name: [(x, y, w, h, conf), ...]} in normalized coordinates
    """
    if target_classes is None:
        target_classes = sam3_config.get('target_classes', 
            ['Helmet', 'Hat', 'Hood', 'Head', 'smart phone', 'headphone'])
    
    # Convert to PIL image if needed
    # if isinstance(crop_image, str):
    #     img = Image.open(crop_image).convert("RGB")
    # elif isinstance(crop_image, np.ndarray):
    #     if crop_image.dtype != np.uint8:
    #         crop_image = (crop_image * 255).astype(np.uint8)
    #     img = Image.fromarray(crop_image)
    # else:
    #     img = crop_image
    crop_pil = Image.fromarray(crop_image)
    
    if isinstance(crop_pil, str):
        img = Image.open(crop_pil).convert("RGB")
    else:
        img = crop_pil

    img_w, img_h = img.size
    detections_by_class = {}
    
    try:
        # Run SAM3 on image
        inference_state = sam3_processor.set_image(img)
        
        confidence_threshold = sam3_config.get('confidence_threshold', 0.6)
        
        for class_name in target_classes:
            try:
                # Get detections for this class
                sam_res = sam3_processor.set_text_prompt(state=inference_state, prompt=class_name)
                det = from_sam(sam_res)
                
                # Apply class-specific confidence thresholds
                if class_name in ['smart phone', 'goods carried on a motorbike']:
                    class_threshold = 0.5
                else:
                    class_threshold = confidence_threshold
                
                det = filter_by_conf(det, class_threshold)
                
                # Convert boxes from xyxy (absolute) to xywh (normalized)
                detections = []
                for idx in range(len(det['xyxy'])):
                    xyxy = det['xyxy'][idx]
                    conf = float(det['confidence'][idx])
                    x, y, w, h = xyxy_to_xywh_normalized(xyxy, img_h, img_w)
                    detections.append((x, y, w, h, conf))
                
                detections_by_class[class_name] = detections
                
            except Exception as e:
                logger.warning(f"Error detecting class '{class_name}': {e}")
                detections_by_class[class_name] = []
            
            # Clear GPU cache after each class
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.synchronize()
        
    except Exception as e:
        logger.error(f"Error in SAM3 detection: {e}")
        for class_name in target_classes:
            detections_by_class[class_name] = []
    
    # Final GPU cache cleanup
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    return detections_by_class


def detect_helmet_attributes(
    crop_image,
    sam3_processor,
    sam3_config: Dict,
) -> Dict[str, List[Tuple[float, float, float, float, float]]]:
    """Wrapper for helmet detection with filtering.
    
    Args:
        crop_image: Cropped vehicle image
        sam3_processor: SAM3 processor instance
        sam3_config: SAM3 configuration dictionary
        
    Returns:
        Filtered detections by class
    """
    detections_by_class = detect_helmet_attributes_sam3(
        crop_image, 
        sam3_processor, 
        sam3_config
    )
    
    # Filter to get best detections (handle overlaps with priority rules)
    iou_threshold = sam3_config.get('iou_threshold', 0.5)
    filtered_detections = filter_best_detections(detections_by_class, iou_threshold)
    
    return filtered_detections
