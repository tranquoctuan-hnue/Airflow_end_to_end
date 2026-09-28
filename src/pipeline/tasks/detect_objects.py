import os
import cv2
from typing import Dict, Any, List, Union
from ultralytics import YOLO
from typing import List, Optional
from src.modules.logger import default_logger as logger


def detecting_objects_by_yolo(frame, model_config: Dict[str, Any]) -> List[tuple]:
    """Run YOLO detection on a single frame and return labels without saving.
    
    Args:
        frame: OpenCV frame
        model_config: Dictionary containing model path and confidence threshold
    Returns:
        List of (cls, x_center, y_center, width, height) tuples
    """
    if frame is None:
        return []
    
    model = YOLO(model_config.get('model_path'))
    results = model.predict(source=frame, conf=model_config.get('conf_threshold'))
    # Collect normalized boxes from detection results
    boxes = []
    for result in results:
        for box in result.boxes:
            cls = int(box.cls.item())
            x_center = box.xywhn[0][0].item()
            y_center = box.xywhn[0][1].item()
            width = box.xywhn[0][2].item()
            height = box.xywhn[0][3].item()
            boxes.append((cls, x_center, y_center, width, height))
    
    return boxes
