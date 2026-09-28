import os
import cv2
from typing import List, Tuple

class BboxUnit:
    def __init__(self):
        pass
    def compute_iou(self, box1: Tuple[int, int, int, int], box2: Tuple[int, int, int, int]) -> float:
        """Compute IoU between two bounding boxes."""
        x1_1, y1_1, x2_1, y2_1 = box1
        x1_2, y1_2, x2_2, y2_2 = box2
        
        # Intersection
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

    def nms(self, boxes: List[Tuple[int, int, int, int]], iou_threshold: float = 0.7) -> List[int]:
        """Non-Maximum Suppression."""
        if len(boxes) == 0:
            return []
        
        indices = list(range(len(boxes)))
        
        keep = []
        while indices:
            current = indices[0]
            keep.append(current)
            remaining = []
            for other in indices[1:]:
                if self.compute_iou(boxes[current], boxes[other]) <= iou_threshold:
                    remaining.append(other)
            indices = remaining
        
        return keep
    
    