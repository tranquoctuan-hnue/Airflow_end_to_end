from typing import List, Optional
from src.modules.logger import default_logger as logger
from src.modules.config import DEFAULT_CLASS_ID, DEFAULT_MIN_COUNT

def check_condition(labels: Optional[List[tuple]], class_id: int = DEFAULT_CLASS_ID, min_count: int = DEFAULT_MIN_COUNT) -> bool:
    """
    Check condition for a label based on detection count.
    
    Args:
        labels: List of (cls, x_center, y_center, width, height) tuples or None
        class_id: Class ID to check for (default 0)
        min_count: Minimum count threshold (default 5)
        
    Returns:
        Boolean status for the label
    """
    if labels is None:
        logger.warning("Labels is None, returning False")
        return False
    
    count_class = sum(1 for box in labels if box[0] == class_id)
    status = count_class >= min_count
    logger.info(f"Label condition: {count_class} detections, status: {status}")
    return status
