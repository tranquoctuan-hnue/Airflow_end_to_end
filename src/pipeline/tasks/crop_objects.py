from typing import List, Union, Tuple, Optional
from src.modules.logger import default_logger as logger
from src.modules.bbox_utils import BboxUnit

def crop_object_cls_xyxy_nms(
    frame,
    label,
    class_id: Union[int, List[int]],
    nms_status: bool = True,
    iou_threshold: float = 0.7,
    min_crop_size: Optional[Union[int, Tuple[int, int]]] = None,
    expand_bbox_tblr: Optional[Union[int, Tuple[int, int, int, int]]] = None
) -> List:
    """Crop objects of specific class(es) from image based on labels, optionally with NMS.
    
    Args:
        frame: Image array (H, W, C)
        label: List of label tuples [cls, x_center, y_center, w, h]
        class_id: Single class ID or list of class IDs to crop
        nms_status: Whether to apply NMS to remove overlapping boxes
        iou_threshold: IoU threshold for NMS
        min_crop_size: Minimum crop size.
            - int: min width and height
            - (min_w, min_h): minimum width and height separately
            - None: no size filter
        expand_bbox_tblr: Expand box by pixels in order (top, bottom, left, right)
            - int: expand all sides equally
            - (t, b, l, r): expand each side separately
            - None: no expansion
    
    Returns:
        List of cropped image arrays
    """

    class_ids = [class_id] if isinstance(class_id, int) else class_id

    height, width = frame.shape[:2]
    cropped_image = []

    labels = []
    for line in label:
        if len(line) == 5:
            cls, x_center, y_center, w, h = map(float, line)
            if int(cls) in class_ids:
                x_center *= width
                y_center *= height
                w *= width
                h *= height

                x1 = int(x_center - w / 2)
                y1 = int(y_center - h / 2)
                x2 = int(x_center + w / 2)
                y2 = int(y_center + h / 2)

                labels.append((x1, y1, x2, y2))

    if not labels:
        return []

    if nms_status:
        keep_indices = BboxUnit().nms(labels, iou_threshold)
    else:
        keep_indices = list(range(len(labels)))

    for idx in keep_indices:
        x1, y1, x2, y2 = labels[idx]

        # Expand bbox theo tọa độ: top, bottom, left, right
        if expand_bbox_tblr is not None:
            if isinstance(expand_bbox_tblr, int):
                t = b = l = r = expand_bbox_tblr
            else:
                t, b, l, r = expand_bbox_tblr

            y1 -= t
            y2 += b
            x1 -= l
            x2 += r

        # Clamp vào biên ảnh
        x1 = max(0, x1)
        y1 = max(0, y1)
        x2 = min(width, x2)
        y2 = min(height, y2)

        crop_w = x2 - x1
        crop_h = y2 - y1

        if crop_w <= 0 or crop_h <= 0:
            continue

        if min_crop_size is not None:
            if isinstance(min_crop_size, int):
                min_w = min_h = min_crop_size
            else:
                min_w, min_h = min_crop_size

            if crop_w < min_w or crop_h < min_h:
                continue

        cropped = frame[y1:y2, x1:x2]
        if cropped.size == 0:
            continue

        cropped_image.append(cropped)

    return cropped_image