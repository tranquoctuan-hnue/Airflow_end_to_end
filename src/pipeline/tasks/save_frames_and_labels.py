from typing import List, Optional, Tuple, Union
from src.modules.save import save_frame, save_labels
from src.modules.logger import default_logger as logger
import os

ClassCondition = Union[
    List[int],                # OR: any one class id
    Tuple[int, ...],          # AND: all class ids
    List[Tuple[int, ...]]     # OR between AND-groups
]

def save_frames_and_labels(
    frame,
    labels: Optional[List[tuple]],
    output_dir: str,
    cam_id: str,
    class_condition: Optional[ClassCondition] = None
) -> Optional[Tuple[str, Optional[str]]]:
    """
    Rule:
    - None -> save normally
    - tuple (0, 1, 2) -> AND: must contain all class ids
    - list [0, 1, 2] -> OR: must contain at least one class id
    - list of tuples [(0, 1), (1, 2)] -> OR between groups:
        valid if (0 and 1) OR (1 and 2)
    """
    labels = labels or []
    present_class_condition = {int(lb[0]) for lb in labels}

    if class_condition is not None:
        # (0, 1, 2) => AND
        if isinstance(class_condition, tuple):
            required = set(class_condition)
            if not required.issubset(present_class_condition):
                return None

        # [0, 1, 2] => OR
        elif isinstance(class_condition, list):
            if not class_condition:
                return None

            # [(0,1), (1,2)] => OR between AND groups
            if all(isinstance(item, tuple) for item in class_condition):
                # Validate: không cho phép empty tuple
                if any(len(group) == 0 for group in class_condition):
                    raise ValueError(
                        "class_condition list contains an empty tuple, which would always match"
                    )
                matched = any(
                    set(group).issubset(present_class_condition)
                    for group in class_condition
                )
                if not matched:
                    return None

            # [0,1,2] => OR
            elif all(isinstance(item, int) for item in class_condition):
                if not any(cls_id in present_class_condition for cls_id in class_condition):
                    return None
            else:
                raise TypeError(
                    "class_condition list must be either List[int] or List[Tuple[int, ...]]"
                )

        else:
            raise TypeError("class_condition must be either list, tuple, or None")

    frame_path = save_frame(frame, output_dir, cam_id)
    if not frame_path:
        logger.error(f"Failed to save frame for {cam_id}")
        return None

    label_filename = os.path.splitext(os.path.basename(frame_path))[0] + ".txt"
    label_path = os.path.join(
        os.path.dirname(frame_path).replace("images", "labels"),
        label_filename
    )
    os.makedirs(os.path.dirname(label_path), exist_ok=True)
    label_path = save_labels(labels, label_path)

    return frame_path, label_path