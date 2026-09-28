"""
Core modules for the Airflow video processing pipeline.

Provides modular components following Single Responsibility Principle:
- Validation: path, image, label validation
- File Operations: directory/file manipulation
- Image Saving: various image persistence formats
- Frame/Image/Label Saving: specialized save operations
- Video Capture: stream capture and URL resolution
- Cropping: object extraction with NMS
- Image Utils: image transformation utilities
- File Utils: file filtering and condition checking
- Config: centralized configuration
- Logger: structured logging setup

Note: Detection module moved to src.pipeline.tasks.detect_objects
"""

from .logger import setup_logger, default_logger
from .config import (
    INITIAL_SLEEP, MAX_SLEEP, IOU_THRESHOLD, 
    DEFAULT_CLASS_ID, DEFAULT_MIN_COUNT, PADDLE_OCR_CONFIG
)
from .validate import (
    validate_path, validate_image_path, validate_directory,
    validate_file_exists, validate_model_path, validate_label_path
)
from .file_ops import (
    create_directory, move_file, copy_file, list_files,
    get_filename_without_extension, get_file_extension
)
from .img_io import (
    save_image_cv2, save_image_pil, convert_bgr_to_rgb_and_save
)
from .save import save_frame, save_img_crop, save_labels

__all__ = [
    # Logger
    'setup_logger',
    'default_logger',
    # Config
    'INITIAL_SLEEP',
    'MAX_SLEEP',
    'IOU_THRESHOLD',
    'DEFAULT_CLASS_ID',
    'DEFAULT_MIN_COUNT',
    'PADDLE_OCR_CONFIG',
    # Validation
    'validate_path',
    'validate_image_path',
    'validate_directory',
    'validate_file_exists',
    'validate_model_path',
    'validate_label_path',
    # File Operations
    'create_directory',
    'move_file',
    'copy_file',
    'list_files',
    'get_filename_without_extension',
    'get_file_extension',
    # Image Operations
    'save_image_cv2',
    'save_image_pil',
    'convert_bgr_to_rgb_and_save',
    'save_frame',
    'save_img_crop',
    'save_labels',
]
