"""
Validation utilities for the pipeline.
"""

import os
from .logger import default_logger as logger

def validate_path(path: str) -> bool:
    """Validate if path exists."""
    if not os.path.exists(path):
        logger.error(f"Path does not exist: {path}")
        return False
    return True

def validate_image_path(image_path: str) -> bool:
    """Validate if image path exists and is a valid image."""
    if not validate_path(image_path):
        return False
    valid_extensions = ['.jpg', '.png', '.jpeg', '.bmp', '.tiff']
    if not any(image_path.lower().endswith(ext) for ext in valid_extensions):
        logger.error(f"Invalid image extension: {image_path}")
        return False
    return True

def validate_directory(path: str) -> bool:
    """Validate if path is a directory."""
    if not validate_path(path):
        return False
    if not os.path.isdir(path):
        logger.error(f"Path is not a directory: {path}")
        return False
    return True

def validate_file_exists(file_path: str) -> bool:
    """Validate if file exists."""
    if not validate_path(file_path):
        return False
    if not os.path.isfile(file_path):
        logger.error(f"Path is not a file: {file_path}")
        return False
    return True

def validate_model_path(model_path: str) -> bool:
    """Validate if model path exists and is .pt file."""
    if not validate_file_exists(model_path):
        return False
    if not model_path.endswith('.pt'):
        logger.error(f"Invalid model file: {model_path}. Must be .pt format")
        return False
    return True

def validate_label_path(label_path: str) -> bool:
    """Validate if label path exists and is .txt file."""
    if not validate_file_exists(label_path):
        return False
    if not label_path.endswith('.txt'):
        logger.error(f"Invalid label file: {label_path}. Must be .txt format")
        return False
    return True