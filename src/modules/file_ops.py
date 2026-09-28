"""
File operations utilities for the pipeline.
"""

import os
import shutil
from typing import List, Optional
from .logger import default_logger as logger
from .validate import validate_path

def get_unique_filename(directory: str, base_name: str, extension: str = ".jpg") -> str:
    """
    Tạo tên file không bị trùng trong directory.
    
    Args:
        directory: Thư mục chứa file
        base_name: Tên gốc của file, không gồm đuôi
        extension: Phần mở rộng, mặc định .jpg
        
    Returns:
        Đường dẫn đầy đủ tới file chưa tồn tại
    """
    os.makedirs(directory, exist_ok=True)

    filename = f"{base_name}{extension}"
    counter = 1

    while os.path.exists(os.path.join(directory, filename)):
        filename = f"{base_name}_{counter}{extension}"
        counter += 1

    return os.path.join(directory, filename)


def create_directory(directory: str) -> bool:
    """Create directory if it doesn't exist."""
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {e}")
        return False

def move_file(source: str, destination: str) -> bool:
    """Move file from source to destination."""
    try:
        if not validate_path(source):
            return False
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(source, destination)
        return True
    except Exception as e:
        logger.error(f"Error moving file from {source} to {destination}: {e}")
        return False

def copy_file(source: str, destination: str) -> bool:
    """Copy file from source to destination."""
    try:
        if not validate_path(source):
            return False
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(source, destination)
        return True
    except Exception as e:
        logger.error(f"Error copying file from {source} to {destination}: {e}")
        return False

def list_files(directory: str, extensions: List[str] = None) -> List[str]:
    """List all files in directory, optionally filtered by extensions."""
    if not validate_path(directory):
        return []
    
    try:
        files = []
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                if extensions is None:
                    files.append(filepath)
                elif any(filename.lower().endswith(ext) for ext in extensions):
                    files.append(filepath)
        return files
    except Exception as e:
        logger.error(f"Error listing files in {directory}: {e}")
        return []

def get_filename_without_extension(filepath: str) -> str:
    """Get filename without extension."""
    return os.path.splitext(os.path.basename(filepath))[0]

def get_file_extension(filepath: str) -> str:
    """Get file extension."""
    return os.path.splitext(filepath)[1]