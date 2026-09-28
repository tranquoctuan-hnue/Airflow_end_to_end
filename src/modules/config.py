"""
Configuration settings for the video processing pipeline.
"""

import os
from src.modules.time_zone import _now

# Thư mục gốc dự án — mọi đường dẫn tính từ đây để clone sang máy khác vẫn chạy.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODELS_DIR = os.environ.get('VIDEO_PIPELINE_MODELS_DIR') or os.path.join(PROJECT_ROOT, 'models')


def _p(*parts):
    """Đường dẫn trong dự án."""
    return os.path.join(PROJECT_ROOT, *parts)


def _ext(env: str, in_project: str, legacy: str) -> str:
    """
    Model nằm NGOÀI dự án trên máy gốc. Thứ tự: env var → đường dẫn cũ (nếu còn
    tồn tại, máy gốc chạy như trước) → <models>/... (máy mới: đặt model vào đây).
    """
    return os.environ.get(env) or (legacy if os.path.exists(legacy) else
                                   os.path.join(MODELS_DIR, in_project))

# Pipeline run mode: 'sequential' or 'parallel'
RUN_MODE = os.environ.get('VIDEO_PIPELINE_RUN_MODE', 'parallel')


# Video capture settings
DATE_FORMAT = _now().strftime('%Y-%m-%d')

# Get current timestamp - call this function each time to get fresh timestamp
def get_timestamp():
    """Get current timestamp in format YYYY-MM-DD_HH-MM-SS"""
    return _now().strftime('%Y-%m-%d_%H-%M-%S')

# Logs directory
LOGS_DIR = _p('logs')

# Image extensions
IMAGE_EXTENSIONS = ['.jpg', '.png', '.jpeg', '.bmp', '.tiff']

# Retry settings
INITIAL_SLEEP = 600  # 10 minutes
MAX_SLEEP = 43200    # 12 hours

# Default processing parameters
IOU_THRESHOLD = 0.5
DEFAULT_CLASS_ID = 0
DEFAULT_MIN_COUNT = 1


VEHICLE_CONFIG = {
    'model_path': _p('models', 'Yolo_detect', 'Vehicle', 'yolov11n_vh_640_170426', 'weights', 'best.pt'),
    'conf_threshold': 0.7,
    'use_gpu': True,
}
PERSON_CONFIG = {
    'model_path': _p('models', 'Yolo_detect', 'Person', 'yolov11n_human_640_0610252', 'weights', 'best.pt'),
    'conf_threshold': 0.5,
    'use_gpu': True,
}

# PaddleOCR settings
PADDLE_OCR_CONFIG = {
    'det_model_dir': _ext('ANPR_DET_MODEL_DIR', 'ANPR/det/inference', os.path.expanduser('~/Documents/models/OCR/det_best_mv3_large_x0.5')),
    'rec_model_dir': _p('models', 'ANPR', 'rec', 'inference'),
    'rec_char_dict_path': _p('models', 'ANPR', 'dict.txt'),
    'cls_model_dir': _ext('ANPR_CLS_MODEL_DIR', 'ANPR/ch_ppocr_mobile_v2.0_cls_infer', os.path.expanduser('~/Documents/Yolo/YoloV8/Paddlocr/model/ch_ppocr_mobile_v2.0_cls_infer')),
    'use_angle_cls': False,
    'lang': 'en',
    'conf_threshold_rec': 0.9,
    'use_gpu': True,
}

PERSON_ATTRIBUTES_CONFIG = {
    'inference_model_dir': _ext('PERSON_ATTR_MODEL_DIR', 'Attribute_person/inference', os.path.expanduser('~/Documents/PaddleClas/output/infer_model')),
    'class_id_map_file': _ext('PERSON_ATTR_CLASS_MAP', 'Attribute_person/class_id_map.txt', os.path.expanduser('~/Documents/Data/Person_attributes/class_id_map.txt')),
    'topk': 15,
    'use_gpu': True,
    'conf_threshold': 0.8
}

# SAM3 settings for helmet and attributes detection
SAM3_CONFIG = {
    'model_path': _p('models', 'sam3', 'model'),  # SAM3 model directory
    'bpe_path': _ext('SAM3_BPE_PATH', 'sam3/bpe_simple_vocab_16e6.txt.gz', os.path.expanduser('~/Documents/sam3/assets/bpe_simple_vocab_16e6.txt.gz')),  # BPE vocab file
    'device': 'cuda:0',
    'resolution': 672,
    'compile': False,  # Disable compilation to avoid Dynamo errors
    'confidence_threshold': 0.8,
    'iou_threshold': 0.6,
    'min_crop_size': 250,
    'target_classes': ['Helmet', 'Hat', 'Hood', 'Head', 'smart phone', 'goods carried on a motorbike'],
}

# YOLO Vehicle detection for helmet DAG (Helmet is on vehicle/motorcycle)
HELMET_VEHICLE_CONFIG = {
    'model_path': _p('models', 'Yolo_detect', 'Helmet', 'yolov26m_320_helmet_190626', 'weights', 'best.pt'),
    'conf_threshold': 0.7,
    'use_gpu': True,
}

OUTPUT_DIR = {
    'vehicle': _p('data', 'vehicle'),
    'person': _p('data', 'person'),
    'person_attributes': _p('data', 'attributes'),
    'anpr': _p('data', 'anpr'),
    'helmet': _p('data', 'helmet'),
}

RTSP_FILE ={
    'vehicle_lp': _p('rtsp', 'cam_vh_lp.txt'),
    'person': _p('rtsp', 'cam_person.txt'),
    'helmet': _p('rtsp', 'cam_vh_helmet.txt'),
} 
