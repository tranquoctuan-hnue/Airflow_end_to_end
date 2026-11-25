"""
Cấu hình từng DAG từ file YAML: config/dags.yaml (có trong git) + config/dags.local.yaml
(riêng máy này, không có trong git, không bắt buộc).

Mỗi DAG gọi apply_dag_config('<dag_id>') ở ĐẦU file DAG, TRƯỚC khi import code dự án.
Hàm này đổi các mục YAML của DAG đó thành biến môi trường (vd threshold →
CAMERA_EVENT_THRESHOLD), nên code đọc biến môi trường như cũ, không phải sửa.

Vì sao đi qua biến môi trường chứ không cho code đọc YAML trực tiếp: toàn bộ code
(crawler_core, platform_crawlers, src/modules) đã đọc cấu hình từ biến môi trường, và
quy ước dự án là mọi tham số ghi đè được bằng biến môi trường. Mỗi task Airflow chạy
trong 1 tiến trình riêng (parse lại file DAG của chính nó) nên mỗi DAG có giá trị riêng
được, vd social_crawler_youtube ngưỡng VideoMAE 0,5 còn social_crawler_x 0,6.

Ưu tiên (cao → thấp):
    biến môi trường thật / .env  >  dags.local.yaml  >  dags.yaml  >  mặc định trong code
Biến môi trường thắng để giữ quy ước cũ và để .env riêng từng máy (vd .35 đặt
CAMERA_EVENT_OUTPUT_DIR lên NAS) vẫn chạy. Khi 1 biến môi trường đang che giá trị YAML,
log task và scripts/doctor.py báo ⚠ để không ai sửa YAML rồi thắc mắc sao không đổi.

Mục YAML viết sai tên / sai kiểu → ConfigError ngay lúc parse DAG (DAG hiện lỗi import
trên giao diện) thay vì âm thầm chạy với mặc định: gõ nhầm `treshold: 0.6` mà DAG vẫn
chạy 0,4 là kiểu lỗi không ai phát hiện.

Đổi vị trí file: DAG_CONFIG_FILE, DAG_CONFIG_LOCAL_FILE.
"""

import logging
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.environ.get('DAG_CONFIG_FILE') or os.path.join(PROJECT_ROOT, 'config', 'dags.yaml')
LOCAL_CONFIG_FILE = (os.environ.get('DAG_CONFIG_LOCAL_FILE')
                     or os.path.join(PROJECT_ROOT, 'config', 'dags.local.yaml'))

logger = logging.getLogger('airflow.task.dag_config')  # nhánh airflow.task.* mới hiện INFO trong log task


class ConfigError(ValueError):
    pass


# ── Bảng mục YAML → biến môi trường ─────────────────────────────────────────
# (biến môi trường, kiểu). Kiểu: str | path | int | float | bool | list.
#   path: đường dẫn tương đối tính từ thư mục dự án, ~ được mở rộng.
#   list: danh sách YAML → nối bằng '|' (cách code đang đọc).
# Mục có dấu chấm = khối lồng nhau trong YAML (videomae: {threshold: ...}).

CHUNG = {
    'tracker_db':       ('CRAWLER_DB_PATH', 'path'),
    'models_dir':       ('VIDEO_PIPELINE_MODELS_DIR', 'path'),
    'gemini_model':     ('GEMINI_MODEL', 'str'),
    'gemini_keys_file': ('GEMINI_KEYS_FILE', 'path'),
}

SOCIAL = {
    'rotation_mode':            ('CRAWL_ROTATION_MODE', 'str'),
    'video_output_dir':         ('CRAWL_VIDEO_OUTPUT_DIR', 'path'),
    'rejected_dir':             ('CRAWL_REJECTED_DIR', 'path'),
    'staging_dir':              ('CRAWL_DOWNLOAD_STAGING_DIR', 'path'),
    'reject_action':            ('CRAWL_REJECT_ACTION', 'str'),
    'labels':                   ('CRAWL_LABELS', 'list'),
    'task_time_budget_minutes': ('CRAWL_TASK_TIME_BUDGET_MINUTES', 'int'),
    'max_per_label':            ('CRAWL_MAX_PER_LABEL', 'int'),
    'max_per_target':           ('CRAWL_MAX_PER_TARGET', 'int'),
    'max_per_platform':         ('CRAWL_MAX_PER_PLATFORM', 'int'),
    'max_scrolls':              ('CRAWL_MAX_SCROLLS', 'int'),
    'discovery_timeout_s':      ('CRAWL_DISCOVERY_TIMEOUT', 'int'),
    'max_duration_s':           ('CRAWL_MAX_DURATION', 'int'),
    'keywords_per_label':       ('CRAWL_KEYWORDS_PER_LABEL', 'int'),
    'llm_keywords':             ('CRAWL_LLM_KEYWORDS', 'bool'),
    'label_rotation.enabled':           ('CRAWL_LABEL_ROTATION', 'bool'),
    'label_rotation.empty_streak':      ('CRAWL_LABEL_EMPTY_STREAK', 'int'),
    'label_rotation.backoff_hours':     ('CRAWL_LABEL_BACKOFF_HOURS', 'int'),
    'label_rotation.backoff_max_hours': ('CRAWL_LABEL_BACKOFF_MAX_HOURS', 'int'),
    'label_rotation.target':            ('CRAWL_LABEL_TARGET', 'int'),
    'filters.cctv':             ('SOCIAL_USE_CLASSIFIER', 'bool'),
    'filters.videomae':         ('SOCIAL_USE_VIDEOMAE', 'bool'),
    'cctv.classifier':          ('CRAWL_CCTV_CLASSIFIER', 'str'),
    'cctv.threshold_hi':        ('CCTV_DETECTOR_HI', 'float'),
    'cctv.threshold_lo':        ('CCTV_DETECTOR_LO', 'float'),
    'cctv.min_cctv_seconds':    ('CCTV_DETECTOR_MIN_CCTV_SECONDS', 'float'),
    'cctv.probe':               ('CCTV_DETECTOR_PROBE', 'path'),
    'cctv.device':              ('CCTV_DETECTOR_DEVICE', 'str'),
    'cctv.qwen_model':          ('QWEN_MODEL_PATH', 'str'),
    'cctv.qwen_load_4bit':      ('QWEN_LOAD_4BIT', 'bool'),
    'videomae.model_dir':       ('VIDEOMAE_MODEL_DIR', 'path'),
    'videomae.checkpoint':      ('VIDEOMAE_CHECKPOINT', 'path'),
    'videomae.labels_file':     ('VIDEOMAE_LABELS', 'path'),
    'videomae.threshold':       ('VIDEOMAE_THRESHOLD', 'float'),
    'videomae.window_s':        ('VIDEOMAE_WINDOW_S', 'float'),
    'videomae.aggregate':       ('VIDEOMAE_AGGREGATE', 'str'),
    'videomae.temporal_views':  ('VIDEOMAE_TEMPORAL_VIEWS', 'str'),
    'videomae.num_crops':       ('VIDEOMAE_NUM_CROPS', 'int'),
    'videomae.device':          ('VIDEOMAE_DEVICE', 'str'),
    'videomae.batch':           ('VIDEOMAE_BATCH', 'int'),
    'videomae.fp16':            ('VIDEOMAE_FP16', 'bool'),
    'videomae.decoder':         ('VIDEOMAE_DECODER', 'str'),
    'dedup.duration_tolerance': ('CRAWL_DEDUP_DURATION_TOLERANCE', 'float'),
    'dedup.cross_dist':         ('CRAWL_DEDUP_CROSS_DIST', 'int'),
    'dedup.cross_min_frames':   ('CRAWL_DEDUP_CROSS_MIN_FRAMES', 'int'),
    'dedup.low_info_edge':      ('CRAWL_DEDUP_LOW_INFO_EDGE', 'float'),
}

CAMERA_DATASET = {
    'run_mode':      ('VIDEO_PIPELINE_RUN_MODE', 'str'),
    # vehicle_anpr và helmet dùng CHUNG model xe (VEHICLE_CONFIG) — 3 DAG camera_dataset
    # nằm chung 1 file nên không tách model xe riêng cho từng DAG được.
    'vehicle_model': ('VEHICLE_MODEL_PATH', 'path'),
    'vehicle_conf':  ('VEHICLE_CONF_THRESHOLD', 'float'),
}

SCHEMA = {
    'chung': CHUNG,
    'social_crawler': SOCIAL,
    'camera_dataset': CAMERA_DATASET,
    'social_crawler_youtube': {
        'keywords': ('CRAWL_YOUTUBE_KEYWORDS', 'list'),
    },
    'social_crawler_dailymotion': {
        'keywords':       ('CRAWL_DAILYMOTION_KEYWORDS', 'list'),
        # Dailymotion có trần thời lượng riêng (900s) — mục này thay cho max_duration_s chung
        'max_duration_s': ('CRAWL_DAILYMOTION_MAX_DURATION', 'int'),
        'use_api':        ('CRAWL_DAILYMOTION_USE_API', 'bool'),
        'api_max_pages':  ('CRAWL_DAILYMOTION_API_MAX_PAGES', 'int'),
        'max_pages':      ('CRAWL_DAILYMOTION_MAX_PAGES', 'int'),
    },
    'social_crawler_reddit': {
        'subreddits':    ('CRAWL_REDDIT_SUBS', 'list'),
        'global_search': ('CRAWL_REDDIT_GLOBAL_SEARCH', 'bool'),
    },
    'social_crawler_x': {
        'queries':       ('CRAWL_X_QUERIES', 'list'),
        'accounts':      ('CRAWL_X_ACCOUNTS', 'list'),
        'search_suffix': ('CRAWL_X_SEARCH_SUFFIX', 'str'),
        'search_tab':    ('CRAWL_X_SEARCH_TAB', 'str'),
    },
    'social_crawler_fb': {
        'keywords':    ('CRAWL_FACEBOOK_KEYWORDS', 'list'),
        # Facebook có số lần cuộn riêng — mục này thay cho max_scrolls chung
        'max_scrolls': ('CRAWL_FACEBOOK_MAX_SCROLLS', 'int'),
    },
    'legacy_fb_groups_crawler': {
        'schedule':   ('FB_GROUPS_SCHEDULE', 'str'),
        'headless':   ('FB_HEADLESS', 'bool'),
        'batch_size': ('FB_BATCH_SIZE', 'int'),
        'category':   ('FB_CATEGORY', 'str'),
        'deep_scan':  ('FB_DEEP_SCAN', 'bool'),
    },
    'camera_dataset_video_events': {
        'camera_file':        ('CAMERA_EVENT_FILE', 'path'),
        'model_dir':          ('CAMERA_EVENT_MODEL_DIR', 'path'),
        'checkpoint':         ('CAMERA_EVENT_CHECKPOINT', 'path'),
        'thresholds_file':    ('CAMERA_EVENT_THRESHOLDS', 'path'),
        'threshold':          ('CAMERA_EVENT_THRESHOLD', 'float'),
        'output_dir':         ('CAMERA_EVENT_OUTPUT_DIR', 'path'),
        'ring_dir':           ('CAMERA_EVENT_RING_DIR', 'path'),
        'minutes_per_camera': ('CAMERA_EVENT_MINUTES', 'float'),
        'run_minutes':        ('CAMERA_EVENT_RUN_MINUTES', 'float'),
        'segment_s':          ('CAMERA_EVENT_SEGMENT_S', 'int'),
        'device':             ('CAMERA_EVENT_DEVICE', 'str'),
        'crf':                ('CAMERA_EVENT_CRF', 'int'),
        'max_height':         ('CAMERA_EVENT_MAX_HEIGHT', 'int'),
        'pool':               ('CAMERA_EVENT_POOL', 'str'),
    },
    'camera_dataset_vehicle_anpr': {
        'schedule':          ('CAMERA_VEHICLE_ANPR_SCHEDULE', 'str'),
        'camera_file':       ('CAMERA_VEHICLE_LP_FILE', 'path'),
        'ocr_det_model_dir': ('ANPR_DET_MODEL_DIR', 'path'),
        'ocr_rec_model_dir': ('ANPR_REC_MODEL_DIR', 'path'),
        'ocr_conf':          ('ANPR_REC_CONF_THRESHOLD', 'float'),
        'vehicle_output_dir': ('CAMERA_VEHICLE_OUTPUT_DIR', 'path'),
        'anpr_output_dir':   ('CAMERA_ANPR_OUTPUT_DIR', 'path'),
    },
    'camera_dataset_person_attributes': {
        'schedule':              ('CAMERA_PERSON_SCHEDULE', 'str'),
        'camera_file':           ('CAMERA_PERSON_FILE', 'path'),
        'person_model':          ('PERSON_MODEL_PATH', 'path'),
        'person_conf':           ('PERSON_CONF_THRESHOLD', 'float'),
        'attribute_model_dir':   ('PERSON_ATTR_MODEL_DIR', 'path'),
        'attribute_class_map':   ('PERSON_ATTR_CLASS_MAP', 'path'),
        'attribute_conf':        ('PERSON_ATTR_CONF_THRESHOLD', 'float'),
        'person_output_dir':     ('CAMERA_PERSON_OUTPUT_DIR', 'path'),
        'attributes_output_dir': ('CAMERA_ATTRIBUTES_OUTPUT_DIR', 'path'),
    },
    'camera_dataset_helmet': {
        'schedule':          ('CAMERA_HELMET_SCHEDULE', 'str'),
        'run_hours':         ('CAMERA_HELMET_RUN_HOURS', 'float'),
        'camera_file':       ('CAMERA_HELMET_FILE', 'path'),
        'helmet_model':      ('HELMET_MODEL_PATH', 'path'),
        'helmet_conf':       ('HELMET_CONF_THRESHOLD', 'float'),
        'helmet_output_dir': ('CAMERA_HELMET_OUTPUT_DIR', 'path'),
    },
    'camera_source_youtube_live': {
        'schedule':             ('YOUTUBE_CCTV_SCHEDULE', 'str'),
        'run_hours':            ('YOUTUBE_CCTV_RUN_HOURS', 'float'),
        'keywords':             ('YOUTUBE_CCTV_KEYWORDS', 'list'),
        'llm_keywords':         ('YOUTUBE_CCTV_LLM_KEYWORDS', 'bool'),
        'num_llm_keywords':     ('YOUTUBE_CCTV_NUM_LLM_KEYWORDS', 'int'),
        'results_per_keyword':  ('YOUTUBE_CCTV_RESULTS_PER_KEYWORD', 'int'),
        'cctv_filter':          ('YOUTUBE_CCTV_USE_CLASSIFIER', 'bool'),
        'qwen_model':           ('QWEN_MODEL_PATH', 'str'),
        'output_file':          ('YOUTUBE_CCTV_OUTPUT_FILE', 'path'),
        'snapshot_dir':         ('YOUTUBE_CCTV_SNAPSHOT_DIR', 'path'),
        'checked_history_file': ('YOUTUBE_CCTV_CHECKED_HISTORY', 'path'),
    },
}

# DAG → nhóm. Mục ghi ở nhóm áp dụng cho mọi DAG trong nhóm; mục cùng tên ở phần riêng
# của DAG ghi đè. legacy_fb_groups_crawler cũng tải qua crawler_core (nơi lưu, bộ lọc
# CCTV, VideoMAE) nên dùng chung nhóm social_crawler.
GROUP_OF = {
    'social_crawler_youtube': 'social_crawler',
    'social_crawler_dailymotion': 'social_crawler',
    'social_crawler_reddit': 'social_crawler',
    'social_crawler_x': 'social_crawler',
    'social_crawler_fb': 'social_crawler',
    'legacy_fb_groups_crawler': 'social_crawler',
    'camera_dataset_vehicle_anpr': 'camera_dataset',
    'camera_dataset_person_attributes': 'camera_dataset',
    'camera_dataset_helmet': 'camera_dataset',
}

# Chỉ được đặt ở phần nhóm: vòng xoay chain/pool phải giống nhau cho mọi DAG trong vòng.
GROUP_ONLY = {'rotation_mode'}


# ── Đọc + kiểm tra ──────────────────────────────────────────────────────────

def _read_yaml(path):
    if not os.path.exists(path):
        return {}
    import yaml
    try:
        with open(path, encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f'{_rel(path)} sai cú pháp YAML: {e}') from None
    if not isinstance(data, dict):
        raise ConfigError(f'{_rel(path)}: cấp ngoài cùng phải là các phần "tên_dag:"')
    for name, body in data.items():
        if body is not None and not isinstance(body, dict):
            raise ConfigError(f'{_rel(path)}: phần {name!r} phải là các dòng "mục: giá trị"')
    return data


def _rel(path):
    return os.path.relpath(path, PROJECT_ROOT) if path.startswith(PROJECT_ROOT) else path


def _flatten(d, prefix=''):
    out = {}
    for k, v in (d or {}).items():
        key = f'{prefix}{k}'
        if isinstance(v, dict):
            out.update(_flatten(v, key + '.'))
        else:
            out[key] = v
    return out


def load():
    """{phần: {mục.có.chấm: (giá trị, file)}} — dags.local.yaml ghi đè dags.yaml theo từng mục."""
    merged = {}
    for path in (CONFIG_FILE, LOCAL_CONFIG_FILE):
        for section, body in _read_yaml(path).items():
            for key, value in _flatten(body).items():
                merged.setdefault(section, {})[key] = (value, _rel(path))
    return merged


def _schema_for(section):
    """Mục hợp lệ của 1 phần: phần nhóm/chung theo SCHEMA, phần DAG = mục nhóm + mục riêng."""
    if section in GROUP_OF:
        group = {k: v for k, v in SCHEMA[GROUP_OF[section]].items() if k not in GROUP_ONLY}
        return {**group, **SCHEMA.get(section, {})}
    return SCHEMA.get(section)


def _convert(kind, value, where):
    if kind == 'bool':
        if isinstance(value, bool):
            return 'true' if value else 'false'
        raise ConfigError(f'{where}: cần true/false, đang là {value!r}')
    if kind == 'int':
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        raise ConfigError(f'{where}: cần số nguyên, đang là {value!r}')
    if kind == 'float':
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        raise ConfigError(f'{where}: cần số, đang là {value!r}')
    if kind == 'list':
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list) or any(isinstance(x, (dict, list)) for x in value):
            raise ConfigError(f'{where}: cần danh sách, đang là {value!r}')
        return '|'.join(str(x).strip() for x in value if str(x).strip())
    if isinstance(value, (dict, list)):
        raise ConfigError(f'{where}: cần 1 giá trị, đang là {value!r}')
    text = str(value).strip()
    if kind == 'path' and text:
        text = os.path.expanduser(text)
        if not os.path.isabs(text):
            text = os.path.normpath(os.path.join(PROJECT_ROOT, text))
    return text


def resolve(*dag_ids, data=None):
    """{biến môi trường: (giá trị, nguồn)} cho các DAG này theo YAML (chưa xét env)."""
    data = load() if data is None else data
    sections = ['chung']
    for dag_id in dag_ids:
        group = GROUP_OF.get(dag_id)
        if group and group not in sections:
            sections.append(group)
    sections += [d for d in dag_ids if d not in sections]

    env = {}
    for section in sections:
        schema = _schema_for(section)
        if schema is None:
            raise ConfigError(f'Không có phần {section!r} trong bảng mục của dags/dag_config.py')
        for key, (value, src) in data.get(section, {}).items():
            where = f'{src} [{section}] {key}'
            if key not in schema:
                hint = ' (chỉ đặt được ở phần nhóm)' if key in GROUP_ONLY else ''
                raise ConfigError(f'{where}: mục không tồn tại{hint}. Mục hợp lệ: '
                                  + ', '.join(sorted(schema)))
            if value is None:       # để trống = dùng mặc định trong code
                continue
            name, kind = schema[key]
            text = _convert(kind, value, where)
            if text == '':          # [] hoặc "" cũng = mặc định trong code
                continue
            env[name] = (text, f'{src} [{section}] {key}')
    return env


# ── Áp vào biến môi trường ──────────────────────────────────────────────────
# Biến có sẵn khi module này được nạp lần đầu (môi trường thật + .env) luôn thắng YAML.
# Biến do YAML đặt được ghi nhớ để lần apply sau (vd doctor parse mọi DAG trong 1 tiến
# trình) gỡ ra trước, không rò giá trị của DAG trước sang DAG sau.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
except Exception:
    pass
_BASE_ENV = frozenset(os.environ)
_applied = {}       # biến → giá trị YAML đã đặt
_shadowed = {}      # biến → (giá trị env đang thắng, giá trị YAML, nguồn)
_current = ()


def apply_dag_config(*dag_ids):
    """Đặt biến môi trường theo YAML cho các DAG này. Gọi ở đầu file DAG."""
    global _current
    for name, value in list(_applied.items()):
        if os.environ.get(name) == value:
            os.environ.pop(name, None)
    _applied.clear()
    _shadowed.clear()
    _current = dag_ids

    for name, (value, src) in resolve(*dag_ids).items():
        if name in _BASE_ENV:
            if os.environ.get(name, '') != value:
                _shadowed[name] = (os.environ.get(name, ''), value, src)
            continue
        os.environ[name] = value
        _applied[name] = value
    return dict(_applied)


def log_applied():
    """Gọi ở đầu task: ghi vào log task cấu hình nào đang có hiệu lực."""
    files = [_rel(p) for p in (CONFIG_FILE, LOCAL_CONFIG_FILE) if os.path.exists(p)]
    logger.info(f'Cấu hình DAG {", ".join(_current)}: {", ".join(files) or "không có file YAML"} '
                f'— {len(_applied)} mục áp dụng')
    for name, (env_val, yaml_val, src) in _shadowed.items():
        logger.warning(f'{src} = {yaml_val!r} KHÔNG có hiệu lực: biến môi trường/.env '
                       f'{name}={env_val!r} đang ghi đè (bỏ nó khỏi .env/start_airflow.sh '
                       f'nếu muốn dùng giá trị YAML)')


def schedule_from_env(name, default):
    """Lịch DAG từ biến môi trường: không đặt → default; 'none'/'manual' → chỉ chạy tay."""
    raw = os.environ.get(name, '').strip()
    if not raw:
        return default
    return None if raw.lower() in ('none', 'manual', 'null') else raw


def check_all():
    """Cho scripts/doctor.py: [(mức, nội dung)] — kiểm tra mọi phần, không đổi môi trường."""
    out = []
    try:
        data = load()
    except ConfigError as e:
        return [('fail', str(e))]
    for section in data:
        if _schema_for(section) is None:
            out.append(('fail', f'phần {section!r} không phải DAG/nhóm nào. Có: '
                                + ', '.join(sorted(set(SCHEMA) | set(GROUP_OF)))))
    dags = sorted(set(GROUP_OF) | {s for s in SCHEMA if s not in ('chung', 'social_crawler', 'camera_dataset')})
    seen = set()
    for dag_id in dags:
        try:
            env = resolve(dag_id, data=data)
        except ConfigError as e:
            if str(e) not in seen:
                seen.add(str(e))
                out.append(('fail', str(e)))
            continue
        for name, (value, src) in env.items():
            cur = os.environ.get(name)
            if name in _BASE_ENV and cur != value and (name, src) not in seen:
                seen.add((name, src))
                out.append(('warn', f'{src} = {value!r} bị biến môi trường/.env {name}={cur!r} ghi đè'))
    return out
