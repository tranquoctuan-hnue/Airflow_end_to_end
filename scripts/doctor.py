"""
Kiểm tra máy đã đủ điều kiện chạy dự án chưa — chạy bất cứ lúc nào nghi có vấn đề.

    airflow_venv/bin/python scripts/doctor.py            # đầy đủ (kèm parse DAG, ~30s)
    airflow_venv/bin/python scripts/doctor.py --no-dags  # bỏ bước parse DAG

✓ = ổn · ⚠ = chạy được nhưng thiếu tính năng · ✗ = phải sửa. Exit code 1 nếu có ✗.
Chỉ ĐỌC, không sửa gì trên máy.
"""

import importlib
import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault('AIRFLOW_HOME', ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, '.env'))
except Exception:
    pass

results = []


def report(level, what, detail='', fix=''):
    results.append(level)
    mark = {'ok': '✓', 'warn': '⚠', 'fail': '✗'}[level]
    print(f'  {mark} {what}' + (f' — {detail}' if detail else ''))
    if fix and level != 'ok':
        print(f'      → {fix}')


def section(title):
    print(f'\n{title}')


# ── Python + thư viện ────────────────────────────────────────────────────────
section('Python & thư viện')
v = sys.version_info
report('ok' if v[:2] == (3, 10) else 'warn', f'Python {v.major}.{v.minor}.{v.micro}',
       '' if v[:2] == (3, 10) else 'dự án chỉ kiểm thử trên 3.10')

for mod, why in [
    ('airflow', 'Airflow'), ('yt_dlp', 'tải video'), ('playwright', 'discovery X/Reddit/FB'),
    ('cv2', 'xử lý video'), ('imagehash', 'dedup L3/L4'), ('decord', 'đọc frame VideoMAE'),
    ('open_clip', 'bộ lọc CCTV'), ('timm', 'VideoMAE'), ('pytesseract', 'OCR timestamp'),
    ('google.genai', 'sinh từ khóa Gemini'), ('dotenv', 'đọc .env'),
]:
    try:
        m = importlib.import_module(mod)
        report('ok', f'{mod}', getattr(m, '__version__', '') or why)
    except Exception as e:
        report('fail', f'{mod} ({why})', type(e).__name__,
               'bash scripts/setup.sh  (hoặc pip install -r requirements.txt)')

try:
    import yt_dlp
    ver = yt_dlp.version.__version__
    report('ok' if ver >= '2026.08.19' else 'warn', f'yt-dlp {ver}',
           '' if ver >= '2026.08.19' else 'bản cũ — YouTube hay lỗi HTTP 403',
           'pip install -U yt-dlp')
except Exception:
    pass

try:
    import torch
    if torch.cuda.is_available():
        p = torch.cuda.get_device_properties(0)
        report('ok', f'torch {torch.__version__} + CUDA', f'{p.name}, {p.total_memory // 2**20} MB')
    else:
        report('warn', f'torch {torch.__version__} KHÔNG có CUDA',
               'bộ lọc CLIP/VideoMAE sẽ rất chậm',
               'dùng GPU NVIDIA, hoặc SOCIAL_USE_VIDEOMAE=false trong .env')
except Exception as e:
    report('fail', 'torch', type(e).__name__, 'pip install -r requirements-torch.txt --index-url ...')

# ── Công cụ hệ thống ─────────────────────────────────────────────────────────
section('Công cụ hệ thống')
for tool, fix in [('ffmpeg', 'sudo apt install ffmpeg'), ('ffprobe', 'sudo apt install ffmpeg'),
                  ('tesseract', 'sudo apt install tesseract-ocr')]:
    report('ok' if shutil.which(tool) else 'fail', tool, '', fix)
deno = shutil.which('deno') or next((p for p in (os.path.expanduser('~/.deno/bin/deno'),
                                                 os.path.expanduser('~/apps/deno/bin/deno'))
                                     if os.path.exists(p)), None)
report('ok' if deno else 'warn', 'deno (yt-dlp giải mã YouTube)', deno or 'không thấy',
       'curl -fsSL https://deno.land/install.sh | sh')
pw_dir = os.path.expanduser('~/.cache/ms-playwright')
has_chromium = os.path.isdir(pw_dir) and any(d.startswith('chromium') for d in os.listdir(pw_dir))
report('ok' if has_chromium else 'fail', 'Playwright Chromium', '',
       'airflow_venv/bin/playwright install chromium')

# ── Cấu hình máy ─────────────────────────────────────────────────────────────
section('Cấu hình (.env, key, nơi lưu)')
report('ok' if os.path.exists(os.path.join(ROOT, '.env')) else 'warn', '.env', '',
       'cp .env.example .env rồi sửa')

try:
    from crawler_core.downloader import OUTPUT_BASE, REJECTED_BASE
    for name, path in (('Nơi lưu video', OUTPUT_BASE), ('Nơi lưu video bị loại', REJECTED_BASE)):
        parent = path if os.path.isdir(path) else os.path.dirname(path)
        ok = os.path.isdir(parent) and os.access(parent, os.W_OK)
        report('ok' if ok else 'fail', name, path,
               'đặt CRAWL_VIDEO_OUTPUT_DIR trong .env tới thư mục ghi được (NAS chưa mount?)')
except Exception as e:
    report('fail', 'crawler_core.downloader', str(e)[:120])

keys_file = os.path.join(ROOT, 'config', 'gemini_keys.json')
try:
    keys = [k for k in json.load(open(keys_file)).get('keys', []) if k and 'DÁN_' not in k]
    report('ok' if keys else 'warn', 'Gemini API key', f'{len(keys)} key',
           f'điền key vào {os.path.relpath(keys_file, ROOT)} (không có → dùng từ khóa tĩnh)')
except FileNotFoundError:
    report('warn', 'Gemini API key', 'chưa có file',
           'cp config/gemini_keys.example.json config/gemini_keys.json')
except Exception as e:
    report('warn', 'Gemini API key', f'file lỗi: {e}')

# DB lịch sử dedup: thiếu thì crawler vẫn chạy (tạo DB rỗng) nhưng tải lại mọi video đã có
try:
    import sqlite3
    from crawler_core.db_manager import DB_PATH, _LEGACY_DB_PATH
    if os.path.exists(DB_PATH):
        n = sqlite3.connect(DB_PATH).execute('SELECT COUNT(*) FROM video_urls').fetchone()[0]
        report('ok', 'DB lịch sử dedup', f'{os.path.relpath(DB_PATH, ROOT)} — {n} URL')
    elif os.path.exists(_LEGACY_DB_PATH):
        report('fail', 'DB lịch sử dedup', 'còn ở vị trí cũ facebook_crawler/db/',
               'mkdir -p data/db && mv facebook_crawler/db/tracker.db data/db/')
    else:
        report('warn', 'DB lịch sử dedup', 'chưa có — sẽ tạo DB rỗng',
               'chép data/db/tracker.db từ máy cũ để không tải trùng video đã có')
except Exception as e:
    report('warn', 'DB lịch sử dedup', f'không đọc được: {e}')

# ── Model ────────────────────────────────────────────────────────────────────
section('Model')
use_vmae = os.environ.get('SOCIAL_USE_VIDEOMAE', 'true').lower() == 'true'
vdir = os.environ.get('VIDEOMAE_MODEL_DIR') or os.path.join(
    ROOT, 'models', 'Video_understanding', 'vitl_224x224_240926')
vfiles = [os.path.join(vdir, f) for f in ('checkpoint-best.pth', 'labels.txt')]
if all(os.path.exists(f) for f in vfiles):
    report('ok', 'VideoMAE', os.path.relpath(vdir, ROOT) if vdir.startswith(ROOT) else vdir)
else:
    report('fail' if use_vmae else 'ok', 'VideoMAE',
           f'thiếu checkpoint-best.pth / labels.txt trong {vdir}' if use_vmae else 'đang tắt',
           'chép model theo models/README.md, hoặc SOCIAL_USE_VIDEOMAE=false trong .env')

# ── Cookie đăng nhập ─────────────────────────────────────────────────────────
section('Cookie đăng nhập (chỉ cần cho platform định chạy)')
try:
    sys.path.insert(0, ROOT)
    from platform_crawlers import sessions as _sessions
    _need = _sessions.login_needed()
except Exception:
    _need = {}
for platform, need in (('x', 'bắt buộc'), ('facebook', 'bắt buộc'),
                       ('reddit', 'nên có'), ('dailymotion', 'dự phòng')):
    path = os.path.join(ROOT, 'cookies', f'{platform}_cookies.json')
    if platform in _need:
        report('warn', f'{platform} ({need})', f'CẦN ĐĂNG NHẬP LẠI từ {_need[platform]["since"]}',
               f'đăng nhập {platform} trong Chrome — lượt crawl sau tự lấy cookie '
               f'(hoặc chạy scripts/check_sessions.py {platform})')
    else:
        report('ok' if os.path.exists(path) else 'warn', f'{platform} ({need})',
               '' if os.path.exists(path) else 'chưa có cookie',
               f'đăng nhập {platform} trong Chrome rồi chạy '
               f'python scripts/import_browser_cookies.py {platform}')

# ── Parse DAG ────────────────────────────────────────────────────────────────
if '--no-dags' not in sys.argv:
    section('DAG (parse bằng Airflow, ~30s)')
    try:
        import logging
        logging.disable(logging.WARNING)
        from airflow.models import DagBag
        bag = DagBag(dag_folder=os.path.join(ROOT, 'dags'), include_examples=False)
        for path, err in bag.import_errors.items():
            report('fail', os.path.relpath(path, ROOT), str(err).strip().splitlines()[-1][:160])
        report('ok' if not bag.import_errors else 'fail', f'{len(bag.dag_ids)} DAG parse được')
    except Exception as e:
        report('fail', 'Không parse được DAG', f'{type(e).__name__}: {e}'[:160])

# ── Tổng kết ─────────────────────────────────────────────────────────────────
n_fail, n_warn = results.count('fail'), results.count('warn')
print(f'\n{"✗" if n_fail else "✓"} {n_fail} lỗi, {n_warn} cảnh báo')
sys.exit(1 if n_fail else 0)
