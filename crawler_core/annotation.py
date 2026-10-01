"""
File annotation <video>.json cạnh mỗi video — format "video_classification" 1.0.0 của
công cụ gán nhãn (mở thẳng được để sửa nhãn từng đoạn):

    {"version": "1.0.0", "type": "video_classification", "video": "x.mp4",
     "fps": 30.0, "duration_ms": 20501, "width": 640, "height": 360,
     "labels": [...], "label_colors": {...},
     "segments": [{"id", "label", "start_ms", "end_ms", "start_frame", "end_frame",
                   "description": "VideoMAE scores:\\nNormal: 67.1%\\n..."}]}

Ghi cho: video ĐƯỢC NHẬN (thư mục dataset) và video bị loại `no_event` — thay file
lý-do-loại cũ (url, CLIP, VideoMAE thô vẫn nằm trong tracker.db, cột filter_detail).
Video bị loại vì lý do khác (not_cctv, portrait, dup_*) vẫn ghi file lý-do-loại cũ.

Mỗi đoạn VideoMAE (5s) = 1 segment. Nhãn của đoạn = nhãn sự việc cao nhất nếu đạt
ngưỡng của bộ lọc (đúng tiêu chí giữ/loại video), ngược lại "Normal" — người gán
nhãn sửa tiếp từ đó. Tên nhãn theo tên thư mục dataset (RoadAccident, GunRobber),
không theo tên lớp của model ("Road Accident", "Gun-toting robber").
"""

import hashlib
import json
import logging
import os
import re
import subprocess

logger = logging.getLogger(__name__)

VERSION = '1.0.0'
TYPE = 'video_classification'

# 9 màu đầu lấy đúng theo file mẫu của công cụ gán nhãn (2026-10-01), phần còn lại
# chọn thêm cho đủ các lớp của VideoMAE — giữ cố định để màu không đổi giữa các file.
LABEL_COLORS = {
    'Abuse': '#ffaaff', 'Arrest': '#55aaff', 'Arson': '#aaffff',
    'Explosion': '#ffaa00', 'Fighting': '#ffaaaa', 'Normal': '#aaffaa',
    'RoadAccident': '#ffffaa', 'Robbery': '#28ffff', 'Vandalism': '#00ff7f',
    'ArmedFight': '#ff7f7f', 'ArmedSuspect': '#ff5500', 'Burglary': '#aa88ff',
    'CarTheft': '#ffd27f', 'Collapse': '#c8a07d', 'GunRobber': '#ff557f',
    'KnifeRobber': '#d07fff', 'MotorTheft': '#7fbfff', 'Shooting': '#ff0000',
    'Smoke': '#b4b4b4', 'Stealing': '#7fffd4', 'Unconscious': '#ffb6c1',
    'Drowning': '#5f9fff', 'PourPetrol': '#e0c000', 'Riot': '#ff8c69',
    'ShootDown': '#c04040', 'Stampede': '#9acd32', 'Snatching': '#40e0d0',
}


def dataset_label(model_label: str) -> str:
    """Tên lớp của model → tên nhãn dataset: 'Road Accident' → RoadAccident,
    'Gun-toting robber' → GunRobber (theo platform_crawlers.labels)."""
    try:
        from crawler_core.pipeline import _same_label
        from platform_crawlers import labels as L
        names = list(L.LABELS)
        for n in names:                          # tên hiển thị trùng khớp
            if L.display(n).lower() == model_label.lower():
                return n
        flat = re.sub(r'[^0-9a-z]', '', model_label.lower())
        for n in names:                          # Road Accident ~ RoadAccident
            if n.lower() == flat:
                return n
        for n in names:                          # Smoke ~ Smok
            if _same_label(model_label, n) or _same_label(model_label, L.display(n)):
                return n
    except Exception:
        pass
    return ''.join(w[:1].upper() + w[1:] for w in re.split(r'[^0-9A-Za-z]+', model_label) if w)


def _probe(path: str) -> dict:
    """fps / width / height / số frame khi VideoMAE không chạy (tắt, hoặc nhãn Normal)."""
    try:
        st = (json.loads(subprocess.run(
            ['ffprobe', '-v', 'quiet', '-select_streams', 'v:0', '-show_entries',
             'stream=width,height,avg_frame_rate', '-of', 'json', path],
            capture_output=True, text=True, timeout=30).stdout or '{}'
        ).get('streams') or [{}])[0]
        num, _, den = str(st.get('avg_frame_rate', '0/1')).partition('/')
        fps = float(num) / float(den or 1) if float(den or 1) else 0.0
        return {'fps': fps if 0 < fps <= 120 else 25.0,
                'width': int(st.get('width') or 0), 'height': int(st.get('height') or 0)}
    except Exception:
        return {}


def build(video_path: str, duration_s: float, videomae: dict | None = None,
          model_labels: list[str] | None = None, model: str = 'VideoMAE') -> dict:
    """
    videomae: {'windows': [...], 'video': {fps, width, height, n_frames}, 'threshold'}
        — 'windows' từ VideoMAEEventFilter.assess_video. None = không có điểm VideoMAE
        (bộ lọc tắt / nhãn Normal) → segments rỗng, phần còn lại vẫn đủ.
    """
    videomae = videomae or {}
    meta = videomae.get('video') or _probe(video_path)
    fps = float(meta.get('fps') or 25.0)
    name = os.path.basename(video_path)
    if duration_s and duration_s > 0:
        duration_ms = int(round(duration_s * 1000))
    else:
        duration_ms = int(round((meta.get('n_frames') or 0) / fps * 1000))

    if model_labels:
        labels = sorted({dataset_label(x) for x in model_labels} | {'Normal'})
    else:                                        # VideoMAE tắt → nhãn của crawler
        try:
            from platform_crawlers import labels as L
            labels = sorted(set(L.enabled_labels()) | {'Normal'})
        except Exception:
            labels = ['Normal']
    threshold = videomae.get('threshold', 0.5)
    segments = []
    for w in videomae.get('windows') or []:
        sf, ef = int(w['start_frame']), int(w['end_frame'])
        is_event = w.get('prob', 0) >= threshold
        scores = '\n'.join(f'{dataset_label(lb)}: {p * 100:.1f}%'
                           for lb, p in w.get('scores') or [])
        segments.append({
            'id':          's' + hashlib.sha1(f'{name}:{sf}'.encode()).hexdigest()[:10],
            'label':       dataset_label(w['label']) if is_event else 'Normal',
            'start_ms':    int(round(sf / fps * 1000)),
            'end_ms':      min(int(round(ef / fps * 1000)), duration_ms or 10**12),
            'start_frame': sf,
            'end_frame':   ef,
            'description': f'{model} scores:\n{scores}',
        })

    return {
        'version':      VERSION,
        'type':         TYPE,
        'video':        name,
        'fps':          round(fps, 3),
        'duration_ms':  duration_ms,
        'width':        int(meta.get('width') or 0),
        'height':       int(meta.get('height') or 0),
        'labels':       labels,
        'label_colors': {lb: LABEL_COLORS.get(lb, '#cccccc') for lb in labels},
        'segments':     segments,
    }


def write(video_path: str, data: dict) -> str | None:
    path = os.path.splitext(video_path)[0] + '.json'
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path
    except Exception as e:
        logger.warning(f'[Annotation] Không ghi được {path}: {e}')
        return None
