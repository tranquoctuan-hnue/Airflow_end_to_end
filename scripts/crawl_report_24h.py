#!/usr/bin/env python3
"""
Báo cáo thống kê sau 24h chạy các DAG crawler.

    python scripts/crawl_report_24h.py                  # 24h gần nhất
    python scripts/crawl_report_24h.py --hours 48       # đổi cửa sổ
    python scripts/crawl_report_24h.py --stdout         # in cả báo cáo ra terminal
    python scripts/crawl_report_24h.py --out /tmp/bc.md # chỉ định file

Ghi 1 file Markdown vào reports/ và in đường dẫn. Trả lời 4 câu hỏi vận hành:

  1. Vòng xoay có chạy đều không?  (5 DAG × pool 1 slot — ai chiếm bao lâu,
     máy rảnh bao nhiêu, có lượt nào hết time budget)
  2. Thu được gì?                  (funnel dedup L0→L4 gộp theo platform)
  3. Nhãn có cân bằng chưa?        (11 nhãn — nhãn nào vẫn trắng, con trỏ xoay
     vòng đang ở đâu; đây là vấn đề chính mà label_cursor.py sinh ra để chữa)
  4. Có gì đang hỏng?              (mục "Cảnh báo" tự phát hiện)

Nguồn số liệu, ưu tiên nguồn chính xác nhất trước:
    airflow.db  → xcom.return_value: funnel dedup CHÍNH XÁC của từng lượt chạy
                  (task trả về dict này, không phải parse log)
                  dag_run/task_instance: thời gian chiếm slot, state
    tracker.db  → video_urls/sources/video_fingerprints: kết quả tích lũy
    logs/       → chỉ để đếm cảnh báo (timeout discovery, cookie hỏng)
    state/label_cursor.json → nhãn sẽ chạy ở lượt tới

CHỈ dùng thư viện chuẩn + sqlite3, KHÔNG import airflow (import airflow mất
vài giây và cần đúng AIRFLOW_HOME). Chạy được cả khi Airflow đang tắt.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

AIRFLOW_DB = os.environ.get('CRAWL_REPORT_AIRFLOW_DB',
                            os.path.join(REPO_ROOT, 'airflow.db'))
TRACKER_DB = os.environ.get('CRAWLER_DB_PATH',
                            os.path.join(REPO_ROOT, 'data', 'db', 'tracker.db'))
LOGS_DIR   = os.path.join(REPO_ROOT, 'logs')


def _video_dir() -> str:
    """Cùng chỗ lưu với downloader (NAS pvn_share) — đừng hardcode data/videos."""
    try:
        from crawler_core.downloader import OUTPUT_BASE
        return OUTPUT_BASE
    except Exception:
        return os.environ.get('CRAWL_VIDEO_OUTPUT_DIR') or os.path.join(
            REPO_ROOT, 'data', 'videos'
        )


VIDEO_DIR = _video_dir()

CRAWLER_DAGS = [
    'social_crawler_dailymotion',
    'social_crawler_fb',
    'social_crawler_reddit',
    'social_crawler_x',
    'social_crawler_youtube',
]
GPU_POOL = 'social_crawler_gpu'

# Cột funnel lấy từ xcom.return_value, theo đúng thứ tự pipeline xử lý
FUNNEL = [
    ('seen',        'URL nhận'),
    ('new_urls',    'URL mới'),
    ('dup_url',     'trùng L0'),
    ('dup_l1',      'trùng L1'),
    ('cctv',        'là CCTV'),
    ('not_cctv',    'loại: không CCTV'),
    ('portrait',    'loại: 9:16'),
    ('dup_l2',      'trùng L2'),
    ('dup_l3',      'trùng L3'),
    ('dup_l4',      'trùng L4'),
    ('downloaded',  'TẢI ĐƯỢC'),
    ('failed',      'lỗi tải'),
]


# ── Tiện ích ────────────────────────────────────────────────────────────────

def _q(db_path: str, sql: str, args: tuple = ()) -> list[tuple]:
    """Query an toàn: DB thiếu/khóa/thiếu cột → trả rỗng, không làm sập báo cáo."""
    if not os.path.exists(db_path):
        return []
    try:
        # read-only để không bao giờ ghi vào DB đang được scheduler dùng
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=10)
        try:
            return list(conn.execute(sql, args))
        finally:
            conn.close()
    except Exception as e:
        print(f'  ! query lỗi ({os.path.basename(db_path)}): {e}', file=sys.stderr)
        return []


def _age(ts, now: datetime) -> str:
    """'2h trước' / '6d trước' — khoảng cách từ mốc thời gian tới `now`."""
    dt = _parse_ts(ts)
    if dt is None:
        return '—'
    secs = (now - dt).total_seconds()
    if secs < 0:
        return 'vừa xong'
    if secs < 3600:
        return f'{int(secs // 60)}m trước'
    if secs < 86400:
        return f'{int(secs // 3600)}h trước'
    return f'{int(secs // 86400)}d trước'


def _age_days(ts, now: datetime) -> float | None:
    dt = _parse_ts(ts)
    return None if dt is None else (now - dt).total_seconds() / 86400


def _cfg_int(name: str) -> str:
    """Đọc 1 hằng số của config để nhét vào câu cảnh báo; lỗi thì trả '?'."""
    try:
        from platform_crawlers import config as cfg
        return str(getattr(cfg, name))
    except Exception:
        return '?'


def _cfg_lc(name: str) -> str:
    """Như _cfg_int nhưng đọc từ platform_crawlers.label_cursor."""
    try:
        from platform_crawlers import label_cursor as lc
        return str(getattr(lc, name))
    except Exception:
        return '?'


def _fmt_dur(seconds: float | None) -> str:
    if not seconds or seconds < 0:
        return '—'
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    if s < 3600:
        return f'{s // 60}m{s % 60:02d}s'
    return f'{s // 3600}h{(s % 3600) // 60:02d}m'


def _fmt_size(nbytes: int) -> str:
    x = float(nbytes)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if x < 1024 or unit == 'TB':
            return f'{x:.1f} {unit}'
        x /= 1024
    return f'{x:.1f} TB'


def _parse_ts(value) -> datetime | None:
    """SQLite trả TEXT; cả 2 DB đều lưu UTC naive."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value).replace('T', ' ').split('+')[0].strip()
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _table(headers: list[str], rows: list[list], align_right_from: int = 1,
           left_cols: tuple = ()) -> list[str]:
    if not rows:
        return ['_(không có dữ liệu)_', '']
    sep = []
    for i in range(len(headers)):
        right = i >= align_right_from and i not in left_cols
        sep.append('---:' if right else ':---')
    out = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(sep) + ' |']
    for r in rows:
        out.append('| ' + ' | '.join('' if c is None else str(c) for c in r) + ' |')
    out.append('')
    return out


# ── Thu thập số liệu ────────────────────────────────────────────────────────

def collect_runs(since: str) -> tuple[list[dict], dict]:
    """Các lượt chạy task crawler trong cửa sổ + tổng hợp thời gian chiếm slot."""
    rows = _q(AIRFLOW_DB, """
        SELECT dag_id, task_id, run_id, state, start_date, end_date, try_number, pool
        FROM task_instance
        WHERE pool = ?
          AND start_date IS NOT NULL
          AND start_date >= ?
        ORDER BY start_date
    """, (GPU_POOL, since))

    runs = []
    for dag_id, task_id, run_id, state, start, end, tries, pool in rows:
        s, e = _parse_ts(start), _parse_ts(end)
        runs.append({
            'dag_id': dag_id, 'task_id': task_id, 'run_id': run_id,
            'state': state or '(None)', 'start': s, 'end': e,
            'tries': tries,
            'duration': (e - s).total_seconds() if s and e else None,
        })

    busy = sum(r['duration'] or 0 for r in runs)
    return runs, {'busy_seconds': busy}


def collect_xcom(since: str) -> dict[str, dict]:
    """Funnel dedup gộp theo platform, lấy từ giá trị task trả về (chính xác)."""
    rows = _q(AIRFLOW_DB, """
        SELECT x.dag_id, x.value, x.timestamp
        FROM xcom x
        WHERE x.key = 'return_value' AND x.timestamp >= ?
    """, (since,))

    agg: dict[str, dict] = defaultdict(lambda: defaultdict(int))
    for dag_id, value, _ts in rows:
        if dag_id not in CRAWLER_DAGS:
            continue
        data = _decode_xcom(value)
        if not isinstance(data, dict):
            continue
        platform = data.get('platform') or dag_id
        bucket = agg[platform]
        bucket['runs'] += 1
        if data.get('time_budget_hit'):
            bucket['budget_hit'] += 1
        if data.get('skipped'):
            bucket['skipped'] += 1
        for key, _label in FUNNEL:
            bucket[key] += int(data.get(key) or 0)
        bucket['error'] += int(data.get('error') or 0)
        for label, n in (data.get('by_category') or {}).items():
            bucket[f'cat:{label}'] += int(n)
    return {k: dict(v) for k, v in agg.items()}


def _decode_xcom(value):
    """XCom Airflow 3 = JSON bytes; bản cũ có thể là repr dict Python."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode('utf-8', 'replace')
        except Exception:
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        return ast.literal_eval(text)
    except Exception:
        return None


def collect_db(since: str) -> dict:
    """Kết quả trong tracker.db — trong cửa sổ và tích lũy."""
    out: dict = {}

    out['status_window'] = _q(TRACKER_DB, """
        SELECT COALESCE(source_platform,'(chưa gán)'), status, COUNT(*)
        FROM video_urls WHERE updated_at >= ? GROUP BY 1,2 ORDER BY 1,3 DESC
    """, (since,))

    out['status_total'] = _q(TRACKER_DB, """
        SELECT COALESCE(source_platform,'(chưa gán)'), status, COUNT(*)
        FROM video_urls GROUP BY 1,2 ORDER BY 1,3 DESC
    """)

    out['label_window'] = _q(TRACKER_DB, """
        SELECT final_category, COUNT(*) FROM video_urls
        WHERE status='downloaded' AND final_category IS NOT NULL AND updated_at >= ?
        GROUP BY 1 ORDER BY 2 DESC
    """, (since,))

    out['label_total'] = _q(TRACKER_DB, """
        SELECT final_category, COUNT(*) FROM video_urls
        WHERE status='downloaded' AND final_category IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)

    # sources.category = nhãn đã được ĐỤNG TỚI (dù có tải được video hay không)
    # → dùng để kiểm tra con trỏ xoay vòng có thực sự đi qua nhãn mới hay không.
    out['labels_touched'] = _q(TRACKER_DB, """
        SELECT COALESCE(source_type,'?'), COALESCE(category,'?'), COUNT(*), MAX(last_scraped)
        FROM sources WHERE last_scraped >= ? GROUP BY 1,2 ORDER BY 1,2
    """, (since,))

    # ── Lịch sử SEARCH theo nhãn (không giới hạn cửa sổ) ──────────────────
    # Trả lời câu "nhãn này 0 video vì chưa từng được search, hay vì search rồi
    # mà không ra gì?" — hai nguyên nhân cần hành động NGƯỢC NHAU:
    #   chưa từng search  → lỗi phân phối lượt (con trỏ nhãn), sửa scheduler
    #   search rồi, 0 video → lỗi nội dung/VLM/tải, sửa từ khóa hoặc pipeline
    # ⚠ sources.url là UNIQUE và add_source() dùng INSERT OR IGNORE, nên COUNT(*)
    # là số NGUỒN RIÊNG BIỆT (platform:nhãn:từ-khóa), KHÔNG phải số lần chạy
    # search. Từ khóa do Gemini sinh mới mỗi lượt nên con số này tỉ lệ với công
    # sức đã bỏ ra, nhưng đừng đọc nó thành "số lượt".
    out['label_search'] = _q(TRACKER_DB, """
        SELECT category, COUNT(*), COUNT(DISTINCT source_type),
               GROUP_CONCAT(DISTINCT source_type), MAX(last_scraped)
        FROM sources WHERE category IS NOT NULL AND category != '' GROUP BY 1
    """)

    out['label_last_video'] = _q(TRACKER_DB, """
        SELECT final_category, MAX(updated_at) FROM video_urls
        WHERE status='downloaded' AND final_category IS NOT NULL GROUP BY 1
    """)

    out['fp_window'] = _q(TRACKER_DB, """
        SELECT COALESCE(source_platform,'(chưa gán)'), COUNT(*)
        FROM video_fingerprints WHERE created_at >= ? GROUP BY 1 ORDER BY 2 DESC
    """, (since,))

    out['fp_total'] = _q(TRACKER_DB,
                         'SELECT COUNT(*) FROM video_fingerprints')
    return out


def collect_disk() -> list[tuple[str, int, int]]:
    """(nhãn, số file, tổng bytes) cho từng thư mục data/videos/<nhãn>/."""
    result = []
    if not os.path.isdir(VIDEO_DIR):
        return result
    for name in sorted(os.listdir(VIDEO_DIR)):
        path = os.path.join(VIDEO_DIR, name)
        if not os.path.isdir(path):
            continue
        count = total = 0
        for root, _dirs, files in os.walk(path):
            for f in files:
                if f.startswith('.'):
                    continue
                try:
                    total += os.path.getsize(os.path.join(root, f))
                    count += 1
                except OSError:
                    pass
        result.append((name, count, total))
    return result


_LOG_PATTERNS = [
    ('discovery quá hạn (đã giữ kết quả dở dang)', re.compile(r'GIỮ LẠI \d+')),
    ('cookie hỏng / bị đá về đăng nhập',           re.compile(r'đá về đăng nhập|cookie hỏng')),
    ('rate limit 429',                             re.compile(r'429')),
    ('Playwright lỗi',                             re.compile(r'Playwright lỗi')),
    ('hết time budget',                            re.compile(r'Hết time budget')),
    ('bỏ qua discovery vì nhãn đủ quota',          re.compile(r'đã đủ quota — bỏ qua')),
]


def collect_log_signals(since_dt: datetime) -> list[tuple[str, int]]:
    """Đếm tín hiệu trong log task crawler, chỉ các file sửa sau `since_dt`."""
    counts = defaultdict(int)
    if not os.path.isdir(LOGS_DIR):
        return []
    cutoff = since_dt.replace(tzinfo=timezone.utc).timestamp()
    for dag in CRAWLER_DAGS:
        base = os.path.join(LOGS_DIR, f'dag_id={dag}')
        if not os.path.isdir(base):
            continue
        for root, _dirs, files in os.walk(base):
            for f in files:
                if not f.endswith('.log'):
                    continue
                p = os.path.join(root, f)
                try:
                    if os.path.getmtime(p) < cutoff:
                        continue
                    with open(p, errors='replace') as fh:
                        text = fh.read()
                except OSError:
                    continue
                for name, rx in _LOG_PATTERNS:
                    n = len(rx.findall(text))
                    if n:
                        counts[name] += n
    return sorted(counts.items(), key=lambda x: -x[1])


# ── Dựng báo cáo ────────────────────────────────────────────────────────────

def build_report(hours: int) -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    since_dt = now - timedelta(hours=hours)
    since = since_dt.strftime('%Y-%m-%d %H:%M:%S')

    runs, slot = collect_runs(since)
    xcom = collect_xcom(since)
    dbd = collect_db(since)
    disk = collect_disk()
    signals = collect_log_signals(since_dt)

    try:
        from platform_crawlers import labels as L
        all_labels = list(L.ALL_LABELS)
    except Exception:
        all_labels = []
    # cursors = {platform: nhãn đang làm}, backoffs = {platform: {nhãn: giờ còn lại}}
    cursors, backoffs = {}, {}
    try:
        from platform_crawlers import label_cursor
        for dag in CRAWLER_DAGS:
            p = dag.replace('social_crawler_', '')
            k = 'facebook' if p == 'fb' else p
            cursors[k] = label_cursor.peek(k)
            backoffs[k] = label_cursor.backoff_state(k)
    except Exception as e:
        print(f'  ! không đọc được label_cursor: {e}', file=sys.stderr)
    try:
        from platform_crawlers import config as cfg
    except Exception:
        cfg = None

    out: list[str] = []
    A = out.append

    A(f'# Báo cáo crawler — {hours}h gần nhất')
    A('')
    A(f'- Cửa sổ (UTC): `{since}` → `{now.strftime("%Y-%m-%d %H:%M:%S")}`')
    A(f'- Sinh lúc: {now.strftime("%Y-%m-%d %H:%M:%S")} UTC')
    A(f'- Nguồn: `{os.path.relpath(AIRFLOW_DB, REPO_ROOT)}`, '
      f'`{os.path.relpath(TRACKER_DB, REPO_ROOT)}`')
    A('')

    # ── 1. Vòng xoay ────────────────────────────────────────────────────
    A('## 1. Vòng xoay DAG (pool `social_crawler_gpu`, 1 slot)')
    A('')
    window_seconds = hours * 3600
    busy = slot['busy_seconds']
    idle = max(0.0, window_seconds - busy)
    A(f'- Tổng thời gian slot GPU bị chiếm: **{_fmt_dur(busy)}** '
      f'/ {_fmt_dur(window_seconds)} (**{busy / window_seconds * 100:.0f}%**)')
    A(f'- Máy rảnh (không crawler nào chạy): {_fmt_dur(idle)}')
    A(f'- Số lượt task: **{len(runs)}**')
    A('')
    A('Slot bị chiếm càng gần 100% thì vòng xoay càng liên tục. Rảnh nhiều mà '
      'DAG vẫn unpause ⇒ xem mục Cảnh báo (scheduler tắt, task zombie giữ slot, '
      'hoặc DAG bị skip vì thiếu cookie).')
    A('')

    per_dag: dict[str, list[dict]] = defaultdict(list)
    for r in runs:
        per_dag[r['dag_id']].append(r)

    rows = []
    for dag in CRAWLER_DAGS:
        rs = per_dag.get(dag, [])
        durs = [r['duration'] for r in rs if r['duration']]
        states = defaultdict(int)
        for r in rs:
            states[r['state']] += 1
        rows.append([
            dag.replace('social_crawler_', ''),
            len(rs),
            ', '.join(f'{k}:{v}' for k, v in sorted(states.items())) or '—',
            _fmt_dur(sum(durs)) if durs else '—',
            _fmt_dur(max(durs)) if durs else '—',
            _fmt_dur(sum(durs) / len(durs)) if durs else '—',
            f'{sum(durs) / busy * 100:.0f}%' if durs and busy else '—',
        ])
    out += _table(
        ['platform', 'lượt', 'state', 'tổng giữ slot', 'lượt dài nhất',
         'trung bình', '% slot'],
        rows, left_cols=(2,),
    )
    A('Cột `% slot` cho thấy có platform nào độc chiếm không. Lệch nhiều thì '
      'giảm `CRAWL_TASK_TIME_BUDGET_MINUTES`.')
    A('')

    # ── 2. Funnel ───────────────────────────────────────────────────────
    A('## 2. Kết quả crawl (funnel dedup 4 tầng)')
    A('')
    if xcom:
        headers = ['platform', 'lượt', 'hết budget'] + [lbl for _k, lbl in FUNNEL]
        rows = []
        totals = defaultdict(int)
        for platform in sorted(xcom):
            d = xcom[platform]
            row = [platform, d.get('runs', 0), d.get('budget_hit', 0)]
            for key, _lbl in FUNNEL:
                v = d.get(key, 0)
                row.append(v)
                totals[key] += v
            rows.append(row)
            totals['runs'] += d.get('runs', 0)
            totals['budget_hit'] += d.get('budget_hit', 0)
        rows.append(['**TỔNG**', totals['runs'], totals['budget_hit']]
                    + [totals[k] for k, _ in FUNNEL])
        out += _table(headers, rows)

        seen_t, dl_t = totals['seen'], totals['downloaded']
        A(f'- Tỷ lệ tải được / URL nhận: **{dl_t}/{seen_t}** '
          f'({dl_t / seen_t * 100:.1f}%)' if seen_t else '- Chưa có URL nào.')
        dup_t = sum(totals[k] for k in ('dup_url', 'dup_l1', 'dup_l2', 'dup_l3', 'dup_l4'))
        if seen_t:
            A(f'- Bị dedup chặn: {dup_t} ({dup_t / seen_t * 100:.1f}%) — '
              f'L0 {totals["dup_url"]}, L1 {totals["dup_l1"]}, L2 {totals["dup_l2"]}, '
              f'L3 {totals["dup_l3"]}, L4 {totals["dup_l4"]}')
            A('- L0 chiếm gần hết là BÌNH THƯỜNG khi vòng xoay chạy lâu: cùng từ '
              'khóa sẽ ra lại URL cũ. Nhưng L0 ~100% liên tục ⇒ discovery không '
              'còn tìm được gì mới, cần đổi/mở rộng từ khóa hoặc cuộn sâu hơn '
              '(`CRAWL_MAX_PER_TARGET`).')
        A('')
    else:
        A('_Không có XCom nào trong cửa sổ — chưa có lượt task nào chạy xong._')
        A('')

    # ── 3. Cân bằng nhãn ────────────────────────────────────────────────
    A('## 3. Cân bằng 11 nhãn')
    A('')
    win = dict(dbd['label_window'])
    tot = dict(dbd['label_total'])
    # label → (số nguồn, số platform, tên platform, lần search cuối)
    srch = {r[0]: r[1:] for r in dbd['label_search']}
    lastvid = dict(dbd['label_last_video'])

    rows = []
    for label in (all_labels or sorted(set(win) | set(tot))):
        n_src, n_plat, plats, last_s = srch.get(label, (0, 0, '', None))
        rows.append([
            label,
            win.get(label, 0),
            tot.get(label, 0),
            n_src,
            n_plat,
            _age(last_s, now),
            _age(lastvid.get(label), now),
            '' if tot.get(label) else '⚠ TRẮNG',
        ])
    out += _table(
        ['nhãn', f'{hours}h qua', 'tích lũy', 'nguồn đã search', 'số platform',
         'search cuối', 'có video cuối', ''],
        rows, left_cols=(5, 6, 7),
    )
    A('`nguồn đã search` = số **nguồn riêng biệt** `platform:nhãn:từ-khóa` từng '
      'được lưu (bảng `sources`, `INSERT OR IGNORE` nên KHÔNG phải số lần chạy '
      'search). Từ khóa do Gemini sinh mới mỗi lượt nên con số này tỉ lệ với công '
      'sức đã bỏ vào nhãn đó.')
    A('')
    A('`số platform` chỉ đáng ngại khi ĐI KÈM 0 video: **1 platform + 0 video** '
      'nghĩa là nhãn chưa hề được thử thật sự (nếu platform đó lúc ấy đang hỏng — '
      'cookie chết, pool URL cạn — thì coi như chưa thử). Ngược lại 1 platform mà '
      'nhiều video là bình thường: nhãn đó đang do đúng 1 platform phụ trách '
      '(ví dụ Robbery ↔ dailymotion), chỉ nói lên độ phủ hẹp, không phải lỗi.')
    A('')

    # ── Chẩn đoán từng nhãn yếu ─────────────────────────────────────────
    weak = [l for l in (all_labels or [])
            if not tot.get(l) or not win.get(l)]
    if weak:
        A('### Chẩn đoán các nhãn không có video')
        A('')
        A('Hai nguyên nhân dưới đây cần hành động NGƯỢC NHAU — đừng sửa từ khóa '
          'khi thật ra nhãn chưa từng được search.')
        A('')
        for label in weak:
            n_src, n_plat, plats, last_s = srch.get(label, (0, 0, '', None))
            age_d = _age_days(last_s, now)
            n_tot, n_win = tot.get(label, 0), win.get(label, 0)
            if not n_src:
                why = ('**CHƯA TỪNG được search** → lỗi phân phối lượt '
                       '(con trỏ nhãn), không phải lỗi từ khóa hay nội dung.')
            elif n_plat <= 1:
                why = (f'chỉ **{n_plat} platform** (`{plats}`) từng search, '
                       f'lần cuối {_age(last_s, now)} → chưa được thử thật sự; '
                       f'các platform khác chưa hề chạm tới nhãn này.')
            elif age_d is not None and age_d >= 1:
                why = (f'đã {_age(last_s, now)} không được search '
                       f'(có {n_src} nguồn từ {n_plat} platform) → con trỏ nhãn '
                       f'không quay lại; kiểm tra cảnh báo "con trỏ không nhích".')
            elif not n_tot:
                why = (f'ĐÃ search gần đây ({n_src} nguồn, {n_plat} platform) '
                       f'mà vẫn **0 video** → đây mới là vấn đề nội dung/VLM/tải. '
                       f'Soi cột `loại: không CCTV` và `lỗi tải` ở mục 2.')
            else:
                why = (f'có {n_tot} video tích lũy nhưng **0 trong {hours}h qua** '
                       f'(search cuối {_age(last_s, now)}, video cuối '
                       f'{_age(lastvid.get(label), now)}).')
            A(f'- **{label}**: {why}')
        A('')

    empty = [l for l in (all_labels or []) if not tot.get(l)]
    if empty:
        A(f'⚠ **{len(empty)}/{len(all_labels)} nhãn vẫn chưa có video nào:** '
          + ', '.join(empty))
        A('')
        A('Nhãn 0 video được cơ chế chọn nhãn ưu tiên CAO NHẤT, nên danh sách này '
          'phải ngắn lại sau 1-2 ngày. Nếu không: kiểm tra `CRAWL_LABEL_ROTATION` '
          'có bị đặt `false`, và cột `đang hạ ưu tiên` bên dưới (nhãn bị backoff '
          'liên tục = đã thử nhiều lần mà không ra video → vấn đề nội dung/từ '
          'khóa, không phải vấn đề lịch).')
    else:
        A('✅ Cả 11 nhãn đều đã có video.')
    A('')

    A('### Thứ tự nhãn — trạng thái cơ chế chọn nhãn')
    A('')
    rows = []
    touched: dict[str, set] = defaultdict(set)
    for platform, category, _n, _last in dbd['labels_touched']:
        if category and category != '?':
            touched[platform].add(category)
    for dag in CRAWLER_DAGS:
        p = dag.replace('social_crawler_', '')
        key = 'facebook' if p == 'fb' else p
        held = backoffs.get(key) or {}
        rows.append([
            key,
            cursors.get(key) or '(chưa có)',
            ', '.join(f'{k} ({v}h)' for k, v in sorted(held.items())) or '—',
            len(touched.get(key, ())),
            ', '.join(sorted(touched.get(key, ()))) or '—',
        ])
    out += _table(
        ['platform', 'nhãn đang/vừa làm', 'đang hạ ưu tiên', 'số nhãn đã đụng',
         'nhãn đã đụng trong cửa sổ'],
        rows, left_cols=(1, 2, 4),
    )
    A('Thứ tự nhãn KHÔNG còn cố định và cũng không còn là vòng tròn: mỗi lượt '
      '`label_cursor.order_labels()` tính lại từ số video thật trong DB — **nhãn '
      'thiếu nhất đi trước** (xem bảng "tích lũy" ở trên để biết thứ tự dự kiến). '
      'Vì vậy không còn con trỏ nào có thể kẹt: hết giờ giữa nhãn thì lượt sau '
      'nhãn đó vẫn đang thiếu nên vẫn được ưu tiên.')
    A('')
    A('`đang hạ ưu tiên` = nhãn bị đẩy xuống CUỐI thứ tự vì '
      f'≥{_cfg_lc("EMPTY_STREAK_LIMIT")} lượt ghé liên tiếp không ra video nào — '
      'chốt an toàn để 1 nhãn vô sản không chiếm slot mãi. Nhãn vẫn được thử nếu '
      'lượt còn thời gian, và được reset ngay khi ra được 1 video. Thấy một nhãn '
      'ở đây nhiều ngày liền ⇒ platform đó có thể không có loại nội dung đó, '
      'cân nhắc sửa từ khóa trong `platform_crawlers/labels.py`.')
    A('')
    A('"Nhãn đã đụng" đếm từ bảng `sources` — nhãn có được search, KỂ CẢ khi '
      'không tải được video nào.')
    A('')

    # ── 4. URL theo status ──────────────────────────────────────────────
    A('## 4. URL theo trạng thái')
    A('')
    A(f'### Trong {hours}h qua')
    A('')
    out += _table(['platform', 'status', 'số URL'],
                  [list(r) for r in dbd['status_window']], align_right_from=2)
    A('### Tích lũy toàn bộ')
    A('')
    out += _table(['platform', 'status', 'số URL'],
                  [list(r) for r in dbd['status_total']], align_right_from=2)
    A('⚠ `skipped_not_cctv` là TRẠNG THÁI CUỐI (`_TERMINAL_STATUSES` trong '
      'crawler_core/pipeline.py): URL bị VLM đánh sai một lần sẽ bị L0 chặn '
      'vĩnh viễn, không bao giờ thử lại. `failed` thì KHÔNG cuối — sẽ được thử lại.')
    A('')

    fp_total = dbd['fp_total'][0][0] if dbd['fp_total'] else 0
    A(f'Fingerprint: **{fp_total}** tổng cộng, '
      f'thêm mới trong cửa sổ: '
      f'{sum(n for _p, n in dbd["fp_window"]) if dbd["fp_window"] else 0}')
    A('')
    out += _table(['platform', 'fingerprint mới'],
                  [list(r) for r in dbd['fp_window']])

    # ── 5. Ổ đĩa ────────────────────────────────────────────────────────
    A('## 5. Video trên ổ đĩa')
    A('')
    if disk:
        rows = [[n, c, _fmt_size(s)] for n, c, s in disk if c]
        out += _table(['thư mục (nhãn)', 'số file', 'dung lượng'], rows)
        A(f'Tổng: **{sum(c for _n, c, _s in disk)} file**, '
          f'**{_fmt_size(sum(s for _n, _c, s in disk))}**')
    else:
        A(f'_Không đọc được `{os.path.relpath(VIDEO_DIR, REPO_ROOT)}`_')
    A('')

    # ── 6. Cảnh báo ─────────────────────────────────────────────────────
    A('## 6. Cảnh báo tự phát hiện')
    A('')
    warns = _detect_warnings(runs, xcom, busy, window_seconds, empty, all_labels,
                             touched, cursors)
    if warns:
        for w in warns:
            A(f'- {w}')
    else:
        A('Không phát hiện vấn đề nào.')
    A('')

    if signals:
        A('### Tín hiệu đếm trong log')
        A('')
        out += _table(['tín hiệu', 'số lần'], [list(s) for s in signals])

    # ── 7. Cấu hình ─────────────────────────────────────────────────────
    A('## 7. Cấu hình lúc sinh báo cáo')
    A('')
    if cfg:
        knobs = [
            ('CRAWL_MAX_PER_TARGET', cfg.MAX_PER_TARGET, 'trần URL/từ khóa — cũng là ĐỘ SÂU CUỘN'),
            ('CRAWL_MAX_PER_LABEL', cfg.MAX_PER_LABEL, 'trần URL/nhãn mỗi lượt'),
            ('CRAWL_MAX_PER_PLATFORM', cfg.MAX_PER_PLATFORM, 'trần URL/lượt'),
            ('CRAWL_MAX_SCROLLS', cfg.MAX_SCROLLS, 'số lần cuộn tối đa'),
            ('CRAWL_FACEBOOK_MAX_SCROLLS', cfg.FACEBOOK_MAX_SCROLLS, 'riêng Facebook'),
            ('CRAWL_DAILYMOTION_MAX_PAGES', cfg.DAILYMOTION_MAX_PAGES, 'Dailymotion phân trang, không cuộn'),
            ('CRAWL_DISCOVERY_TIMEOUT', cfg.DISCOVERY_TIMEOUT, 'trần giây/lượt discovery'),
            ('CRAWL_TASK_TIME_BUDGET_MINUTES', cfg.TASK_TIME_BUDGET_MINUTES,
             'trần phút/lượt (0 = không giới hạn ⇒ platform chậm sẽ độc chiếm slot)'),
            ('CRAWL_KEYWORDS_PER_LABEL', cfg.KEYWORDS_PER_LABEL, 'số từ khóa/nhãn'),
        ]
        out += _table(['env', 'giá trị', 'ý nghĩa'],
                      [[f'`{k}`', v, d] for k, v, d in knobs], align_right_from=1)
    else:
        A('_Không import được platform_crawlers.config_')
        A('')

    return '\n'.join(out) + '\n'


def _detect_warnings(runs, xcom, busy, window_seconds, empty_labels, all_labels,
                     touched=None, cursors=None) -> list[str]:
    warns = []
    touched = touched or {}

    # Con trỏ nhãn KHÔNG NHÍCH — chạy nhiều lượt mà vẫn quay quanh 1-2 nhãn.
    # Đây là bệnh đã xảy ra 2026-08-05: MAX_PER_LABEL cao hơn số URL 1 lượt xử
    # lý nổi ⇒ nhãn không bao giờ đạt quota ⇒ con trỏ đứng yên ⇒ 6/11 nhãn trắng.
    per_dag_runs: dict[str, int] = defaultdict(int)
    for r in runs:
        per_dag_runs[r['dag_id']] += 1
    for dag_id, n in sorted(per_dag_runs.items()):
        p = dag_id.replace('social_crawler_', '')
        key = 'facebook' if p == 'fb' else p
        n_labels = len(touched.get(key, ()))
        if n >= 3 and n_labels <= 1:
            warns.append(
                f'**`{key}` chạy {n} lượt mà chỉ đụng {n_labels} nhãn** ⇒ các nhãn '
                f'khác không tới lượt. Nguyên nhân thường gặp: '
                f'`CRAWL_MAX_PER_LABEL` lớn hơn số URL 1 lượt xử lý nổi trong '
                f'`CRAWL_TASK_TIME_BUDGET_MINUTES` (VLM ~48s/URL ⇒ ~75 URL/60 phút), '
                f'nên 1 nhãn ăn hết cả lượt. Hạ MAX_PER_LABEL để mỗi lượt chia được '
                f'cho nhiều nhãn. Đối chiếu log `[LabelCursor] {key}: thứ tự lượt '
                f'này ...` của 2 lượt liên tiếp. '
                f'(MAX_PER_LABEL lúc SINH BÁO CÁO: {_cfg_int("MAX_PER_LABEL")}; '
                f'nhãn đang làm: `{(cursors or {}).get(key) or "?"}` — cả hai có thể '
                f'đã khác giá trị trong cửa sổ.)'
            )

    # Task zombie: đang running/queued mà tiến trình airflow không tồn tại
    live = _q(AIRFLOW_DB, """
        SELECT dag_id, task_id, state, start_date FROM task_instance
        WHERE pool = ? AND state IN ('running','queued','restarting')
    """, (GPU_POOL,))
    if live:
        any_proc = False
        try:
            any_proc = bool(os.popen("pgrep -f 'airflow' | head -1").read().strip())
        except Exception:
            any_proc = True
        for dag_id, task_id, state, start in live:
            if state == 'running' and not any_proc:
                warns.append(
                    f'**Task ZOMBIE giữ slot GPU:** `{dag_id}/{task_id}` state=`{state}` '
                    f'từ `{start}` nhưng không có tiến trình airflow nào. Mọi crawler '
                    f'sẽ kẹt `queued` — dọn theo mục 1.6 của skill youtube-dag.'
                )
            else:
                warns.append(f'Đang chiếm slot: `{dag_id}/{task_id}` state=`{state}` từ `{start}`.')

    # DAG live cũng dùng pool GPU và giữ 12h
    livedag = _q(AIRFLOW_DB, """
        SELECT is_paused FROM dag WHERE dag_id = 'camera_source_youtube_live'
    """)
    if livedag and not livedag[0][0]:
        warns.append(
            '`camera_source_youtube_live` đang BẬT và cũng dùng pool `social_crawler_gpu`, '
            'giữ slot **liên tục 12h (18h→6h)** ⇒ nửa ngày không platform nào crawl được. '
            'Pause nó nếu ưu tiên crawl VOD: `airflow dags pause camera_source_youtube_live`.'
        )

    # DAG còn pause
    paused = _q(AIRFLOW_DB,
                f"SELECT dag_id FROM dag WHERE is_paused = 1 AND dag_id IN "
                f"({','.join('?' * len(CRAWLER_DAGS))})", tuple(CRAWLER_DAGS))
    for (dag_id,) in paused:
        warns.append(f'`{dag_id}` đang **PAUSE** → không tham gia vòng xoay. '
                     f'Bật: `airflow dags unpause {dag_id}`.')

    # DAG chưa từng được scheduler parse
    known = {r[0] for r in _q(AIRFLOW_DB, 'SELECT dag_id FROM dag')}
    for dag in CRAWLER_DAGS:
        if dag not in known:
            warns.append(f'`{dag}` chưa xuất hiện trong DB Airflow — scheduler chưa parse '
                         f'file DAG. Chờ ~30s sau khi start, rồi `airflow dags unpause {dag}`.')

    # Slot rảnh nhiều
    if window_seconds and busy / window_seconds < 0.5:
        warns.append(
            f'Slot GPU chỉ được dùng **{busy / window_seconds * 100:.0f}%** thời gian — '
            f'vòng xoay đang hở. Kiểm tra scheduler còn chạy không (`pgrep -af airflow`) '
            f'và các DAG đã unpause chưa.'
        )

    # Platform bị skip (thiếu cookie...) hoặc không tải được gì
    for platform, d in sorted(xcom.items()):
        if d.get('skipped'):
            warns.append(f'`{platform}`: **{d["skipped"]}/{d["runs"]}** lượt bị SKIP '
                         f'(thường là thiếu/hết hạn cookie) — xem log lượt gần nhất.')
        elif d.get('runs') and not d.get('downloaded'):
            warns.append(f'`{platform}`: chạy {d["runs"]} lượt, nhận {d.get("seen", 0)} URL '
                         f'nhưng **tải được 0 video**.')

    # Không lượt nào hết budget mà nhãn vẫn trắng ⇒ discovery mới là điểm nghẽn
    if empty_labels and xcom and not any(d.get('budget_hit') for d in xcom.values()):
        warns.append(
            f'Không lượt nào hết time budget nhưng vẫn còn {len(empty_labels)} nhãn trắng ⇒ '
            f'điểm nghẽn KHÔNG phải thời gian mà là discovery không tìm được video cho '
            f'những nhãn đó (từ khóa quá hẹp, hoặc platform không có loại nội dung này).'
        )

    # Task lỗi
    failed = [r for r in runs if r['state'] in ('failed', 'up_for_retry')]
    if failed:
        by = defaultdict(int)
        for r in failed:
            by[r['dag_id']] += 1
        warns.append('Lượt lỗi: ' + ', '.join(f'`{k}`×{v}' for k, v in sorted(by.items())))

    return warns


# ── main ────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description='Thống kê sau 24h chạy DAG crawler')
    ap.add_argument('--hours', type=int, default=24, help='cửa sổ thống kê (mặc định 24)')
    ap.add_argument('--out', help='đường dẫn file .md (mặc định reports/crawl_report_<time>.md)')
    ap.add_argument('--stdout', action='store_true', help='in cả báo cáo ra terminal')
    args = ap.parse_args()

    report = build_report(args.hours)

    out_path = args.out
    if not out_path:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')
        out_path = os.path.join(REPO_ROOT, 'reports', f'crawl_report_{stamp}.md')
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w') as f:
        f.write(report)

    if args.stdout:
        print(report)
    else:
        # Digest ngắn để không phải mở file mới biết có gì đáng lo
        keep = False
        for line in report.splitlines():
            if line.startswith('## 6.'):
                keep = True
            elif line.startswith('## 7.'):
                keep = False
            elif keep and line.strip():
                print(line)
    print(f'\n→ Báo cáo đầy đủ: {out_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
