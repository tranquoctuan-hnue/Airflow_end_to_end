"""
SQLite tracker dùng chung cho MỌI platform.

Bảng:
  video_urls          — mọi URL đã gặp + status + file_path (URL-level dedup)
  sources             — nhóm/kênh/subreddit đã scan
  video_fingerprints  — fingerprint 4 tầng (L1..L4), dùng chung mọi platform

DB path: data/db/tracker.db (không vào git — chép sang máy mới để giữ lịch sử dedup,
28.663 URL / 6.361 fingerprint ngày 2026-09-28). Override bằng env CRAWLER_DB_PATH.
Trước 2026-09-28 DB nằm ở facebook_crawler/db/tracker.db.
"""

import os
import sqlite3
import logging

logger = logging.getLogger(__name__)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_DEFAULT_DB_PATH = os.path.join(REPO_ROOT, 'data', 'db', 'tracker.db')
_LEGACY_DB_PATH = os.path.join(REPO_ROOT, 'facebook_crawler', 'db', 'tracker.db')
DB_PATH = os.environ.get('CRAWLER_DB_PATH', _DEFAULT_DB_PATH)


def _check_legacy_location(db_path):
    """DB mới chưa có mà vị trí cũ còn DB → dừng hẳn. Nếu cứ tạo DB rỗng thì L0–L4
    quên sạch lịch sử và tải lại hàng nghìn video đã có — lỗi âm thầm, không báo gì."""
    if (os.path.abspath(db_path) == _DEFAULT_DB_PATH and not os.path.exists(db_path)
            and os.path.exists(_LEGACY_DB_PATH)):
        raise RuntimeError(
            f'tracker.db đã chuyển chỗ: chạy  mkdir -p data/db && '
            f'mv facebook_crawler/db/tracker.db data/db/  (hoặc đặt CRAWLER_DB_PATH)')


class DBManager:
    def __init__(self, db_path=None):
        db_path = db_path or DB_PATH
        _check_legacy_location(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    # ── Schema ─────────────────────────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS video_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    category_hint TEXT,
                    caption TEXT,
                    source_page TEXT,
                    status TEXT DEFAULT 'pending',
                    file_path TEXT,
                    final_category TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    source_type TEXT,
                    category TEXT,
                    last_scraped TEXT,
                    scan_complete INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Fingerprint 4 tầng — dùng chung mọi platform.
            # L1 = (source_platform, canonical_object_type, canonical_object_id)
            # L2 = file_hash | L3 = thumbnail_hash | L4 = fingerprint
            conn.execute('''
                CREATE TABLE IF NOT EXISTS video_fingerprints (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    url                    TEXT,
                    source_platform        TEXT,
                    canonical_object_type  TEXT,
                    canonical_object_id    TEXT,
                    file_hash              TEXT,
                    thumbnail_hash         TEXT,
                    fingerprint            TEXT,
                    duration               REAL NOT NULL DEFAULT 0,
                    file_path              TEXT,
                    created_at             TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_fp_canonical ON video_fingerprints'
                '(source_platform, canonical_object_type, canonical_object_id)'
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_fp_file_hash '
                'ON video_fingerprints(file_hash)'
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_fp_duration '
                'ON video_fingerprints(duration)'
            )

            # ── Migration an toàn cho DB cũ (SQLite không có DROP COLUMN) ──
            for table, col, typedef in [
                ('sources',            'scan_complete',         'INTEGER DEFAULT 0'),
                ('video_urls',         'source_platform',       'TEXT'),
                ('video_fingerprints', 'source_platform',       'TEXT'),
                ('video_fingerprints', 'canonical_object_type', 'TEXT'),
                ('video_fingerprints', 'canonical_object_id',   'TEXT'),
                ('video_fingerprints', 'file_hash',             'TEXT'),
                ('video_fingerprints', 'thumbnail_hash',        'TEXT'),
                ('video_fingerprints', 'url',                   'TEXT'),
                # Lý do loại (not_cctv | portrait | dup_l1..dup_l4) + JSON kết quả
                # của các bộ lọc đã chạy (cả video được nhận) — xem VideoPipeline.
                ('video_urls',         'reject_reason',         'TEXT'),
                ('video_urls',         'filter_detail',         'TEXT'),
            ]:
                try:
                    conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {typedef}')
                except Exception:
                    pass       # cột đã tồn tại
            conn.commit()

    # ── video_urls ─────────────────────────────────────────────────────

    def add_video_url(self, url: str, category_hint: str = None,
                      caption: str = None, source_page: str = None,
                      source_platform: str = None) -> bool:
        """Trả True nếu URL này chưa từng có trong DB (tầng 0 — URL-level)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'INSERT OR IGNORE INTO video_urls '
                '(url, category_hint, caption, source_page, source_platform) '
                'VALUES (?, ?, ?, ?, ?)',
                (url, category_hint, caption, source_page, source_platform)
            )
            conn.commit()
            return cursor.rowcount > 0

    def get_pending_urls(self, limit: int = 50) -> list:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                'SELECT id, url, category_hint, caption FROM video_urls '
                'WHERE status = ? LIMIT ?',
                ('pending', limit)
            ).fetchall()

    def update_url_status(self, url: str, status: str,
                          file_path: str = None, final_category: str = None,
                          reject_reason: str = None, filter_detail: str = None):
        """
        file_path: với video bị loại được lưu lại (move, xem crawler_reject_action), đây là đường dẫn
            trong thư mục loại (REJECTED_BASE), KHÔNG phải trong dataset.
        filter_detail: chuỗi JSON kết quả các bộ lọc đã chạy.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                '''UPDATE video_urls
                   SET status = ?, file_path = ?, final_category = ?,
                       reject_reason = ?, filter_detail = ?,
                       updated_at = CURRENT_TIMESTAMP
                   WHERE url = ?''',
                (status, file_path, final_category, reject_reason,
                 filter_detail, url)
            )
            conn.commit()

    def url_exists(self, url: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                'SELECT 1 FROM video_urls WHERE url = ?', (url,)
            ).fetchone() is not None

    def get_url_status(self, url: str) -> str | None:
        """Status hiện tại của URL, None nếu chưa có trong DB (dùng cho L0)."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                'SELECT status FROM video_urls WHERE url = ?', (url,)
            ).fetchone()
        return row[0] if row else None

    def get_known_urls_for_source(self, source_url: str) -> set[str]:
        """Tất cả URL đã gặp từ một nhóm/kênh cụ thể."""
        with sqlite3.connect(self.db_path) as conn:
            return {
                row[0] for row in conn.execute(
                    'SELECT url FROM video_urls WHERE source_page = ?',
                    (source_url,)
                )
            }

    # ── sources ────────────────────────────────────────────────────────

    def add_source(self, url: str, source_type: str, category: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'INSERT OR IGNORE INTO sources (url, source_type, category) '
                'VALUES (?, ?, ?)',
                (url, source_type, category)
            )
            conn.commit()
            return cursor.rowcount > 0

    def get_sources_to_scrape(self) -> list:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                '''SELECT url, source_type, category FROM sources
                   WHERE last_scraped IS NULL
                   OR datetime(last_scraped) < datetime("now", "-6 hours")'''
            ).fetchall()

    def update_source_scraped(self, url: str, scan_complete: bool = False):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                '''UPDATE sources
                   SET last_scraped = CURRENT_TIMESTAMP,
                       scan_complete = CASE WHEN ? THEN 1 ELSE scan_complete END
                   WHERE url = ?''',
                (1 if scan_complete else 0, url)
            )
            conn.commit()

    def is_source_complete(self, url: str) -> bool:
        """True nếu nguồn đã được deep scan hết (từng đạt đáy scroll)."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                'SELECT scan_complete FROM sources WHERE url = ?', (url,)
            ).fetchone()
            return bool(row and row[0])

    # ══════════════════════════════════════════════════════════════════
    # Dedup 4 tầng
    # ══════════════════════════════════════════════════════════════════

    def find_by_canonical_id(
        self, platform: str, obj_type: str, obj_id: str
    ) -> str | None:
        """
        L1 — composite lookup (source_platform, object_type, object_id).
        Phân biệt đúng facebook:video:123 vs facebook:reel:123 vs tiktok:video:123.
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                'SELECT url FROM video_fingerprints '
                'WHERE source_platform = ? AND canonical_object_type = ? '
                '  AND canonical_object_id = ? LIMIT 1',
                (platform, obj_type, obj_id),
            ).fetchone()
        return row[0] if row else None

    def find_by_file_hash(self, file_hash: str) -> str | None:
        """L2 — SHA-256 exact match. KHÔNG filter platform → bắt trùng chéo."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                'SELECT url FROM video_fingerprints WHERE file_hash = ? LIMIT 1',
                (file_hash,),
            ).fetchone()
        return row[0] if row else None

    def find_by_thumbnail(self, thumbnail_hash: str, duration: float) -> str | None:
        """L3 — phash frame giữa, pre-filter duration ±5%. Không filter platform."""
        from crawler_core.dedup import (
            phash_distance, THUMBNAIL_THRESHOLD, DURATION_TOLERANCE,
        )
        lo = duration * (1 - DURATION_TOLERANCE)
        hi = duration * (1 + DURATION_TOLERANCE)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                'SELECT thumbnail_hash, url FROM video_fingerprints '
                'WHERE duration BETWEEN ? AND ? AND thumbnail_hash IS NOT NULL',
                (lo, hi),
            ).fetchall()
        for th, url in rows:
            if phash_distance(thumbnail_hash, th) < THUMBNAIL_THRESHOLD:
                return url
        return None

    def find_by_fingerprint(self, fingerprint: str, duration: float) -> str | None:
        """L4 — avg phash distance 5 frame, pre-filter duration ±5%. Không filter platform."""
        from crawler_core.dedup import (
            fingerprint_avg_distance, FINGERPRINT_THRESHOLD, DURATION_TOLERANCE,
        )
        lo = duration * (1 - DURATION_TOLERANCE)
        hi = duration * (1 + DURATION_TOLERANCE)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                'SELECT fingerprint, url FROM video_fingerprints '
                'WHERE duration BETWEEN ? AND ? AND fingerprint IS NOT NULL',
                (lo, hi),
            ).fetchall()
        for fp, url in rows:
            if fingerprint_avg_distance(fingerprint, fp) < FINGERPRINT_THRESHOLD:
                return url
        return None

    def save_video_fingerprint(
        self,
        url: str,
        file_path: str,
        duration: float = 0.0,
        source_platform: str | None = None,
        canonical_object_type: str | None = None,
        canonical_object_id: str | None = None,
        file_hash: str | None = None,
        thumbnail_hash: str | None = None,
        fingerprint: str | None = None,
    ):
        """Lưu cả 4 tầng sau khi xác nhận video là unique."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                '''INSERT INTO video_fingerprints
                   (url, source_platform, canonical_object_type, canonical_object_id,
                    file_hash, thumbnail_hash, fingerprint, duration, file_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (url, source_platform, canonical_object_type, canonical_object_id,
                 file_hash, thumbnail_hash, fingerprint, duration, file_path),
            )
            conn.commit()

    def fingerprint_count(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                'SELECT COUNT(*) FROM video_fingerprints'
            ).fetchone()[0]

    # ── Stats ──────────────────────────────────────────────────────────

    def downloaded_by_label(self) -> dict:
        """
        {nhãn: số video đã tải}. Dùng để CHỌN NHÃN theo thiếu hụt — nhãn ít video
        nhất được crawl trước (xem platform_crawlers/label_cursor.order_labels).

        Đếm trên `video_urls.final_category` (nhãn thực tế lúc lưu file), không
        phải `category_hint`, để phản ánh đúng dataset đang có trên đĩa.
        """
        with sqlite3.connect(self.db_path) as conn:
            return dict(conn.execute(
                'SELECT final_category, COUNT(*) FROM video_urls '
                'WHERE status = "downloaded" AND final_category IS NOT NULL '
                'GROUP BY final_category'
            ).fetchall())

    def get_stats(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            url_stats = dict(conn.execute(
                'SELECT status, COUNT(*) FROM video_urls GROUP BY status'
            ).fetchall())
            category_stats = dict(conn.execute(
                'SELECT final_category, COUNT(*) FROM video_urls '
                'WHERE status = "downloaded" GROUP BY final_category'
            ).fetchall())
            platform_stats = dict(conn.execute(
                'SELECT COALESCE(source_platform, "unknown"), COUNT(*) '
                'FROM video_fingerprints GROUP BY 1'
            ).fetchall())
            return {
                'urls': url_stats,
                'categories': category_stats,
                'platforms': platform_stats,
                'fingerprints': self.fingerprint_count(),
            }
