"""
Tài khoản camera RTSP — KHÔNG viết thẳng vào URL.

File danh sách camera (rtsp/*.txt) chỉ chứa CHỖ GIỮ CHỖ tên tài khoản:

    "rtsp://{rtsp_cam_vh}@203.0.113.10:554/stream0"

{rtsp_cam_vh} là id của một Airflow Connection (Admin → Connections, kiểu Generic,
Login + Password). Connection được MÃ HÓA trong airflow.db bằng fernet_key, sửa trên
web được, không nằm trong file nào. Lúc chạy, load_rtsp_urls() thay chỗ giữ chỗ bằng
login:password và đăng ký mật khẩu với bộ che secret của Airflow → mọi dòng log
(logging lẫn print, kể cả lỗi của OpenCV/ffmpeg) hiện '***' thay cho mật khẩu.

Ngoài Airflow (script, test): đặt env RTSP_CRED_<ID_VIẾT_HOA>="user:password",
ví dụ RTSP_CRED_RTSP_CAM_VH="admin:matkhau".

URL kiểu cũ có mật khẩu viết thẳng (rtsp://user:pass@host) VẪN chạy được, và mật khẩu
vẫn được che trong log — nhưng nên chuyển sang dạng mới bằng
scripts/migrate_rtsp_credentials.py.

Mật khẩu có thể chứa '@' (đã gặp thật): URL được ghép NGUYÊN VĂN, không mã hóa %,
đúng như dạng cũ mà OpenCV/ffmpeg đang đọc được.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# {conn_id}@ ngay sau rtsp://
_PLACEHOLDER = re.compile(r'^(rtsps?://)\{([A-Za-z0-9_]+)\}@')
# user:pass@host — mật khẩu có thể chứa '@', nên lấy tới '@' CUỐI cùng trước host
_INLINE = re.compile(r'^(rtsps?://)([^/]*)@([^@/]+(?:/.*)?)$')

_cache: dict[str, tuple[str, str]] = {}


def _mask(secret: str):
    """Đăng ký secret với bộ che log của Airflow (không có Airflow thì bỏ qua)."""
    if not secret:
        return
    try:
        from airflow.sdk.log import mask_secret
        mask_secret(secret)
    except Exception:
        pass


def get_credentials(conn_id: str) -> tuple[str, str]:
    """(login, password) của một Connection; env RTSP_CRED_<ID> khi chạy ngoài Airflow."""
    if conn_id in _cache:
        return _cache[conn_id]
    env = os.environ.get(f'RTSP_CRED_{conn_id.upper()}')
    if env:
        login, _, password = env.partition(':')
    else:
        try:
            from airflow.sdk import BaseHook
            conn = BaseHook.get_connection(conn_id)
            login, password = conn.login or '', conn.password or ''
        except Exception as e:
            raise RuntimeError(
                f'Không lấy được tài khoản camera "{conn_id}": {type(e).__name__}. '
                f'Tạo Connection {conn_id} (Admin → Connections, Login + Password) '
                f'hoặc đặt env RTSP_CRED_{conn_id.upper()}="user:password"') from e
    _mask(password)
    _cache[conn_id] = (login, password)
    return login, password


def resolve(url: str) -> str:
    """Thay {conn_id} bằng login:password. URL kiểu cũ giữ nguyên nhưng vẫn che mật khẩu."""
    m = _PLACEHOLDER.match(url)
    if m:
        login, password = get_credentials(m.group(2))
        cred = f'{login}:{password}' if password else login
        return f'{m.group(1)}{cred}@{url[m.end():]}'
    m = _INLINE.match(url)
    if m and ':' in m.group(2):
        _mask(m.group(2).split(':', 1)[1])
    return url


def redact(url: str) -> str:
    """URL để in log: bỏ hẳn phần tài khoản (dùng thêm cho chắc, ngoài bộ che Airflow)."""
    m = _INLINE.match(url or '')
    return f'{m.group(1)}***@{m.group(3)}' if m else url


# Mọi 'rtsp://<tài khoản>@' trong một chuỗi bất kỳ (thông báo lỗi, traceback…)
_ANY_INLINE = re.compile(r'(rtsps?://)[^\s"\'/]*@(?=[^\s"\'@/]+(?:[:/\s"\']|$))')


def redact_text(text: str) -> str:
    """Che tài khoản của MỌI URL RTSP trong một đoạn văn bản."""
    return _ANY_INLINE.sub(r'\1***@', text) if text and 'rtsp' in text else text


class RedactRtspFilter(logging.Filter):
    """Gắn vào handler của logger: không dòng log nào ra file/console còn mật khẩu camera."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        clean = redact_text(msg)
        if clean != msg:
            record.msg, record.args = clean, None
        return True


def load_rtsp_urls(path: str) -> list[str]:
    """Đọc file danh sách camera (mỗi dòng 1 URL, có thể trong ngoặc kép) → URL chạy được."""
    with open(path, encoding='utf-8') as f:
        raw = [line.strip().strip('"').strip("'") for line in f]
    urls = []
    for u in raw:
        if not u or u.startswith('#'):
            continue
        try:
            urls.append(resolve(u))
        except RuntimeError as e:
            logger.error(f'[RTSP] Bỏ qua {redact(u)}: {e}')
    inline = sum(1 for u in raw if u and not _PLACEHOLDER.match(u) and _INLINE.match(u))
    if inline:
        logger.warning(f'[RTSP] {os.path.basename(path)}: {inline} URL còn mật khẩu viết thẳng '
                       f'— chạy scripts/migrate_rtsp_credentials.py để tách ra')
    return urls
