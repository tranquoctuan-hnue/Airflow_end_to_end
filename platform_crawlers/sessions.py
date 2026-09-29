"""
Giữ phiên đăng nhập của crawler luôn sống — MỌI platform đăng nhập bằng Chrome thật của
người dùng, không bao giờ tự điền mật khẩu.

Đầu mỗi lượt crawl, social_crawler_common gọi ensure_session(platform):
  1. Mở trang thật bằng cookies/<platform>_cookies.json → còn đăng nhập không? Kiểm tra
     bằng browser, KHÔNG tin hạn ghi trong cookie: cookie X ghi hạn 2027 mà phiên đã chết
     ở server (.claude/skills/x-dag mục 1.2).
  2. Chết → TỰ NHẬP LẠI từ Chrome. Chrome người dùng mở hằng ngày tự làm mới phiên, nên
     phần lớn trường hợp không cần ai làm gì. Quét mọi profile, profile nào có phiên SỐNG
     thì lấy (đã gặp: Profile 1 giữ token X chết, Default còn sống).
  3. Chrome cũng đã đăng xuất → BÁO NGƯỜI DÙNG đăng nhập lại (thông báo desktop +
     Telegram nếu cấu hình), tối đa 1 lần / SESSION_NOTIFY_HOURS mỗi platform. Platform
     bắt buộc đăng nhập (X, Facebook) bị bỏ qua lượt đó, vòng xoay chạy tiếp; người dùng
     đăng nhập Chrome lúc nào thì lượt kế tiếp tự lấy cookie, không cần chạy lệnh gì.

Vì sao không tự đăng nhập bằng mật khẩu (thử 2026-09-29): Reddit chặn MỌI trình duyệt
Playwright ở /login ("blocked by network security"), X/Facebook hay đòi mã xác nhận /
checkpoint thiết bị mới, Google chặn đăng nhập tự động. Chrome thật thì không bị chặn.

Cấu hình (env):
  COOKIE_BROWSER=chrome            chrome | chromium | brave | edge | firefox
  COOKIE_PROFILE="Profile 1"       chỉ lấy từ 1 profile (mặc định: quét mọi profile)
  SESSION_CHECK=true               false = bỏ kiểm tra, dùng cookie như cũ
  SESSION_NOTIFY_HOURS=12          khoảng cách tối thiểu giữa 2 lần báo cùng 1 platform
  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID   báo qua Telegram (khi không ngồi cạnh máy)
"""

import fcntl
import json
import logging
import os
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass

from platform_crawlers import browser_cookies as BC

logger = logging.getLogger(__name__)

REPO_ROOT = BC.REPO_ROOT
STATE_DIR = os.path.join(REPO_ROOT, 'state', 'sessions')
LOGIN_NEEDED_FILE = os.path.join(STATE_DIR, 'login_needed.json')

BROWSER = os.environ.get('COOKIE_BROWSER', 'chrome')
PROFILE = os.environ.get('COOKIE_PROFILE') or None
ENABLED = os.environ.get('SESSION_CHECK', 'true').lower() == 'true'
NOTIFY_HOURS = float(os.environ.get('SESSION_NOTIFY_HOURS', '12'))

# Cùng UA với discovery.py — phiên kiểm tra trông như phiên crawl
UA = ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')

LOGIN_URL = {
    'x': 'https://x.com/login',
    'facebook': 'https://www.facebook.com/',
    'reddit': 'https://www.reddit.com/login/',
    'dailymotion': 'https://www.dailymotion.com/signin',
}
PLATFORMS = tuple(LOGIN_URL)
# Thiếu phiên thì discovery chắc chắn ra 0 URL → bỏ qua lượt thay vì chạy vô ích.
# Reddit (còn OAuth / .json) và Dailymotion (search công khai) vẫn chạy được khi chết phiên.
REQUIRED = ('x', 'facebook')


@dataclass
class SessionResult:
    platform: str
    state: str        # alive | refreshed | login_needed | disabled | unknown
    message: str = ''

    @property
    def ok(self) -> bool:
        return self.state in ('alive', 'refreshed')

    @property
    def blocking(self) -> bool:
        return self.platform in REQUIRED and self.state == 'login_needed'


# ═══════════════════════════════════════════════════════════════════════════════
# Kiểm tra phiên bằng browser — True sống / False chết
# ═══════════════════════════════════════════════════════════════════════════════

def _visible(page, selectors):
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible():
                return True
        except Exception:
            pass
    return False


def _has(ctx, names):
    return any(c['name'] in names and c.get('value') for c in ctx.cookies())


def _check_x(ctx, page):
    page.goto('https://x.com/home', wait_until='domcontentloaded')
    for _ in range(40):
        if any(k in page.url for k in ('/login', '/i/flow', '/i/jf/onboarding', 'logout')):
            return False
        if _visible(page, ['[data-testid="SideNav_AccountSwitcher_Button"]',
                           '[data-testid="AppTabBar_Home_Link"]']):
            return True
        page.wait_for_timeout(500)
    return False


def _check_facebook(ctx, page):
    page.goto('https://www.facebook.com/', wait_until='domcontentloaded')
    page.wait_for_timeout(3000)
    if any(k in page.url for k in ('login', 'checkpoint')) or _visible(page, ['input[name="pass"]']):
        return False
    return _has(ctx, ('c_user',)) and _has(ctx, ('xs',))


def _check_reddit(ctx, page):
    r = ctx.request.get('https://www.reddit.com/api/me.json', headers={'User-Agent': UA})
    try:
        return r.status == 200 and bool((r.json().get('data') or {}).get('name'))
    except Exception:
        return False


def _check_dailymotion(ctx, page):
    page.goto('https://www.dailymotion.com/', wait_until='domcontentloaded')
    page.wait_for_timeout(3000)
    # uid_dm / refresh_token chỉ có khi đã đăng nhập (browser_cookies.PLATFORMS)
    return _has(ctx, ('uid_dm', 'refresh_token'))


CHECK = {'x': _check_x, 'facebook': _check_facebook,
         'reddit': _check_reddit, 'dailymotion': _check_dailymotion}


def _own(platform, c):
    d = (c.get('domain') or '').lstrip('.').lower()
    return any(d == x.lstrip('.') or d.endswith(x if x.startswith('.') else '.' + x)
               for x in BC.PLATFORMS[platform][0])


def check_cookies(platform, cookies: list[dict]) -> tuple[bool, list[dict]]:
    """Mở trang bằng bộ cookie (định dạng Playwright) → (còn sống?, cookie sau khi site
    làm mới). Headless MỚI (channel='chromium'): headless cũ bị X trả trang trống, đo
    2026-09-29, Playwright 1.60."""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=True, channel='chromium',
                               args=['--disable-blink-features=AutomationControlled'])
        try:
            ctx = b.new_context(user_agent=UA, locale='en-US')
            ctx.add_cookies([c for c in cookies if _own(platform, c)])
            page = ctx.new_page()
            page.set_default_timeout(30000)
            alive = CHECK[platform](ctx, page)
            return alive, [c for c in ctx.cookies() if _own(platform, c)]
        finally:
            b.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Nhập lại từ Chrome
# ═══════════════════════════════════════════════════════════════════════════════

class _PwCookie:
    """dict Playwright → object giống http.cookiejar.Cookie cho to_netscape/to_playwright."""
    def __init__(self, d):
        self.name, self.value, self.domain = d['name'], d['value'], d.get('domain', '')
        self.path, self.secure = d.get('path', '/'), d.get('secure', False)
        self.expires = d.get('expires', -1)
        self._http, self._same = d.get('httpOnly', False), d.get('sameSite', 'Lax')

    def has_nonstandard_attr(self, k):
        return k == 'HttpOnly' and self._http

    def get_nonstandard_attr(self, k, default=None):
        return self._same if k == 'SameSite' else default


def _save(platform, pw_cookies):
    cks = [_PwCookie(c) for c in pw_cookies]
    BC.write_cookie_files(platform, cks)
    if platform == 'facebook':
        # Bản riêng cho DAG cũ legacy_fb_groups_crawler + bộ lọc Qwen (yt-dlp ghi ngược
        # vào file đó nên KHÔNG dùng chung với facebook_cookies.*)
        BC.write_cookie_files(platform, cks, name='facebook_legacy')


def _profiles(platform):
    """Profile đã thành công lần trước được thử TRƯỚC — đỡ phải giải mã cả chục profile."""
    if PROFILE:
        return [PROFILE]
    found = BC._chromium_profiles(BROWSER) if BROWSER != 'firefox' else [None]
    last = _read_state(platform).get('chrome_profile', '__none__')
    return sorted(found, key=lambda p: (p or 'Default') != last)


def refresh_from_chrome(platform) -> tuple[bool, str]:
    """(thành công?, mô tả). Chỉ ghi file khi bộ cookie từ Chrome đã được kiểm tra SỐNG."""
    tried = []
    for prof in _profiles(platform):
        name = prof or 'Default'
        try:
            jar = BC.load_jar(BROWSER, prof)
        except Exception as e:
            tried.append(f'{name}: không đọc được ({type(e).__name__})')
            continue
        cks = BC.login_cookies(jar, platform)
        if not cks:
            tried.append(f'{name}: chưa đăng nhập')
            continue
        alive, fresh = check_cookies(platform, BC.to_playwright(cks))
        if alive:
            _save(platform, fresh)
            _write_state(platform, chrome_profile=name)
            return True, f'đã lấy lại cookie từ {BROWSER} [{name}]'
        tried.append(f'{name}: phiên trong Chrome cũng đã chết')
    return False, '; '.join(tried) or f'không thấy profile {BROWSER} nào'


# ═══════════════════════════════════════════════════════════════════════════════
# Trạng thái + thông báo
# ═══════════════════════════════════════════════════════════════════════════════

def _read_state(platform) -> dict:
    try:
        with open(os.path.join(STATE_DIR, f'{platform}.json'), encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _write_state(platform, **kw):
    os.makedirs(STATE_DIR, exist_ok=True)
    path = os.path.join(STATE_DIR, f'{platform}.json')
    st = {**_read_state(platform), **kw}
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


def login_needed() -> dict:
    """{platform: {since, message}} — các platform đang chờ người dùng đăng nhập Chrome."""
    try:
        with open(LOGIN_NEEDED_FILE, encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _set_login_needed(platform, message=None):
    data = login_needed()
    if message is None:
        data.pop(platform, None)
    else:
        data.setdefault(platform, {'since': time.strftime('%Y-%m-%d %H:%M')})['message'] = message
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(LOGIN_NEEDED_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@contextmanager
def _lock(platform):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, f'{platform}.lock'), 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def notify(title: str, body: str) -> dict:
    """Thông báo desktop (khi ngồi ở máy) + Telegram (khi đi vắng). Lỗi gửi không làm hỏng
    lượt crawl. Trả {'desktop': ..., 'telegram': ...} để --notify-test báo đúng kết quả —
    Telegram trả HTTP 4xx kèm JSON ok=false khi token sai, KHÔNG raise (2026-09-29: token
    dán thiếu phần '<id>:' → 404, trước đây vẫn báo "đã gửi")."""
    res = {}
    try:
        r = subprocess.run(['notify-send', '-u', 'critical', '-a', 'CCTV crawler', title, body],
                           timeout=10, capture_output=True)
        res['desktop'] = 'ok' if r.returncode == 0 else f'lỗi {r.returncode}'
    except Exception as e:
        res['desktop'] = f'không gửi được ({type(e).__name__})'
    token = (os.environ.get('TELEGRAM_BOT_TOKEN') or '').strip()
    chat = (os.environ.get('TELEGRAM_CHAT_ID') or '').strip()
    if not (token and chat):
        res['telegram'] = 'chưa cấu hình'
        return res
    try:
        import requests
        j = requests.post(f'https://api.telegram.org/bot{token}/sendMessage',
                          data={'chat_id': chat, 'text': f'{title}\n{body}'}, timeout=15).json()
        res['telegram'] = 'ok' if j.get('ok') else f"lỗi {j.get('error_code')}: {j.get('description')}"
    except Exception as e:
        res['telegram'] = f'lỗi {type(e).__name__}'
    if res['telegram'] != 'ok':
        logger.warning(f"[Session] Gửi Telegram thất bại: {res['telegram']}")
    return res


def _ask_user(platform, detail):
    st = _read_state(platform)
    if time.time() - st.get('notified_at', 0) < NOTIFY_HOURS * 3600:
        return
    notify(f'Cần đăng nhập lại {platform}',
           f'Mở Chrome, đăng nhập {LOGIN_URL[platform]} — crawler tự lấy cookie ở lượt sau, '
           f'không cần chạy lệnh. ({detail[:200]})')
    _write_state(platform, notified_at=time.time())


# ═══════════════════════════════════════════════════════════════════════════════
# API chính
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_session(platform: str, check_only: bool = False, notify_user: bool = True) -> SessionResult:
    """KHÔNG bao giờ raise — lỗi trả trong SessionResult để lượt crawl tự quyết."""
    if platform not in CHECK:
        return SessionResult(platform, 'unknown', 'platform không cần / không hỗ trợ kiểm tra phiên')
    if not ENABLED:
        return SessionResult(platform, 'disabled', 'SESSION_CHECK=false')
    try:
        with _lock(platform):
            path = os.path.join(BC.COOKIES_DIR, f'{platform}_cookies.json')
            current = json.load(open(path, encoding='utf-8')) if os.path.exists(path) else []
            if current:
                alive, fresh = check_cookies(platform, current)
                if alive:
                    _save(platform, fresh)           # giữ token site vừa làm mới
                    _write_state(platform, checked_at=time.time(), alive=True)
                    was = platform in login_needed()
                    _set_login_needed(platform, None)
                    if was:
                        notify(f'{platform} đã đăng nhập lại', 'Crawler chạy tiếp bình thường.')
                    return SessionResult(platform, 'alive', 'phiên còn đăng nhập')
            if check_only:
                return SessionResult(platform, 'login_needed',
                                     'phiên của crawler đã chết' if current else 'chưa có cookie')

            ok, detail = refresh_from_chrome(platform)
            _write_state(platform, checked_at=time.time(), alive=ok)
            if ok:
                _set_login_needed(platform, None)
                logger.info(f'[Session] {platform}: {detail}')
                return SessionResult(platform, 'refreshed', detail)

            msg = (f'cần đăng nhập lại {platform} trong {BROWSER} ({LOGIN_URL[platform]}); '
                   f'lượt sau crawler tự lấy cookie. Chi tiết: {detail}')
            _set_login_needed(platform, msg)
            if notify_user:
                _ask_user(platform, detail)
            return SessionResult(platform, 'login_needed', msg)
    except Exception as e:
        logger.exception(f'[Session] {platform}: lỗi khi kiểm tra phiên')
        return SessionResult(platform, 'unknown', f'{type(e).__name__}: {str(e)[:200]}')
