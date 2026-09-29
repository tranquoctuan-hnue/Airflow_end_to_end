"""
Đọc cookie từ trình duyệt THẬT của người dùng (Chrome/Firefox…) và đổi sang 2 định dạng
crawler dùng:  cookies/<platform>_cookies.txt (Netscape, yt-dlp) + .json (Playwright).

Dùng chung cho scripts/import_browser_cookies.py (nhập tay) và platform_crawlers/sessions.py
(tự nhập lại khi phiên của crawler chết). Giải mã cookie Chrome trên Linux cần khóa trong
GNOME Keyring → phải chạy trong phiên đăng nhập desktop của người dùng.
"""

import json
import os
import time

REPO_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_DIR = os.path.join(REPO_ROOT, 'cookies')

# platform → (các domain cần lấy, các cookie chứng tỏ đã đăng nhập)
PLATFORMS = {
    'tiktok':      (['.tiktok.com'],                  ['sessionid', 'sid_tt']),
    'x':           (['.x.com', '.twitter.com'],       ['auth_token', 'ct0']),
    'instagram':   (['.instagram.com'],               ['sessionid', 'ds_user_id']),
    'reddit':      (['.reddit.com'],                  ['reddit_session', 'token_v2']),
    'youtube':     (['.youtube.com', '.google.com'],  ['SAPISID', '__Secure-1PSID']),
    'facebook':    (['.facebook.com'],                ['c_user', 'xs']),
    'vimeo':       (['.vimeo.com'],                   ['vimeo', 'vuid']),
    # v1st/ts/damd/usprivacy là cookie tracking/consent GẮN CHO MỌI KHÁCH,
    # kể cả ẩn danh — không chứng minh đã đăng nhập. Cookie đăng nhập thật
    # là uid_dm (user ID) / access_token / refresh_token (đã xác nhận thực
    # tế: chỉ xuất hiện khi tài khoản Chrome đang login Dailymotion).
    'dailymotion': (['.dailymotion.com'],             ['uid_dm', 'access_token', 'refresh_token']),
}


class _Quiet:
    """yt-dlp cần logger; ta không muốn nó in rác ra terminal."""
    def debug(self, *a, **k): pass
    def info(self, *a, **k): pass
    def warning(self, *a, **k): pass
    def error(self, *a, **k): pass
    def to_screen(self, *a, **k): pass


def load_jar(browser: str, profile: str | None):
    from yt_dlp.cookies import extract_cookies_from_browser
    return extract_cookies_from_browser(browser, profile, _Quiet())


def cookies_for(jar, domains: list[str]) -> list:
    """Cookie thuộc bất kỳ domain nào trong danh sách (kể cả subdomain)."""
    out = []
    for c in jar:
        cd = (c.domain or '').lower()
        for d in domains:
            bare = d.lstrip('.')
            if cd == bare or cd.endswith('.' + bare) or cd == d:
                out.append(c)
                break
    return out


# Số giây giữa 1601-01-01 (epoch của Chrome/WebKit) và 1970-01-01 (Unix)
_WEBKIT_TO_UNIX = 11_644_473_600


def normalize_expires(raw) -> int:
    """
    Đưa expires về Unix seconds, hoặc -1 nếu là session cookie.

    Cần thiết vì yt-dlp trả expires NGUYÊN DẠNG của từng browser, không
    quy đổi. Playwright chỉ nhận Unix seconds và báo lỗi thẳng:
        "Cookie should have a valid expires, only -1 or a positive number
         for the unix timestamp in seconds is allowed"

    Ba dạng gặp thực tế:
        > 1e14   Chrome/Edge/Brave — microseconds từ 1601-01-01
        > 1e11   một số nguồn      — milliseconds từ 1970-01-01
        còn lại  Firefox           — đã là Unix seconds
    """
    if not raw:
        return -1
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return -1
    if v <= 0:
        return -1

    if v > 1e14:
        v = v / 1_000_000 - _WEBKIT_TO_UNIX
    elif v > 1e11:
        v = v / 1000

    # Sau quy đổi vẫn vô lý (âm, hoặc quá xa) → coi như session cookie
    if v <= 0 or v > 1e11:
        return -1
    return int(v)


def to_playwright(cookies: list) -> list[dict]:
    """http.cookiejar.Cookie → dict cho Playwright ctx.add_cookies()."""
    out = []
    for c in cookies:
        if not c.name or c.value is None:
            continue
        same = 'Lax'
        raw = (c.get_nonstandard_attr('SameSite') or '') if hasattr(c, 'get_nonstandard_attr') else ''
        if str(raw).lower() == 'none':
            same = 'None'
        elif str(raw).lower() == 'strict':
            same = 'Strict'
        out.append({
            'name': c.name,
            'value': c.value,
            'domain': c.domain,
            'path': c.path or '/',
            'expires': normalize_expires(c.expires),
            'httpOnly': bool(
                c.has_nonstandard_attr('HttpOnly')
                if hasattr(c, 'has_nonstandard_attr') else False
            ),
            'secure': bool(c.secure),
            'sameSite': same,
        })
    return out


def to_netscape(cookies: list) -> str:
    """Định dạng Netscape cho yt-dlp."""
    lines = ['# Netscape HTTP Cookie File',
             '# Sinh bởi scripts/import_browser_cookies.py']
    for c in cookies:
        if not c.name or c.value is None:
            continue
        domain  = c.domain or ''
        inc_sub = 'TRUE' if domain.startswith('.') else 'FALSE'
        secure  = 'TRUE' if c.secure else 'FALSE'
        exp     = normalize_expires(c.expires)
        expires = 0 if exp < 0 else exp      # 0 = session cookie trong Netscape
        lines.append(
            f"{domain}\t{inc_sub}\t{c.path or '/'}\t{secure}\t{expires}"
            f"\t{c.name}\t{c.value}"
        )
    return '\n'.join(lines) + '\n'


_CHROMIUM_DIRS = {
    'chrome':   '~/.config/google-chrome',
    'chromium': '~/.config/chromium',
    'brave':    '~/.config/BraveSoftware/Brave-Browser',
    'edge':     '~/.config/microsoft-edge',
}


def _chromium_profiles(browser: str) -> list[str | None]:
    """
    Liệt kê profile THẬT có trên đĩa (thư mục nào có file 'Cookies').

    Trước đây hàm này hardcode ['Default', 'Profile 1', 'Profile 2'] — đã gây mất
    thời gian thật 2026-08-03: user đăng nhập Dailymotion ở **Profile 3** nên
    --list báo "không có dailymotion", trong khi cookie login nằm ngay đó. Chrome
    đánh số profile theo thứ tự tạo và KHÔNG lấp lại chỗ trống, nên số profile có
    thể lớn tuỳ ý — phải quét đĩa, đừng đoán.
    """
    base = os.path.expanduser(_CHROMIUM_DIRS.get(browser, ''))
    if not base or not os.path.isdir(base):
        return [None]
    out: list[str | None] = []
    for name in sorted(os.listdir(base)):
        if not os.path.isfile(os.path.join(base, name, 'Cookies')):
            continue
        out.append(None if name == 'Default' else name)
    return out or [None]


def login_cookies(jar, platform) -> list:
    """Cookie của platform trong jar, [] nếu profile này chưa đăng nhập platform đó."""
    domains, keys = PLATFORMS[platform]
    cks = cookies_for(jar, domains)
    return cks if {c.name for c in cks} & set(keys) else []


def write_cookie_files(platform, cookies, name=None) -> tuple[str, str]:
    """Ghi 2 file (quyền 600, ghi nguyên khối để crawler đang đọc không thấy file dở)."""
    os.makedirs(COOKIES_DIR, exist_ok=True)
    base = os.path.join(COOKIES_DIR, f'{name or platform}_cookies')
    out = []
    for path, content in ((base + '.txt', to_netscape(cookies)),
                          (base + '.json', json.dumps(to_playwright(cookies), indent=2))):
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        out.append(path)
    return out[0], out[1]


def soonest_expiry(cookies):
    now = time.time()
    future = [e for e in (normalize_expires(c.expires) for c in cookies) if e > now]
    return min(future) if future else None
