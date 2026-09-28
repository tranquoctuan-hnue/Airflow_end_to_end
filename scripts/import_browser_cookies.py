#!/usr/bin/env python3
"""
Nhập cookie từ browser BẠN ĐANG DÙNG HẰNG NGÀY vào crawler.

VÌ SAO CẦN CÁCH NÀY
───────────────────
Đăng nhập bằng Playwright (scripts/save_platform_cookies.py) thất bại khi
platform dùng "Continue with Google": Google phát hiện browser bị điều khiển
tự động và chặn với thông báo
    "Couldn't sign you in — This browser or app may not be secure"
Không có cách vượt đáng tin cậy, và cũng không nên vượt.

Cách này không đăng nhập tự động gì cả — chỉ đọc cookie từ profile browser
thật, nơi bạn đã đăng nhập như người dùng bình thường. Google/TikTok/X không
thấy có gì bất thường vì phiên đăng nhập đó do chính bạn tạo bằng tay.

CÁCH DÙNG
─────────
    # Xem browser nào đang có cookie đăng nhập platform nào
    python scripts/import_browser_cookies.py --list

    # Nhập tất cả platform từ Chrome profile Default
    python scripts/import_browser_cookies.py

    # Chỉ định profile / browser khác
    python scripts/import_browser_cookies.py --browser chrome --profile "Profile 1"
    python scripts/import_browser_cookies.py --browser firefox

    # Chỉ một vài platform
    python scripts/import_browser_cookies.py tiktok x

Ghi ra 2 định dạng vì hai công cụ đọc khác nhau:
    cookies/<platform>_cookies.txt   → yt-dlp (download)
    cookies/<platform>_cookies.json  → Playwright (discovery)

LƯU Ý: cookie là thông tin đăng nhập. Script không in giá trị cookie ra
terminal, chỉ in tên. File sinh ra chứa secret thật — đừng commit vào git.
"""

import argparse
import json
import os
import sys
import time

REPO_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COOKIES_DIR = os.path.join(REPO_ROOT, 'cookies')
sys.path.insert(0, REPO_ROOT)

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


# ── Chế độ --list ────────────────────────────────────────────────────────────

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


def do_list():
    candidates: list[tuple[str, str | None]] = []
    for browser in ('chrome', 'chromium', 'brave', 'edge'):
        candidates += [(browser, p) for p in _chromium_profiles(browser)]
    candidates.append(('firefox', None))

    print('Quét cookie đăng nhập trong các browser/profile...\n')
    found_any = False

    for browser, profile in candidates:
        tag = f"{browser}" + (f" [{profile}]" if profile else " [Default]")
        try:
            jar = load_jar(browser, profile)
        except Exception:
            continue        # browser/profile không tồn tại → bỏ qua im lặng

        rows = []
        for plat, (domains, keys) in PLATFORMS.items():
            cks   = cookies_for(jar, domains)
            names = {c.name for c in cks}
            hits  = [k for k in keys if k in names]
            if hits:
                rows.append((plat, len(cks), hits))

        if rows:
            found_any = True
            print(f"  {tag}  ({len(jar)} cookie tổng)")
            for plat, n, hits in sorted(rows):
                print(f"      ✓ {plat:12} {n:3} cookie   đã login ({', '.join(hits)})")
            print()

    if not found_any:
        print('  Không tìm thấy profile nào có cookie đăng nhập.')
        print('  Hãy mở browser thường của bạn, đăng nhập các platform, rồi chạy lại.')
    return 0


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('platforms', nargs='*',
                    help=f"platform cần nhập (mặc định tất cả): {', '.join(PLATFORMS)}")
    ap.add_argument('--browser', default='chrome',
                    help='chrome | firefox | brave | edge | chromium | opera | vivaldi')
    ap.add_argument('--profile', default=None,
                    help='tên profile, ví dụ "Profile 1" (mặc định Default)')
    ap.add_argument('--list', action='store_true',
                    help='chỉ liệt kê browser nào có cookie đăng nhập gì')
    args = ap.parse_args()

    if args.list:
        return do_list()

    wanted = args.platforms or list(PLATFORMS)
    unknown = [p for p in wanted if p not in PLATFORMS]
    if unknown:
        print(f"Platform không hỗ trợ: {unknown}. Có: {', '.join(PLATFORMS)}")
        return 1

    tag = f"{args.browser}" + (f" [{args.profile}]" if args.profile else " [Default]")
    try:
        jar = load_jar(args.browser, args.profile)
    except Exception as e:
        print(f"Không đọc được cookie từ {tag}: {type(e).__name__}: {e}")
        print("Thử: python scripts/import_browser_cookies.py --list")
        return 1

    os.makedirs(COOKIES_DIR, exist_ok=True)
    print(f"Nguồn: {tag} — {len(jar)} cookie tổng")
    print('-' * 68)

    saved = skipped = 0
    for plat in wanted:
        domains, keys = PLATFORMS[plat]
        cks   = cookies_for(jar, domains)
        names = {c.name for c in cks}
        hits  = [k for k in keys if k in names]

        if not hits:
            reason = 'chưa đăng nhập' if cks else 'không có cookie'
            print(f"  {plat:12} — bỏ qua ({reason}, cần: {'/'.join(keys)})")
            skipped += 1
            continue

        txt_path  = os.path.join(COOKIES_DIR, f'{plat}_cookies.txt')
        json_path = os.path.join(COOKIES_DIR, f'{plat}_cookies.json')
        try:
            with open(txt_path, 'w') as f:
                f.write(to_netscape(cks))
            with open(json_path, 'w') as f:
                json.dump(to_playwright(cks), f, indent=2)
            os.chmod(txt_path, 0o600)      # chứa secret → chỉ chủ sở hữu đọc
            os.chmod(json_path, 0o600)
        except Exception as e:
            print(f"  {plat:12} ✗ ghi file thất bại: {e}")
            continue

        # Cookie hết hạn sớm nhất → biết khi nào cần nhập lại
        now    = time.time()
        future = [e for e in (normalize_expires(c.expires) for c in cks)
                  if e > now]
        soonest = min(future) if future else None
        when = (
            f", hạn sớm nhất {time.strftime('%Y-%m-%d', time.localtime(soonest))}"
            if soonest else ''
        )
        print(f"  {plat:12} ✓ {len(cks):3} cookie  ({', '.join(hits)}){when}")
        saved += 1

    print('-' * 68)
    print(f"Đã ghi {saved} platform, bỏ qua {skipped}.")
    print(f"Thư mục: {COOKIES_DIR}  (chmod 600 — chứa thông tin đăng nhập)")
    if skipped:
        print()
        print("Platform bị bỏ qua: mở browser THƯỜNG của bạn, đăng nhập platform đó")
        print("bằng tay (không qua Playwright), rồi chạy lại script này.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
