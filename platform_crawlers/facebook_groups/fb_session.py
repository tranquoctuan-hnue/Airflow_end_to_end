import os
import json
import time
import random
import logging
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COOKIES_FILE         = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.json')
COOKIES_NETSCAPE     = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.txt')   # for yt-dlp

def _save_netscape_cookies(cookies: list, path: str):
    """
    Convert Playwright JSON cookies → Netscape format required by yt-dlp.

    Netscape columns (tab-separated):
      domain  include_subdomains  path  secure  expiry  name  value
    """
    lines = ['# Netscape HTTP Cookie File']
    for c in cookies:
        domain      = c.get('domain', '')
        include_sub = 'TRUE' if domain.startswith('.') else 'FALSE'
        cookie_path = c.get('path', '/')
        secure      = 'TRUE' if c.get('secure', False) else 'FALSE'
        expiry      = int(c.get('expires', 0))
        if expiry < 0:
            expiry = 0
        name  = c.get('name', '')
        value = c.get('value', '')
        lines.append(f"{domain}\t{include_sub}\t{cookie_path}\t{secure}\t{expiry}\t{name}\t{value}")

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


# All known selectors Facebook uses for the email/password fields


class FBSession:
    """Manages a persistent Facebook browser session using Playwright."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._playwright = None
        self._browser: Browser = None
        self._context: BrowserContext = None
        self.page: Page = None

    def __enter__(self):
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
            ]
        )
        self._context = self._browser.new_context(
            user_agent=(
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
            viewport={'width': 1280, 'height': 800},
            locale='en-US',
        )
        self._load_cookies()
        self.page = self._context.new_page()
        self._login_if_needed()
        return self

    def __exit__(self, *args):
        self._save_cookies()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

    # ------------------------------------------------------------------
    # Cookie management
    # ------------------------------------------------------------------

    def _load_cookies(self):
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, 'r') as f:
                cookies = json.load(f)
            self._context.add_cookies(cookies)
            logger.info("Loaded Facebook session cookies")

    def _save_cookies(self):
        os.makedirs(os.path.dirname(COOKIES_FILE), exist_ok=True)
        cookies = self._context.cookies()

        # JSON — dùng để load lại vào Playwright
        with open(COOKIES_FILE, 'w') as f:
            json.dump(cookies, f)

        # Netscape — dùng để truyền vào yt-dlp
        _save_netscape_cookies(cookies, COOKIES_NETSCAPE)

        logger.info(f"Saved {len(cookies)} cookies (JSON + Netscape)")

    def _is_logged_in(self) -> bool:
        """c_user cookie is always present when FB session is active."""
        cookies = self._context.cookies()
        return any(c['name'] == 'c_user' for c in cookies)

    # ------------------------------------------------------------------
    # Login flow
    # ------------------------------------------------------------------

    def _login_if_needed(self):
        self.page.goto('https://www.facebook.com/', wait_until='domcontentloaded')
        self.random_delay(2, 4)

        if self._is_logged_in():
            logger.info("Already logged in (c_user cookie present)")
            return

        # Không tự điền mật khẩu nữa (2026-09-29): mọi platform đăng nhập bằng Chrome thật,
        # platform_crawlers/sessions.py lấy cookie sang. Tới đây nghĩa là cookie đã chết.
        raise RuntimeError(
            "Cookie Facebook đã hết đăng nhập. Đăng nhập facebook.com trong Chrome rồi chạy: "
            "airflow_venv/bin/python scripts/check_sessions.py facebook")

    def _dismiss_consent_dialog(self):
        """Click 'Allow all cookies' or 'Accept' button if FB shows a consent wall."""
        consent_selectors = [
            'button[data-cookiebanner="accept_button"]',
            'button[title="Allow all cookies"]',
            'button[title="Accept all"]',
            '[data-testid="cookie-policy-manage-dialog-accept-button"]',
        ]
        for selector in consent_selectors:
            btn = self.page.query_selector(selector)
            if btn:
                try:
                    btn.click()
                    self.random_delay(1, 2)
                    logger.info("Dismissed cookie consent dialog")
                    return
                except Exception:
                    continue

    def _find_element(self, selectors: list, timeout_ms: int = 5000):
        """Try each selector in order; return first visible element found."""
        for selector in selectors:
            try:
                el = self.page.wait_for_selector(selector, timeout=timeout_ms)
                if el:
                    return el
            except Exception:
                continue
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def random_delay(self, min_s: float = 1.0, max_s: float = 3.0):
        time.sleep(random.uniform(min_s, max_s))

    def scroll_page(self, times: int = 3, pause: float = 2.5):
        for _ in range(times):
            self.page.evaluate('window.scrollBy(0, window.innerHeight * 1.5)')
            self.random_delay(pause - 0.5, pause + 0.5)
