"""
CCTV detector using a LOCAL Qwen2.5-VL vision model.

Flow per video:
  1. yt-dlp extract_info (no download) → get thumbnail URL
  2. Download thumbnail image (fallback: chụp frame bằng Playwright)
  3. Lọc nhanh video 9:16 (Reels/dọc) — bỏ qua
  4. Đưa ảnh vào model Qwen2.5-VL local → model trả JSON {is_cctv, confidence, reason}
  5. Return True / False (dựa trên is_cctv trong JSON)
"""

import os
import json
import logging
import tempfile
import threading

import requests
import yt_dlp
from PIL import Image

logger = logging.getLogger(__name__)

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Cookie của DAG Facebook nhóm (cũ) — scripts/refresh_fb_cookies.py tạo ra. Bộ phân loại
# này dùng nó làm cookiefile cho MỌI platform, và yt-dlp ghi ngược cả jar vào đó, nên
# DAG social_crawler_fb KHÔNG dùng file này (dùng cookies/facebook_cookies.*).
COOKIES_FILE = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.txt')
COOKIES_JSON = os.path.join(BASE_DIR, 'cookies', 'facebook_legacy_cookies.json')

# Model VLM local — có thể override bằng env (đường dẫn thư mục model đã tải).
MODEL_NAME = os.environ.get('QWEN_MODEL_PATH', 'Qwen/Qwen2.5-VL-3B-Instruct')
# fp16 mặc định: CCTV_PROMPT hiện khá dài/chi tiết — đã test 4-bit làm model sinh
# output rác (không phải JSON hợp lệ) với prompt này, còn fp16 thì ổn định.
# Bật lại 4-bit (nhẹ VRAM hơn) bằng QWEN_LOAD_4BIT=true nếu rút gọn CCTV_PROMPT sau này.
LOAD_4BIT  = os.environ.get('QWEN_LOAD_4BIT', 'false').lower() == 'true'
MAX_NEW_TOKENS = 200

CCTV_PROMPT = """You are an expert in visual scene analysis.

Your task is to determine whether the given image is likely captured by a CCTV/security surveillance camera.

Analyze the entire image instead of relying on a single cue. Consider multiple pieces of evidence including but not limited to:

- Elevated or ceiling-mounted viewpoint.
- Wide-angle or surveillance-style field of view.
- Low image quality, compression artifacts, motion blur, noise.
- Infrared/night vision appearance (grayscale or IR illumination).
- Fixed camera perspective.
- Continuous monitoring viewpoint instead of artistic composition.
- Typical surveillance locations such as entrances, hallways, stores, parking lots, warehouses, offices, elevators, streets, intersections, or building exteriors.
- Presence of timestamps, camera IDs, channel numbers, recording overlays, or DVR/NVR interface elements.
- Lack of photographic aesthetics (poor framing, no depth-of-field, no intentional composition).
- Lens distortion commonly seen in surveillance cameras.

Do NOT rely on overlays alone. An image with timestamps or camera labels is not necessarily CCTV if the overall visual characteristics contradict it.

Likewise, some CCTV images may not contain timestamps or overlays.

Classify based on the overall visual evidence.

Return only JSON.

{
  "is_cctv": true,
  "confidence": 0.97,
  "reason": "The image has a fixed elevated viewpoint, wide-angle lens, surveillance-style composition, visible compression artifacts, and depicts a building entrance consistent with a security camera."
}"""

# ── Lazy singleton: model chỉ load MỘT lần cho cả tiến trình ─────────────
_MODEL = None
_PROCESSOR = None
_MODEL_LOCK = threading.Lock()


def _get_model():
    """Load (lazy) và cache model + processor. Thread-safe."""
    global _MODEL, _PROCESSOR
    if _MODEL is None:
        with _MODEL_LOCK:
            if _MODEL is None:
                logger.info(f"Đang load Qwen2.5-VL local: {MODEL_NAME} (4bit={LOAD_4BIT}) ...")
                from transformers import (
                    Qwen2_5_VLForConditionalGeneration, AutoProcessor,
                )
                import torch

                kwargs = {'device_map': 'auto'}
                if LOAD_4BIT:
                    from transformers import BitsAndBytesConfig
                    kwargs['quantization_config'] = BitsAndBytesConfig(
                        load_in_4bit=True,
                        bnb_4bit_compute_dtype=torch.float16,
                    )
                else:
                    kwargs['torch_dtype'] = torch.float16

                _MODEL = Qwen2_5_VLForConditionalGeneration.from_pretrained(
                    MODEL_NAME, **kwargs
                )
                _PROCESSOR = AutoProcessor.from_pretrained(MODEL_NAME)
                logger.info("Qwen2.5-VL đã sẵn sàng.")
    return _MODEL, _PROCESSOR


class CCTVClassifier:
    def __init__(self):
        # Load model ngay để lỗi (thiếu thư viện/model) lộ ra ở đầu task,
        # và các video sau không phải chờ load.
        _get_model()
        logger.info(f"CCTVClassifier sẵn sàng — model local {MODEL_NAME}")

    # ── Inference ──────────────────────────────────────────────────────

    # Giới hạn kích thước ảnh đưa vào Qwen2.5-VL.
    # Ảnh 1280×800 → ~1300 patches → vượt max position embedding → CUDA assert.
    # 448×448 → ~256 patches → an toàn và đủ cho phân loại CCTV.
    MAX_INFERENCE_SIZE = 448

    @staticmethod
    def _validate_and_resize(image_path: str) -> str | None:
        """
        Validate ảnh và resize về MAX_INFERENCE_SIZE nếu cần.
        Trả về đường dẫn ảnh đã resize (file tạm mới), hoặc None nếu ảnh không hợp lệ.

        Điều kiện loại bỏ:
          - không đọc được
          - kích thước < 28×28 (dưới ngưỡng tối thiểu của Qwen2.5-VL)
          - frame đen toàn phần (mean pixel < 8)
        """
        try:
            import numpy as np
            img = Image.open(image_path).convert('RGB')
            w, h = img.size

            if w < 28 or h < 28:
                logger.warning(f"[SKIP - too small {w}x{h}]")
                return None

            arr = np.array(img)
            if arr.mean() < 8:
                logger.warning("[SKIP - black frame]")
                return None

            # Resize xuống MAX_INFERENCE_SIZE nếu ảnh lớn hơn
            max_side = CCTVClassifier.MAX_INFERENCE_SIZE
            if w > max_side or h > max_side:
                img.thumbnail((max_side, max_side), Image.LANCZOS)
                tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
                tmp.close()
                img.save(tmp.name, 'JPEG', quality=90)
                logger.debug(f"Resized {w}×{h} → {img.size}")
                return tmp.name

            return image_path  # ảnh đủ nhỏ, dùng trực tiếp
        except Exception as e:
            logger.warning(f"[SKIP - invalid image] {e}")
            return None

    def _ask_model(self, image_path: str) -> bool:
        """Đưa 1 ảnh vào Qwen2.5-VL, hỏi có phải CCTV không. True/False."""
        result = self._ask_model_detail(image_path)
        return bool(result and result.get('is_cctv'))

    def assess_image(self, image_path: str) -> dict:
        """
        Như classify_image nhưng trả CHI TIẾT để ghi lại lý do loại/nhận:

            {'is_cctv': bool, 'confidence': float|None, 'reason': str, 'raw': str}
            {'is_cctv': None, 'invalid_image': True, 'error': ...}  ← ảnh hỏng/đen
            {'is_cctv': None, 'error': '...'}                        ← lỗi inference

        KHÔNG có bước lọc 9:16 như classify_image: VideoPipeline tự kiểm 9:16 trên
        file video (và có thể cố ý cho qua với ALLOW_PORTRAIT). is_cctv=None nghĩa là
        "không đánh giá được" — caller KHÔNG được coi là "không phải CCTV", nếu
        không lỗi GPU sẽ bị ghi nhầm thành video bị loại vĩnh viễn.
        """
        try:
            result = self._ask_model_detail(image_path)
        except Exception as e:
            logger.error(f"[Qwen] Inference lỗi: {e}")
            return {'is_cctv': None, 'error': f'{type(e).__name__}: {e}'}
        if result is None:
            return {'is_cctv': None, 'invalid_image': True,
                    'error': 'ảnh không hợp lệ (quá nhỏ / frame đen / không đọc được)'}
        return result

    def _ask_model_detail(self, image_path: str) -> dict | None:
        """
        Đưa 1 ảnh vào Qwen2.5-VL. Trả {'is_cctv', 'confidence', 'reason', 'raw'},
        None nếu ảnh không hợp lệ. Lỗi inference thì raise.
        """
        global _MODEL, _PROCESSOR

        from qwen_vl_utils import process_vision_info
        import torch

        resized_path = self._validate_and_resize(image_path)
        if resized_path is None:
            return None

        # Dọn file resize tạm (nếu khác file gốc) sau khi inference xong
        cleanup_resized = resized_path != image_path

        try:
            model, processor = _get_model()
            messages = [{
                "role": "user",
                "content": [
                    {"type": "image", "image": resized_path},
                    {"type": "text", "text": CCTV_PROMPT},
                ],
            }]

            text = processor.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True
            )
            image_inputs, video_inputs = process_vision_info(messages)
            inputs = processor(
                text=[text],
                images=image_inputs,
                videos=video_inputs,
                padding=True,
                return_tensors="pt",
            )
            # Chuyển từng tensor sang device; tránh dtype conflict với 4-bit quantization
            device = next(model.parameters()).device
            inputs = {k: v.to(device) if hasattr(v, 'to') else v for k, v in inputs.items()}

            try:
                with torch.inference_mode():
                    generated_ids = model.generate(**inputs, max_new_tokens=MAX_NEW_TOKENS)
            except RuntimeError as e:
                err = str(e)
                if 'device-side assert' in err or 'CUDA' in err:
                    logger.error(f"[Qwen] CUDA assert — reset model singleton: {e}")
                    with _MODEL_LOCK:
                        _MODEL = None
                        _PROCESSOR = None
                    try:
                        torch.cuda.empty_cache()
                    except Exception:
                        pass
                raise

            trimmed = [
                out[len(inp):]
                for inp, out in zip(inputs['input_ids'], generated_ids)
            ]
            answer = processor.batch_decode(
                trimmed, skip_special_tokens=True,
                clean_up_tokenization_spaces=False,
            )[0].strip()

            logger.debug(f"[Qwen] raw answer='{answer}'")

            parsed = self._parse_cctv_json(answer)
            if parsed is not None:
                logger.info(
                    f"[Qwen] is_cctv={parsed.get('is_cctv')} "
                    f"confidence={parsed.get('confidence')} reason={parsed.get('reason')}"
                )
                return {
                    'is_cctv':    bool(parsed['is_cctv']),
                    'confidence': parsed.get('confidence'),
                    'reason':     parsed.get('reason') or '',
                    'raw':        answer,
                }

            # Model không trả về JSON hợp lệ — fallback về check yes/no thô
            logger.warning(f"[Qwen] Không parse được JSON, fallback yes/no: '{answer[:120]}'")
            lowered = answer.lower()
            return {
                'is_cctv': (lowered.startswith('yes') or lowered.startswith('có')
                            or lowered.startswith('true')),
                'confidence': None,
                'reason': '(model không trả JSON — đoán theo yes/no thô)',
                'raw': answer,
            }
        finally:
            if cleanup_resized:
                try:
                    os.remove(resized_path)
                except Exception:
                    pass

    @staticmethod
    def _parse_cctv_json(text: str) -> dict | None:
        """Trích JSON {"is_cctv": bool, ...} từ output của model. None nếu không parse được."""
        import re

        match = re.search(r'\{.*\}', text, re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
        return data if 'is_cctv' in data else None

    # ── Thumbnail extraction ───────────────────────────────────────────

    @staticmethod
    def _normalize_fb_url(url: str) -> str:
        """
        Chuyển URL dạng /videos/ID/?idorvanity=GROUP_ID về watch?v=ID&idorvanity=GROUP_ID
        để yt-dlp giữ ngữ cảnh nhóm khi resolve video.
        """
        import re as _re
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)

        # Lấy video ID từ path /videos/ID hoặc query v=ID
        m = _re.search(r'/videos/(\d+)', parsed.path)
        vid = m.group(1) if m else qs.get('v', [None])[0]
        if not vid:
            return url

        # Giữ lại idorvanity nếu có — yt-dlp cần để resolve video trong nhóm riêng tư
        idorvanity = qs.get('idorvanity', [None])[0]
        if idorvanity:
            return f'https://www.facebook.com/watch/?v={vid}&idorvanity={idorvanity}'
        return f'https://www.facebook.com/watch/?v={vid}'

    # oEmbed công khai — không cần API key, nên KHÔNG bị ảnh hưởng bởi
    # client_id/client_secret dùng chung mà yt-dlp hardcode cho một số
    # extractor (đã xác nhận thực tế: extractor Dailymotion của yt-dlp dùng
    # client_id 'f1a362d288c1b98099c7' CHUNG cho MỌI người dùng yt-dlp trên
    # thế giới → bị Dailymotion rate-limit ngẫu nhiên, lỗi 401 không liên tục
    # dù URL hợp lệ). oEmbed test thực tế ổn định 3/3 lượt liên tiếp.
    _OEMBED_ENDPOINTS = {
        'dailymotion': 'https://www.dailymotion.com/services/oembed?url={url}&format=json',
        'vimeo':       'https://vimeo.com/api/oembed.json?url={url}',
    }

    def _get_oembed_thumbnail(self, video_url: str) -> str | None:
        """Thử lấy thumbnail qua oEmbed công khai của platform (nếu hỗ trợ)."""
        from urllib.parse import quote
        try:
            from crawler_core.dedup import detect_platform
            platform = detect_platform(video_url)
        except Exception:
            platform = None

        template = self._OEMBED_ENDPOINTS.get(platform)
        if not template:
            return None

        try:
            resp = requests.get(
                template.format(url=quote(video_url, safe='')),
                headers={'User-Agent': (
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                )},
                timeout=15,
            )
            if resp.status_code == 200:
                thumb = resp.json().get('thumbnail_url')
                if thumb:
                    logger.info(f"Thumbnail lấy được qua oEmbed ({platform})")
                    return thumb
        except Exception as e:
            logger.debug(f"[oEmbed] {platform} thất bại ({video_url}): {e}")
        return None

    def _get_thumbnail_via_og(self, video_url: str) -> str | None:
        """Fallback: fetch HTML trang FB và đọc og:image meta tag."""
        import re as _re
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
        }
        # Load cookies từ Netscape file nếu có
        cookies = {}
        if os.path.exists(COOKIES_FILE):
            try:
                with open(COOKIES_FILE) as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        parts = line.split('\t')
                        if len(parts) >= 7:
                            cookies[parts[5]] = parts[6]
            except Exception:
                pass

        try:
            resp = requests.get(
                video_url, headers=headers, cookies=cookies, timeout=15
            )
            m = _re.search(
                r'<meta\s+property="og:image"\s+content="([^"]+)"', resp.text
            )
            if m:
                return m.group(1).replace('&amp;', '&')
        except Exception as e:
            logger.warning(f"og:image fallback thất bại: {e}")
        return None

    def _get_thumbnail_url(self, video_url: str) -> str | None:
        """
        Lấy thumbnail URL, thử theo thứ tự:
          0. oEmbed công khai (platform hỗ trợ) — nhanh, tránh rate-limit
             client_id dùng chung của yt-dlp (xem _get_oembed_thumbnail)
          1. yt-dlp với URL gốc
          2. yt-dlp với URL đã normalize (watch?v=ID)
          3. Scrape og:image từ HTML trang
        """
        result = self._get_oembed_thumbnail(video_url)
        if result:
            return result

        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
        }
        if os.path.exists(COOKIES_FILE):
            ydl_opts['cookiefile'] = COOKIES_FILE

        def _ydl_thumb(url: str) -> str | None:
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if not info:
                        return None
                    thumbnails = info.get('thumbnails') or []
                    if thumbnails:
                        return thumbnails[-1].get('url') or info.get('thumbnail')
                    return info.get('thumbnail')
            except Exception:
                return None

        # 1. URL gốc
        result = _ydl_thumb(video_url)
        if result:
            return result

        # 2. URL đã normalize
        normalized = self._normalize_fb_url(video_url)
        if normalized != video_url:
            result = _ydl_thumb(normalized)
            if result:
                logger.info(f"Thumbnail lấy được qua URL normalize: {normalized}")
                return result

        # 3. og:image fallback
        result = self._get_thumbnail_via_og(video_url)
        if result:
            logger.info(f"Thumbnail lấy được qua og:image fallback")
            return result

        logger.warning(f"Không lấy được thumbnail [{video_url}]")
        return None

    def _download_thumbnail(self, url: str) -> str | None:
        """Tải thumbnail về file tạm. Trả về đường dẫn hoặc None."""
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            tmp.write(resp.content)
            tmp.close()
            return tmp.name
        except Exception as e:
            logger.warning(f"Không tải được thumbnail: {e}")
            return None

    # ── Playwright frame capture ───────────────────────────────────────

    def _capture_frame_playwright(self, video_url: str) -> str | None:
        """
        Dùng Playwright để mở trang video, nhấn play, chụp ảnh frame
        từ element <video>. Chạy trong thread riêng để tránh xung đột
        với asyncio event loop của Airflow.
        """
        from concurrent.futures import ThreadPoolExecutor

        def _run() -> str | None:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError:
                return None

            cookies_pw = []
            if os.path.exists(COOKIES_JSON):
                try:
                    with open(COOKIES_JSON) as f:
                        cookies_pw = json.load(f)
                except Exception:
                    pass

            try:
                with sync_playwright() as pw:
                    browser = pw.chromium.launch(
                        headless=True,
                        args=['--disable-blink-features=AutomationControlled', '--no-sandbox'],
                    )
                    ctx = browser.new_context(
                        user_agent=(
                            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                            '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
                        ),
                        viewport={'width': 1280, 'height': 800},
                    )
                    ctx.add_init_script(
                        'Object.defineProperty(navigator,"webdriver",{get:()=>undefined})'
                    )
                    if cookies_pw:
                        ctx.add_cookies(cookies_pw)

                    page = ctx.new_page()
                    page.goto(video_url, wait_until='domcontentloaded', timeout=30_000)
                    page.wait_for_timeout(2_000)

                    # Bypass sensitive content overlay:
                    # Facebook hiển thị overlay blur + nút "Tìm hiểu thêm" (không phải "Xem nội dung").
                    # Cách click qua: click trực tiếp vào vùng video bị che.
                    # Nếu không được → dùng JS force play để bypass overlay hoàn toàn.
                    try:
                        video_el = page.query_selector('video')
                        if video_el:
                            box = video_el.bounding_box()
                            if box:
                                cx = box['x'] + box['width'] / 2
                                cy = box['y'] + box['height'] / 2
                                page.mouse.click(cx, cy)
                                logger.info("[Classifier] Click vào vùng video overlay")
                                page.wait_for_timeout(1_000)
                    except Exception:
                        pass

                    # JS force play — bypass mọi overlay UI
                    try:
                        page.evaluate(
                            '() => { const v = document.querySelector("video"); '
                            'if (v) { v.muted = true; v.play().catch(() => {}); } }'
                        )
                    except Exception:
                        pass

                    for sel in ('video', '[aria-label="Play"]', '[aria-label="Phát"]'):
                        try:
                            page.click(sel, timeout=3_000)
                            break
                        except Exception:
                            continue

                    try:
                        # Chờ video thực sự bắt đầu play (currentTime > 0)
                        page.wait_for_function(
                            '() => { const v = document.querySelector("video"); '
                            'return v && v.readyState >= 2 && v.currentTime > 0.1; }',
                            timeout=15_000,
                        )
                    except Exception:
                        pass

                    # Thêm 1.5s để frame đầu tiên render xong
                    page.wait_for_timeout(1_500)

                    # Dùng JS canvas để lấy frame thực của video — bỏ qua overlay CSS
                    # element.screenshot() sẽ chụp cả overlay "Nội dung nhạy cảm"
                    try:
                        data_url = page.evaluate(
                            '() => {'
                            '  const v = document.querySelector("video");'
                            '  if (!v || v.videoWidth === 0) return null;'
                            '  const c = document.createElement("canvas");'
                            '  c.width = v.videoWidth; c.height = v.videoHeight;'
                            '  c.getContext("2d").drawImage(v, 0, 0);'
                            '  return c.toDataURL("image/jpeg", 0.85);'
                            '}'
                        )
                        if data_url and data_url.startswith('data:image'):
                            import base64
                            b64 = data_url.split(',', 1)[1]
                            tmp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
                            tmp.write(base64.b64decode(b64))
                            tmp.close()
                            browser.close()
                            logger.info(f"Frame captured via JS canvas: {video_url}")
                            return tmp.name
                    except Exception as e:
                        logger.debug(f"JS canvas thất bại: {e}")

                    # Fallback: screenshot element (nếu không có overlay)
                    video_el = page.query_selector('video')
                    if video_el:
                        tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
                        tmp.close()
                        video_el.screenshot(path=tmp.name)
                        browser.close()
                        logger.info(f"Frame captured via element.screenshot: {video_url}")
                        return tmp.name

                    browser.close()
            except Exception as e:
                logger.warning(f"Playwright frame capture thất bại: {e}")
            return None

        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                return ex.submit(_run).result(timeout=60)
        except Exception as e:
            logger.warning(f"Playwright thread thất bại: {e}")
            return None

    # ── CCTV detection ─────────────────────────────────────────────────

    def classify_image(self, image_path: str, label: str = "") -> bool:
        """
        Phân loại 1 ảnh cục bộ đã có sẵn (thumbnail hoặc frame đã chụp từ nơi khác):
          1. Lọc nhanh 9:16 (video dọc) — chắc chắn không phải CCTV
          2. Hỏi Qwen2.5-VL local → Yes/No

        Không xoá image_path — caller tự quản lý file của mình.
        """
        try:
            img = Image.open(image_path)
            w, h = img.size
            if h > 0 and abs((w / h) - (9 / 16)) < 0.05:
                logger.info(f"[SKIP - 9:16 portrait] {label or image_path}")
                return False
        except Exception as e:
            logger.warning(f"[SKIP - invalid image] {label or image_path}: {e}")
            return False

        try:
            is_cctv = self._ask_model(image_path)
        except Exception as e:
            logger.error(f"[Qwen] Inference lỗi, bỏ qua: {e}")
            return False

        result_label = 'CCTV ✓' if is_cctv else 'SKIP'
        logger.info(f"[{result_label}] {label or image_path}")
        return is_cctv

    def is_cctv(self, video_url: str) -> bool:
        """
        Trả về True nếu video trông như footage từ camera CCTV.
        Quy trình:
          1. Lấy thumbnail qua yt-dlp / og:image
          2. Nếu không được → chụp frame trực tiếp bằng Playwright
          3. classify_image() → lọc 9:16 + hỏi Qwen2.5-VL local
        """
        thumb_path = None

        # Thử thumbnail URL trước
        thumb_url = self._get_thumbnail_url(video_url)
        if thumb_url:
            thumb_path = self._download_thumbnail(thumb_url)

        # Fallback: chụp frame bằng Playwright
        if not thumb_path:
            logger.info(f"Thumbnail thất bại, chụp frame Playwright: {video_url}")
            thumb_path = self._capture_frame_playwright(video_url)

        if not thumb_path:
            logger.warning(f"[SKIP - no image] {video_url}")
            return False

        try:
            return self.classify_image(thumb_path, label=video_url)
        finally:
            try:
                os.remove(thumb_path)
            except Exception:
                pass
