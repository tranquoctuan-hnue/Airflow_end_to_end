"""
Dedup store for YouTube live CCTV streams.

Mỗi luồng mới tìm thấy (đã qua lọc tiêu đề ở live_stream_finder) sẽ được:
  0. Kiểm tra xem kênh này đã từng đăng ĐÚNG title này chưa (xem giải thích dưới) —
     nếu rồi thì bỏ qua ngay, không chụp/phân loại.
  1. Chụp 1 frame
  2. Xác minh bằng CCTVClassifier (Qwen2.5-VL local) nếu use_visual_classifier=True
  3. Ghi 1 dòng vào OUTPUT_TXT_PATH: <url>\t<image_path>\t<title>\t<channel_id>
     (chỉ link ĐÃ XÁC NHẬN là CCTV)

Dù kết quả thế nào (added / capture_failed / rejected_not_cctv / duplicate_broadcast),
url đều được ghi vào CHECKED_HISTORY_PATH:
    <url>\t<status>\t<timestamp>\t<channel_id>\t<title>
để các lần chạy sau (vòng lặp trong 12 tiếng, hoặc ngày hôm sau) không kiểm tra lại
cùng 1 link nhiều lần.

Vấn đề "re-broadcast": nhiều kênh camera live 24/7 nhưng YouTube tự ngắt sau ~24h,
kênh phát lại thành 1 video live MỚI (video_id/url khác) cho cùng 1 góc camera vật lý,
thường dùng lại NGUYÊN VĂN title cũ. Dedup theo URL không bắt được việc này. Nên ngoài
dedup theo URL, còn dedup theo (channel_id, title đã chuẩn hoá) — nếu kênh đó đã từng
đăng đúng title này (bằng bất kỳ url nào trong quá khứ) thì coi là cùng 1 luồng, bỏ qua.
Giới hạn: chỉ bắt được khi title lặp lại y hệt (sau khi bỏ khoảng trắng thừa + hạ chữ
thường) — nếu kênh đổi title mỗi lần phát lại (vd thêm ngày/giờ) sẽ không bắt được.
"""

import os
from typing import Optional, Tuple

from src.modules.config import get_timestamp
from src.modules.file_ops import create_directory, get_unique_filename
from src.modules.img_io import save_image_cv2
from src.modules.logger import default_logger as logger
from src.modules.video_capture import VideoCapture

from .config import CHECKED_HISTORY_PATH, OUTPUT_TXT_PATH, SNAPSHOT_DIR, USE_VISUAL_CLASSIFIER

_FIELD_SEP = "\t"


def _normalize_title(title: str) -> str:
    return " ".join(title.strip().lower().split())


class SnapshotStore:
    def __init__(
        self,
        txt_path: str = OUTPUT_TXT_PATH,
        snapshot_dir: str = SNAPSHOT_DIR,
        history_path: str = CHECKED_HISTORY_PATH,
        use_visual_classifier: bool = USE_VISUAL_CLASSIFIER,
    ):
        self.txt_path = txt_path
        self.snapshot_dir = snapshot_dir
        self.history_path = history_path
        create_directory(os.path.dirname(self.txt_path))
        create_directory(self.snapshot_dir)
        create_directory(os.path.dirname(self.history_path))

        known_urls, known_channel_keys = self._load_live_file(self.txt_path)
        hist_urls, hist_channel_keys = self._load_history_file(self.history_path)

        self._known_urls = known_urls  # đã xác nhận CCTV (trong OUTPUT_TXT_PATH)
        self._checked_urls = known_urls | hist_urls  # đã kiểm tra (mọi kết quả)
        self._channel_keys = known_channel_keys | hist_channel_keys  # (channel_id, title) đã gặp

        self.stats = {
            "added": 0,
            "capture_failed": 0,
            "rejected_not_cctv": 0,
            "duplicate_broadcast": 0,
        }

        self._classifier = None
        if use_visual_classifier:
            # Import + load model (nặng: GPU, ~vài giây) chỉ khi thực sự bật —
            # tránh kéo transformers/torch vào các chỗ chỉ cần lọc theo tiêu đề.
            from crawler_core.qwen_classifier import CCTVClassifier
            self._classifier = CCTVClassifier()

    @staticmethod
    def _channel_key(channel_id: str, title: str) -> Optional[Tuple[str, str]]:
        if not channel_id or not title:
            return None
        return (channel_id, _normalize_title(title))

    @staticmethod
    def _load_live_file(path: str):
        """OUTPUT_TXT_PATH: <url>\\t<image_path>\\t<title>\\t<channel_id>."""
        urls, channel_keys = set(), set()
        if not os.path.exists(path):
            return urls, channel_keys
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(_FIELD_SEP)
                urls.add(parts[0])
                title = parts[2] if len(parts) > 2 else ""
                channel_id = parts[3] if len(parts) > 3 else ""
                key = SnapshotStore._channel_key(channel_id, title)
                if key:
                    channel_keys.add(key)
        return urls, channel_keys

    @staticmethod
    def _load_history_file(path: str):
        """CHECKED_HISTORY_PATH: <url>\\t<status>\\t<timestamp>\\t<channel_id>\\t<title>."""
        urls, channel_keys = set(), set()
        if not os.path.exists(path):
            return urls, channel_keys
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(_FIELD_SEP)
                urls.add(parts[0])
                channel_id = parts[3] if len(parts) > 3 else ""
                title = parts[4] if len(parts) > 4 else ""
                key = SnapshotStore._channel_key(channel_id, title)
                if key:
                    channel_keys.add(key)
        return urls, channel_keys

    def has(self, url: str) -> bool:
        """True nếu url đã được kiểm tra trước đó (bất kể kết quả) — không xử lý lại."""
        return url in self._checked_urls

    def is_known_broadcast(self, channel_id: str, title: str) -> bool:
        """
        True nếu kênh này đã từng đăng đúng title này rồi (dù url lúc đó khác) — dấu
        hiệu của việc kênh ngắt live cũ rồi phát lại thành live mới cho cùng 1 camera.
        """
        key = self._channel_key(channel_id, title)
        return bool(key) and key in self._channel_keys

    def _record_checked(self, url: str, status: str, channel_id: str = "", title: str = "") -> None:
        """Ghi lại 1 url đã kiểm tra (mọi kết quả) vào CHECKED_HISTORY_PATH."""
        safe_title = title.replace("\t", " ").replace("\n", " ").strip()
        with open(self.history_path, "a", encoding="utf-8") as f:
            f.write(f"{url}{_FIELD_SEP}{status}{_FIELD_SEP}{get_timestamp()}{_FIELD_SEP}{channel_id}{_FIELD_SEP}{safe_title}\n")
        self._checked_urls.add(url)
        key = self._channel_key(channel_id, title)
        if key:
            self._channel_keys.add(key)

    def _capture_snapshot(self, url: str, video_id: str) -> Optional[str]:
        """Resolve luồng live rồi chụp 1 frame. Trả về đường dẫn ảnh đã lưu hoặc None."""
        vc = VideoCapture()
        stream_url = vc.resolve_youtube_url(url)
        if not stream_url:
            logger.warning(f"Could not resolve stream, skip: {url}")
            return None

        frame, ok = vc.capture_frame(stream_url)
        if not ok:
            logger.warning(f"Could not capture frame, skip: {url}")
            return None

        base_name = f"{video_id}_{get_timestamp()}"
        image_path = get_unique_filename(self.snapshot_dir, base_name, ".jpg")
        return save_image_cv2(frame, image_path)

    def add(self, url: str, image_path: str, title: str = "", channel_id: str = "") -> bool:
        """Ghi 1 dòng mới vào OUTPUT_TXT_PATH nếu url chưa có. Trả về True nếu vừa ghi thêm."""
        if url in self._known_urls:
            return False

        safe_title = title.replace("\t", " ").replace("\n", " ").strip()
        with open(self.txt_path, "a", encoding="utf-8") as f:
            f.write(f"{url}{_FIELD_SEP}{image_path}{_FIELD_SEP}{safe_title}{_FIELD_SEP}{channel_id}\n")

        self._known_urls.add(url)
        key = self._channel_key(channel_id, title)
        if key:
            self._channel_keys.add(key)
        logger.info(f"Recorded new live stream: {url} -> {image_path}")
        return True

    def process(self, url: str, video_id: str, title: str = "", channel_id: str = "") -> Optional[str]:
        """
        Xử lý 1 luồng ứng viên: bỏ qua nếu ĐÃ TỪNG kiểm tra (dù kết quả gì) hoặc kênh
        đã từng đăng đúng title này rồi (re-broadcast), ngược lại chụp ảnh, xác minh
        bằng model thị giác (nếu bật), rồi mới ghi vào file — luôn ghi lại vào lịch sử
        kiểm tra để không lặp lại việc này ở các vòng/ngày sau.

        Trả về đường dẫn ảnh nếu vừa thêm mới, None nếu đã kiểm tra rồi / là re-broadcast
        của luồng cũ / chụp thất bại / bị model xác định không phải CCTV.
        """
        if self.has(url):
            return None

        if self.is_known_broadcast(channel_id, title):
            logger.info(f"[SKIP - kênh đã phát luồng này trước đó (title trùng)] {url}")
            self.stats["duplicate_broadcast"] += 1
            self._record_checked(url, "duplicate_broadcast", channel_id, title)
            return None

        image_path = self._capture_snapshot(url, video_id)
        if not image_path:
            self.stats["capture_failed"] += 1
            self._record_checked(url, "capture_failed", channel_id, title)
            return None

        if self._classifier and not self._classifier.classify_image(image_path, label=title or url):
            logger.info(f"[SKIP - not CCTV by classifier] {url}")
            self.stats["rejected_not_cctv"] += 1
            try:
                os.remove(image_path)
            except Exception:
                pass
            self._record_checked(url, "rejected_not_cctv", channel_id, title)
            return None

        self.add(url, image_path, title, channel_id)
        self.stats["added"] += 1
        self._record_checked(url, "added", channel_id, title)
        return image_path
