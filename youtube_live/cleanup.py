"""Dọn OUTPUT_TXT_PATH: xoá các dòng có ảnh (cột 2) không còn tồn tại trong SNAPSHOT_DIR."""

import os

from src.modules.logger import default_logger as logger

from .config import OUTPUT_TXT_PATH

_FIELD_SEP = "\t"


def remove_entries_with_missing_images(txt_path: str = "data/videos/youtube_cctv/live_streams_check.txt", dry_run: bool = False) -> int:
    """
    Đọc lại txt_path, giữ các dòng có image_path (cột 2) còn tồn tại trên đĩa, xoá
    các dòng còn lại. dry_run=True chỉ log những dòng SẼ bị xoá, không ghi đè file.

    Trả về số dòng bị xoá (hoặc sẽ bị xoá nếu dry_run).
    """
    if not os.path.exists(txt_path):
        logger.warning(f"Không tìm thấy file: {txt_path}")
        return 0

    with open(txt_path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]

    kept, removed = [], []
    for line in lines:
        parts = line.split(_FIELD_SEP)
        image_path = parts[1] if len(parts) >= 2 else ""
        if image_path and os.path.exists(image_path):
            kept.append(line)
        else:
            removed.append(line)

    for line in removed:
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Xoá dòng vì ảnh không còn tồn tại: {line}")

    if removed and not dry_run:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(kept) + ("\n" if kept else ""))

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"{len(removed)}/{len(lines)} dòng bị xoá khỏi {txt_path}, còn lại {len(kept)}"
    )
    return len(removed)


def remove_duplicate_urls(txt_path: str = OUTPUT_TXT_PATH, dry_run: bool = False) -> int:
    """
    Nếu 1 url xuất hiện nhiều dòng (thường do 2 tiến trình từng chạy đè lên nhau),
    chỉ giữ dòng CUỐI CÙNG cho url đó (file ghi kiểu append nên dòng cuối luôn là
    bản mới nhất), xoá các dòng trùng phía trên. Không đụng tới file ảnh trên đĩa.

    Trả về số dòng bị xoá (hoặc sẽ bị xoá nếu dry_run).
    """
    if not os.path.exists(txt_path):
        logger.warning(f"Không tìm thấy file: {txt_path}")
        return 0

    with open(txt_path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]

    last_index_for_url = {}
    for i, line in enumerate(lines):
        url = line.split(_FIELD_SEP)[0]
        last_index_for_url[url] = i

    kept, removed = [], []
    for i, line in enumerate(lines):
        url = line.split(_FIELD_SEP)[0]
        if i == last_index_for_url[url]:
            kept.append(line)
        else:
            removed.append(line)

    for line in removed:
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Xoá dòng trùng url (giữ bản mới nhất): {line}")

    if removed and not dry_run:
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(kept) + ("\n" if kept else ""))

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"{len(removed)}/{len(lines)} dòng trùng url bị xoá khỏi {txt_path}, còn lại {len(kept)}"
    )
    return len(removed)


if __name__ == "__main__":
    import sys
    remove_entries_with_missing_images(dry_run="--dry-run" in sys.argv)
