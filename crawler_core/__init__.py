"""
crawler_core — code dùng chung cho MỌI platform (Facebook, YouTube, TikTok,
Instagram, X, Reddit, Vimeo, Dailymotion, ...).

  dedup.py       — Dedup 4 tầng, platform-agnostic
  db_manager.py  — SQLite tracker (dùng chung 1 DB cho tất cả platform)
  pipeline.py    — VideoPipeline: cổng BẮT BUỘC mọi video phải đi qua

Mỗi platform chỉ cần cung cấp danh sách URL; toàn bộ dedup/filter/download
do VideoPipeline xử lý → không platform nào bỏ sót bước dedup.
"""
