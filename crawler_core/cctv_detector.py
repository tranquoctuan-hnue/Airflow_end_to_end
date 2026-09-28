#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cctv_detector.py - Phát hiện các ĐOẠN quay bằng CCTV bên trong video.

Dùng được cho:
  * video phóng sự / tin tức ghép nhiều cảnh, chỉ có một đoạn là CCTV
  * camera PTZ có chế độ tracking (hình ảnh di chuyển)
  * video CCTV có âm thanh

Cách hoạt động
  1. Giải mã video một lượt bằng FFmpeg, tách thành các cảnh (shot) dựa trên
     độ thay đổi màu giữa hai khung hình liên tiếp.
  2. Chấm điểm TỪNG cảnh bằng:
       - CLIP (mô hình thị giác - ngôn ngữ): khung hình trông giống CCTV đến
         đâu (góc cao, ống kính rộng, chất lượng hình, bố cục...). Mặc định
         chạy zero-shot; huấn luyện thêm bằng lệnh `train` trên mẫu của chính
         bạn sẽ chính xác hơn rất nhiều.
       - Timestamp OSD: OCR tìm ngày giờ trên hình và kiểm tra đồng hồ có
         chạy theo thời gian video không. Chữ đồ hoạ của đài truyền hình
         không có đặc điểm này.
       - FPS thực: CCTV thường ghi 8-15 fps; khi ghép vào video 25/30 fps sẽ
         có nhiều khung hình lặp.
       - Tỷ lệ khung hình đen trắng (hồng ngoại ban đêm, tín hiệu yếu).
     KHÔNG dùng âm thanh và KHÔNG dùng việc camera đứng yên hay di chuyển.
  3. Video được xếp "cctv" khi tổng thời lượng các đoạn CCTV >= --min-cctv-seconds.

Cài đặt
  FFmpeg (ffmpeg + ffprobe trong PATH)
  pip install opencv-python numpy pillow torch open_clip_torch scikit-learn joblib tqdm
  Tuỳ chọn OCR : cài Tesseract (Windows: bản UB-Mannheim) rồi pip install pytesseract
  Lần chạy đầu sẽ tự tải trọng số CLIP (khoảng 600 MB).

Sử dụng
  # Quét thư mục; lưu ảnh các đoạn nghi ngờ để xem lại
  python cctv_detector.py scan D:/videos -o ketqua.csv --thumbs D:/xem_lai

  # Huấn luyện bộ phân loại riêng (khuyến nghị). Đặt ảnh chụp màn hình hoặc
  # clip ngắn vào 2 thư mục. Có thể lấy luôn ảnh trong D:/xem_lai, kéo vào
  # đúng thư mục sau khi xem.
  python cctv_detector.py train --cctv D:/mau/cctv --other D:/mau/khac -o probe.joblib

  # Quét lại bằng bộ phân loại đã huấn luyện
  python cctv_detector.py scan D:/videos -o ketqua.csv --probe probe.joblib

Dùng trong crawler (thay Qwen2.5-VL)
  crawler_core.pipeline.VideoPipeline gọi CCTVVideoClassifier.assess_video() trên
  file đã tải — xem class ở cuối file. Probe đã huấn luyện đặt tại
  models/cctv_probe.joblib sẽ được dùng tự động (hoặc env CCTV_DETECTOR_PROBE).
  Video từ thư mục video_rejected/not_cctv và dataset là nguồn mẫu train sẵn có.
"""

import argparse
import copy
import csv
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cv2
import numpy as np

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    import pytesseract
    pytesseract.get_tesseract_version()
    HAS_OCR = True
except Exception:
    HAS_OCR = False

logger = logging.getLogger(__name__)

NAN = float("nan")

VIDEO_EXTS = {".mp4", ".avi", ".mkv", ".mov", ".m4v", ".wmv", ".flv", ".ts",
              ".3gp", ".webm", ".mpg", ".mpeg", ".asf", ".dav", ".264",
              ".h264", ".265", ".h265"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
CCTV_FILE_EXTS = {".dav", ".264", ".h264", ".265", ".h265"}
CCTV_TAG_RE = re.compile(r"hikvision|dahua|dhav|uniview|ezviz|imou|kbvision|"
                         r"surveillance|\bnvr\b|\bdvr\b", re.I)
TIME_RE = re.compile(r"(?<!\d)([01]?\d|2[0-3])[:.]([0-5]\d)[:.]([0-5]\d)(?!\d)")
DATE_RE = re.compile(r"(?<!\d)(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|"
                     r"\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})(?!\d)")

# Câu mô tả cho CLIP zero-shot (tiếng Anh vì CLIP được huấn luyện bằng tiếng Anh)
CCTV_PROMPTS = [
    "CCTV footage",
    "security camera footage",
    "surveillance camera footage with a timestamp",
    "a high angle view from a security camera mounted on a wall",
    "grainy black and white night vision security camera footage",
    "CCTV video of a street",
    "CCTV video inside a shop",
    "a fisheye view from a ceiling security camera",
]
OTHER_PROMPTS = [
    "a news anchor in a TV studio",
    "a TV news report filmed by a cameraman",
    "a journalist interviewing a person",
    "a video filmed with a handheld camera",
    "a video filmed with a smartphone",
    "a professional cinematic shot",
    "dashcam footage from a car",
    "a screen with text and graphics",
    "an aerial drone shot",
    "a close-up of a person talking",
]


# ------------------------------------------------------------------ tiện ích
def isnum(x):
    return isinstance(x, (int, float, np.floating)) and not math.isnan(x)


def fmt_time(t):
    m, s = divmod(float(t), 60)
    h, m = divmod(int(m), 60)
    return f"{h}:{m:02d}:{s:04.1f}" if h else f"{m:02d}:{s:04.1f}"


def fmt_val(v):
    if isinstance(v, (float, np.floating)):
        return "" if math.isnan(v) else round(float(v), 4)
    return v


def imread_any(path):
    """cv2.imread không đọc được đường dẫn có dấu tiếng Việt trên Windows."""
    data = np.fromfile(str(path), dtype=np.uint8)
    return cv2.imdecode(data, cv2.IMREAD_COLOR) if data.size else None


def imwrite_any(path, img, quality=90):
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if ok:
        buf.tofile(str(path))


def collect(inputs, exts):
    files = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            files += [x for x in p.rglob("*")
                      if x.is_file() and x.suffix.lower() in exts]
        elif p.is_file():
            files.append(p)
        else:
            print(f"[!] Không tìm thấy: {inp}", file=sys.stderr)
    return sorted({f.resolve() for f in files})


def parse_rate(s):
    try:
        n, d = (float(x) for x in str(s).split("/"))
        r = n / d if d else NAN
        return r if 0 < r <= 120 else NAN
    except Exception:
        return NAN


# ------------------------------------------------------------------ ffprobe
def probe_video(path):
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", str(path)]
    try:
        info = json.loads(subprocess.run(cmd, capture_output=True, text=True,
                                         timeout=60).stdout or "{}")
    except Exception:
        return None
    streams = info.get("streams") or []
    fmt = info.get("format") or {}
    v = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not v:
        return None
    w, h = int(v.get("width") or 0), int(v.get("height") or 0)
    rot = 0
    try:
        rot = int(float((v.get("tags") or {}).get("rotate", 0)))
    except ValueError:
        pass
    for sd in v.get("side_data_list") or []:
        if sd.get("rotation"):
            rot = int(sd["rotation"])
    if abs(rot) % 180 == 90:
        w, h = h, w
    fps = parse_rate(v.get("avg_frame_rate"))
    if not isnum(fps):
        fps = parse_rate(v.get("r_frame_rate"))
    if not isnum(fps):
        fps = 25.0
    try:
        duration = float(fmt.get("duration") or v.get("duration") or NAN)
    except ValueError:
        duration = NAN
    tag_text = json.dumps(fmt.get("tags", {})) + json.dumps(v.get("tags", {}))
    file_cctv = (path.suffix.lower() in CCTV_FILE_EXTS
                 or fmt.get("format_name") == "dhav"
                 or bool(CCTV_TAG_RE.search(tag_text)))
    return {"w": w, "h": h, "fps": fps, "duration": duration,
            "file_cctv": file_cctv}


# -------------------------------------------------------- giải mã 1 lượt
def decode_pass(path, meta, work_width, keep_interval):
    """Giải mã toàn bộ video ở độ phân giải làm việc.

    Trả về: cdelta (độ thay đổi màu giữa 2 khung liên tiếp, để cắt cảnh),
            gdelta (độ thay đổi độ sáng, để đo khung hình lặp),
            kept   ({chỉ số khung: ảnh JPEG}) lưu mỗi keep_interval giây,
            n      (số khung hình).
    """
    W, H = meta["w"], meta["h"]
    if not (W and H):
        return None
    ow = max(2, min(work_width, W) // 2 * 2)
    oh = max(2, int(round(H * ow / W / 2)) * 2)
    size = ow * oh * 3
    keep_every = max(1, int(round(meta["fps"] * keep_interval)))

    for sync in (["-fps_mode", "passthrough"], ["-vsync", "0"]):
        cmd = ["ffmpeg", "-v", "error", "-nostdin", "-i", str(path),
               "-map", "0:v:0", "-an", "-sn", *sync,
               "-vf", f"scale={ow}:{oh}", "-f", "rawvideo",
               "-pix_fmt", "bgr24", "-"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL)
        cdelta, gdelta, kept = [], [], {}
        prev_hsv = prev_gray = None
        n = 0
        while True:
            buf = proc.stdout.read(size)
            if len(buf) < size:
                break
            fr = np.frombuffer(buf, np.uint8).reshape(oh, ow, 3)
            sm = cv2.resize(fr, (64, 36), interpolation=cv2.INTER_AREA)
            hsv = cv2.cvtColor(sm, cv2.COLOR_BGR2HSV).astype(np.int16)
            gray = cv2.cvtColor(sm, cv2.COLOR_BGR2GRAY).astype(np.float32)
            if prev_hsv is None:
                cdelta.append(0.0)
                gdelta.append(0.0)
            else:
                cdelta.append(float(np.abs(hsv - prev_hsv).mean()))
                gdelta.append(float(np.abs(gray - prev_gray).mean()))
            prev_hsv, prev_gray = hsv, gray
            if n % keep_every == 0:
                ok, jpg = cv2.imencode(".jpg", fr, [cv2.IMWRITE_JPEG_QUALITY, 92])
                if ok:
                    kept[n] = jpg
            n += 1
        proc.stdout.close()
        proc.wait()
        if n > 0:
            return np.array(cdelta), np.array(gdelta), kept, n
    return None


def split_shots(cdelta, fps, threshold, min_shot_s):
    n = len(cdelta)
    min_len = max(2, int(round(min_shot_s * fps)))
    cuts = [0]
    for i in range(1, n):
        if cdelta[i] >= threshold and i - cuts[-1] >= min_len:
            cuts.append(i)
    cuts.append(n)
    return [(cuts[k], cuts[k + 1]) for k in range(len(cuts) - 1)]


def frame_repeat(gdelta, s, e, fps):
    """Tỷ lệ khung hình lặp trong cảnh -> ước lượng fps thực của nguồn quay."""
    g = gdelta[s + 1:e]
    if len(g) < max(10, fps):
        return NAN, NAN
    m = np.percentile(g, 75)
    if m < 0.4:  # cảnh gần như không có chuyển động -> không đo được
        return NAN, NAN
    dup = float(np.mean(g < 0.15 * m))
    return dup, fps * (1 - dup)


# ----------------------------------------------------------------------- OCR
def ocr_times(img):
    """OCR dải trên và dưới khung hình. Trả về (các giờ đọc được, tính bằng
    giây trong ngày; có đọc được ngày hay không)."""
    h, w = img.shape[:2]
    sh = max(16, int(h * 0.16))
    scale = max(1.0, 1600 / w)
    times, has_date = [], False
    for strip in (img[:sh], img[h - sh:]):
        hsv = cv2.cvtColor(strip, cv2.COLOR_BGR2HSV)
        gray = cv2.cvtColor(strip, cv2.COLOR_BGR2GRAY)
        _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        white = np.where((hsv[..., 2] > 190) & (hsv[..., 1] < 50), 0, 255)
        for mask in (white, otsu):
            mask = cv2.resize(mask.astype(np.uint8), None, fx=scale, fy=scale,
                              interpolation=cv2.INTER_NEAREST)
            mask = cv2.copyMakeBorder(mask, 20, 20, 20, 20,
                                      cv2.BORDER_CONSTANT, value=255)
            try:
                txt = pytesseract.image_to_string(mask, config="--psm 6")
            except Exception:
                continue
            for mt in TIME_RE.finditer(txt):
                hh, mm, ss = (int(x) for x in mt.groups())
                times.append(hh * 3600 + mm * 60 + ss)
            has_date |= bool(DATE_RE.search(txt))
        if times and has_date:
            break
    return times, has_date


def timestamp_score(samples):
    """samples: [(thời điểm trong video, ảnh)].
    1.0  = đồng hồ trên hình chạy theo thời gian video (rất mạnh)
    0.75 = có cả ngày và giờ;  0.4 = chỉ có một trong hai;  0 = không có."""
    if not HAS_OCR or not samples:
        return NAN
    mid = len(samples) // 2
    order = [mid] + [i for i in range(len(samples)) if i != mid]
    reads = {}
    for k, i in enumerate(order):
        times, has_date = ocr_times(samples[i][1])
        reads[i] = (times, has_date)
        if k == 0 and not times and not has_date:
            return 0.0  # khung giữa không có chữ ngày giờ -> dừng sớm
    any_time = any(r[0] for r in reads.values())
    any_date = any(r[1] for r in reads.values())
    idx = sorted(reads)
    for a in range(len(idx)):
        for b in range(a + 1, len(idx)):
            ta, tb = samples[idx[a]][0], samples[idx[b]][0]
            dv = tb - ta
            if dv < 1.0:
                continue
            for x in reads[idx[a]][0]:
                for y in reads[idx[b]][0]:
                    if 0 < y - x <= 4 * dv + 2:  # cho phép video bị tua nhanh
                        return 1.0
    if any_time and any_date:
        return 0.75
    return 0.4 if (any_time or any_date) else 0.0


# ---------------------------------------------------------------------- CLIP
class ClipScorer:
    def __init__(self, model_name, pretrained, probe_path=None, device=None):
        import torch
        import open_clip
        from PIL import Image
        self.torch, self.Image = torch, Image
        self.probe = None
        if probe_path:
            import joblib
            obj = joblib.load(probe_path)
            self.probe = obj["clf"]
            model_name, pretrained = obj["clip_model"], obj["clip_pretrained"]
        self.model_name, self.pretrained = model_name, pretrained
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = device
        pre = None if str(pretrained).lower() == "none" else pretrained
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(
            model_name, pretrained=pre, device=device)
        self.model.eval()
        tok = open_clip.get_tokenizer(model_name)
        with torch.no_grad():
            cls = []
            for prompts in (CCTV_PROMPTS, OTHER_PROMPTS):
                t = self.model.encode_text(tok(prompts).to(device)).float()
                t = t / t.norm(dim=-1, keepdim=True)
                c = t.mean(0)
                cls.append(c / c.norm())
            self.text = torch.stack(cls).cpu().numpy()  # [cctv, other]
            self.scale = float(self.model.logit_scale.exp())

    def embed(self, bgr_list, batch=32):
        out = []
        for i in range(0, len(bgr_list), batch):
            x = self.torch.stack([
                self.preprocess(self.Image.fromarray(cv2.cvtColor(b, cv2.COLOR_BGR2RGB)))
                for b in bgr_list[i:i + batch]]).to(self.device)
            with self.torch.no_grad():
                e = self.model.encode_image(x).float()
                e = e / e.norm(dim=-1, keepdim=True)
            out.append(e.cpu().numpy())
        return np.vstack(out) if out else np.zeros((0, 1), np.float32)

    def prob(self, bgr_list):
        if not bgr_list:
            return np.zeros(0)
        emb = self.embed(bgr_list)
        if self.probe is not None:
            return self.probe.predict_proba(emb)[:, 1]
        logits = self.scale * emb @ self.text.T
        logits -= logits.max(1, keepdims=True)
        p = np.exp(logits)
        return p[:, 0] / p.sum(1)


# ----------------------------------------------------------- chấm điểm 1 đoạn
def score_segment(seg, fps, has_probe):
    s, why = 0.0, []

    def add(v, msg):
        nonlocal s
        s += v
        why.append(f"{msg} ({v:+.1f})")

    pv = seg["p_visual"]
    add((8 if has_probe else 5) * (pv - 0.5), f"hình ảnh giống CCTV {pv:.2f}")
    ts = seg["ts_score"]
    if isnum(ts):
        if ts >= 1:
            add(4, "đồng hồ trên hình chạy theo video")
        elif ts >= 0.75:
            add(3, "có ngày + giờ trên hình")
        elif ts >= 0.4:
            add(1.5, "có ngày hoặc giờ trên hình")
    ef = seg["eff_fps"]
    if isnum(ef) and fps >= 23 and ef <= 16:
        add(1.5, f"fps thực thấp ~{ef:.0f}")
    if isnum(seg["gray_ratio"]) and seg["gray_ratio"] >= 0.5:
        add(0.5, "hình đen trắng")
    prob = 1 / (1 + math.exp(-(s - 1.0)))
    return prob, "; ".join(why)


def seg_label(p, hi, lo):
    return "cctv" if p >= hi else "not_cctv" if p <= lo else "uncertain"


# ------------------------------------------------------------ phân tích video
def analyze_video(path, scorer, args):
    meta = probe_video(path)
    if meta is None:
        return None, [], "ffprobe không đọc được video"
    if meta["file_cctv"]:
        return meta, [], ""  # file gốc từ đầu ghi, không cần phân tích hình
    dec = decode_pass(path, meta, args.work_width, args.keep_interval)
    if dec is None:
        return meta, [], "không giải mã được video"
    cdelta, gdelta, kept, n = dec
    fps = meta["fps"]
    keep_idx = np.array(sorted(kept))

    segs = []
    for s, e in split_shots(cdelta, fps, args.cut_threshold, args.min_shot):
        dur = (e - s) / fps
        if dur < args.min_seg:
            continue
        inside = keep_idx[(keep_idx >= s) & (keep_idx < e)]
        if len(inside) == 0:
            continue
        k = min(args.frames_per_seg, len(inside))
        pick = inside[np.unique(np.linspace(0, len(inside) - 1, k).round().astype(int))]
        dup, eff = frame_repeat(gdelta, s, e, fps)
        segs.append({"start": s / fps, "end": e / fps, "duration": dur,
                     "frames": [int(i) for i in pick],
                     "dup_ratio": dup, "eff_fps": eff})
    if not segs:
        return meta, [], ""

    needed = sorted({i for g in segs for i in g["frames"]})
    imgs = {i: cv2.imdecode(kept[i], cv2.IMREAD_COLOR) for i in needed}
    del kept
    probs = dict(zip(needed, scorer.prob([imgs[i] for i in needed])))

    for g in segs:
        g["p_visual"] = float(np.mean([probs[i] for i in g["frames"]]))
        sats = [cv2.cvtColor(imgs[i], cv2.COLOR_BGR2HSV)[..., 1].mean()
                for i in g["frames"]]
        g["gray_ratio"] = float(np.mean([x < 12 for x in sats]))
        g["ts_score"] = NAN

    # OCR (chậm) chỉ chạy cho các đoạn có khả năng, trừ khi --ocr-all
    if HAS_OCR and not args.no_ocr:
        def need_ocr(g):
            return (args.ocr_all or g["p_visual"] >= args.ocr_min_visual
                    or (isnum(g["eff_fps"]) and g["eff_fps"] <= 16)
                    or g["gray_ratio"] >= 0.5)
        todo = [g for g in segs if need_ocr(g)]

        def run(g):
            g["ts_score"] = timestamp_score([(i / fps, imgs[i]) for i in g["frames"]])
        with ThreadPoolExecutor(max_workers=args.ocr_threads) as ex:
            list(ex.map(run, todo))

    for g in segs:
        g["prob"], g["reasons"] = score_segment(g, fps, scorer.probe is not None)
        g["label"] = seg_label(g["prob"], args.hi, args.lo)
        g["thumb"] = ""
        if args.thumbs and (args.thumbs_all or g["label"] != "not_cctv"):
            d = Path(args.thumbs) / g["label"]
            d.mkdir(parents=True, exist_ok=True)
            mid = g["frames"][len(g["frames"]) // 2]
            name = f"{path.stem}__{g['start']:08.1f}s__p{g['prob']:.2f}.jpg"
            imwrite_any(d / name, imgs[mid])
            g["thumb"] = str(d / name)
    return meta, segs, ""


def merge_intervals(segs):
    out = []
    for g in sorted(segs, key=lambda x: x["start"]):
        if out and g["start"] - out[-1][1] < 0.5:
            out[-1][1] = max(out[-1][1], g["end"])
            out[-1][2] = max(out[-1][2], g["prob"])
        else:
            out.append([g["start"], g["end"], g["prob"]])
    return out


def video_verdict(meta, segs, args):
    """Kết luận cho cả video từ các đoạn đã chấm điểm — dùng chung cho lệnh
    scan và CCTVVideoClassifier để hai nơi luôn cho cùng một nhãn."""
    cctv = [g for g in segs if g["prob"] >= args.hi]
    cctv_sec = sum(g["duration"] for g in cctv)
    maxp = max((g["prob"] for g in segs), default=0.0)
    dur = meta["duration"] if isnum(meta["duration"]) else \
        sum(g["duration"] for g in segs)
    if cctv_sec >= args.min_cctv_seconds:
        label = "cctv"
    elif maxp > args.lo:
        label = "uncertain"
    else:
        label = "not_cctv"
    return {"label": label, "maxp": maxp, "cctv": cctv, "cctv_sec": cctv_sec,
            "dur": dur}


# ----------------------------------------------------------------- lệnh scan
def cmd_scan(args):
    for tool in ("ffprobe", "ffmpeg"):
        if not shutil.which(tool):
            sys.exit(f"Không tìm thấy {tool}. Hãy cài FFmpeg và thêm vào PATH.")
    if not args.no_ocr and not HAS_OCR:
        print("[i] Chưa có Tesseract/pytesseract -> bỏ qua OCR timestamp "
              "(độ chính xác sẽ giảm).", file=sys.stderr)

    print("Đang nạp mô hình CLIP...")
    scorer = ClipScorer(args.clip_model, args.clip_pretrained, args.probe, args.device)
    print(f"  {scorer.model_name} / {scorer.pretrained} trên {scorer.device}; "
          f"{'dùng bộ phân loại đã huấn luyện' if scorer.probe is not None else 'zero-shot'}")

    out = Path(args.output)
    seg_out = Path(args.segments) if args.segments else out.with_name(out.stem + "_segments.csv")
    files = collect(args.inputs, VIDEO_EXTS)
    done = set()
    if args.resume and out.exists():
        with open(out, encoding="utf-8-sig", newline="") as fh:
            done = {r["file"] for r in csv.DictReader(fh)}
    todo = [p for p in files if str(p) not in done]
    print(f"Tìm thấy {len(files)} video, cần xử lý {len(todo)}.")
    if not todo:
        return

    vfields = ["file", "label", "max_prob", "cctv_seconds", "duration",
               "cctv_ratio", "n_segments", "n_cctv_segments", "cctv_intervals",
               "note", "error"]
    sfields = ["file", "seg", "start", "end", "duration", "label", "prob",
               "p_visual", "ts_score", "eff_fps", "dup_ratio", "gray_ratio",
               "reasons", "thumb"]
    append = args.resume and out.exists()
    mode = "a" if append else "w"
    counts = Counter()
    with open(out, mode, encoding="utf-8-sig", newline="") as fv, \
         open(seg_out, "a" if (append and seg_out.exists()) else "w",
              encoding="utf-8-sig", newline="") as fs:
        wv = csv.DictWriter(fv, vfields)
        ws = csv.DictWriter(fs, sfields)
        if not append:
            wv.writeheader()
        if fs.tell() == 0:
            ws.writeheader()

        it = tqdm(todo, unit="video") if tqdm else todo
        for path in it:
            try:
                meta, segs, err = analyze_video(path, scorer, args)
            except Exception as e:
                meta, segs, err = None, [], repr(e)
            row = {"file": str(path), "error": err, "note": ""}
            if err:
                row["label"] = "error"
            elif meta["file_cctv"]:
                row.update(label="cctv", max_prob=1.0, note="file gốc từ đầu ghi / camera",
                           duration=fmt_val(meta["duration"]))
            else:
                v = video_verdict(meta, segs, args)
                dur = v["dur"]
                row.update(
                    label=v["label"], max_prob=fmt_val(v["maxp"]),
                    cctv_seconds=round(v["cctv_sec"], 1), duration=fmt_val(dur),
                    cctv_ratio=fmt_val(v["cctv_sec"] / dur if dur else NAN),
                    n_segments=len(segs), n_cctv_segments=len(v["cctv"]),
                    cctv_intervals=" | ".join(
                        f"{fmt_time(a)}-{fmt_time(b)} ({p:.2f})"
                        for a, b, p in merge_intervals(v["cctv"])))
                for k, g in enumerate(segs):
                    ws.writerow({"file": str(path), "seg": k,
                                 "start": round(g["start"], 2), "end": round(g["end"], 2),
                                 "duration": round(g["duration"], 2),
                                 **{c: fmt_val(g[c]) for c in (
                                     "label", "prob", "p_visual", "ts_score",
                                     "eff_fps", "dup_ratio", "gray_ratio",
                                     "reasons", "thumb")}})
            wv.writerow(row)
            fv.flush()
            fs.flush()
            counts[row["label"]] += 1
            if args.sort_into and row["label"] != "error":
                d = Path(args.sort_into) / row["label"]
                d.mkdir(parents=True, exist_ok=True)
                dest, i = d / path.name, 1
                while dest.exists():
                    dest, i = d / f"{path.stem}_{i}{path.suffix}", i + 1
                shutil.copy2(path, dest)

    print("\nKết quả:", ", ".join(f"{k}: {v}" for k, v in counts.most_common()))
    print(f"Theo video : {out}\nTheo đoạn  : {seg_out}")


# ---------------------------------------------------------------- lệnh train
def load_samples(path, n_frames):
    if path.suffix.lower() in IMAGE_EXTS:
        img = imread_any(path)
        return [img] if img is not None else []
    cap = cv2.VideoCapture(str(path))
    cnt = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    imgs = []
    if cnt > 0:
        for idx in np.linspace(cnt * 0.05, cnt * 0.95, n_frames).astype(int):
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(idx))
            ok, fr = cap.read()
            if ok:
                imgs.append(fr)
    cap.release()
    return imgs


def cmd_train(args):
    import joblib
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedGroupKFold, cross_validate

    scorer = ClipScorer(args.clip_model, args.clip_pretrained, None, args.device)
    X, y, groups = [], [], []
    for label, dirs in ((1, args.cctv), (0, args.other)):
        files = collect(dirs, VIDEO_EXTS | IMAGE_EXTS)
        it = tqdm(files, desc="CCTV" if label else "Khác") if tqdm else files
        for f in it:
            imgs = load_samples(f, args.frames_per_video)
            if not imgs:
                continue
            X.append(scorer.embed(imgs))
            y += [label] * len(imgs)
            groups += [str(f)] * len(imgs)
    if not X:
        sys.exit("Không đọc được mẫu nào.")
    X, y = np.vstack(X), np.array(y)
    n_files = {c: len({g for g, t in zip(groups, y) if t == c}) for c in (0, 1)}
    print(f"Mẫu: {len(y)} khung hình từ {n_files[1]} file CCTV và {n_files[0]} file khác.")
    if min(n_files.values()) < 2:
        sys.exit("Cần ít nhất 2 file mỗi lớp (khuyến nghị >= 100).")

    clf = LogisticRegression(C=args.C, class_weight="balanced", max_iter=5000)
    k = min(5, min(n_files.values()))
    cv = StratifiedGroupKFold(n_splits=k, shuffle=True, random_state=0)
    res = cross_validate(clf, X, y, groups=groups, cv=cv,
                         scoring=("accuracy", "precision", "recall", "f1"))
    print(f"Đánh giá chéo {k}-fold (tách theo file, không lẫn khung của cùng file):")
    for m in ("accuracy", "precision", "recall", "f1"):
        v = res[f"test_{m}"]
        print(f"  {m:9s}: {v.mean():.3f} ± {v.std():.3f}")
    clf.fit(X, y)
    joblib.dump({"clf": clf, "clip_model": scorer.model_name,
                 "clip_pretrained": scorer.pretrained}, args.output)
    print(f"Đã lưu: {args.output}")


# ------------------------------------------------ dùng trong VideoPipeline
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Probe đã huấn luyện (lệnh train) đặt ở đây sẽ được dùng tự động.
DEFAULT_PROBE = os.path.join(REPO_ROOT, "models", "cctv_probe.joblib")


def _env_float(name, default):
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


class CCTVVideoClassifier:
    """
    Bộ phân loại CCTV cho crawler_core.pipeline.VideoPipeline (thay Qwen2.5-VL).

    Chấm điểm TOÀN BỘ file đã tải (tách cảnh → CLIP + OCR timestamp + fps thực
    + đen trắng), không phải 1 thumbnail. Tham số giống hệt lệnh `scan` (một
    nguồn mặc định duy nhất là argparse bên dưới), override bằng env:

        CCTV_DETECTOR_PROBE              đường dẫn probe.joblib
                                         (mặc định models/cctv_probe.joblib nếu có,
                                         không có thì chạy zero-shot)
        CCTV_DETECTOR_DEVICE             cuda | cpu (bỏ trống = tự chọn)
        CCTV_DETECTOR_HI / _LO           ngưỡng đoạn cctv / not_cctv (0.7 / 0.3)
        CCTV_DETECTOR_MIN_CCTV_SECONDS   tổng giây CCTV tối thiểu (2.0)
    """

    def __init__(self, probe=None, device=None):
        args = build_parser().parse_args(["scan", "_"])
        args.hi = _env_float("CCTV_DETECTOR_HI", args.hi)
        args.lo = _env_float("CCTV_DETECTOR_LO", args.lo)
        args.min_cctv_seconds = _env_float(
            "CCTV_DETECTOR_MIN_CCTV_SECONDS", args.min_cctv_seconds)
        probe = probe or os.environ.get("CCTV_DETECTOR_PROBE", "").strip() or (
            DEFAULT_PROBE if os.path.exists(DEFAULT_PROBE) else None)
        device = device or os.environ.get("CCTV_DETECTOR_DEVICE", "").strip() or None

        self.args = args
        self.scorer = ClipScorer(args.clip_model, args.clip_pretrained, probe, device)
        self.name = (f"clip:{self.scorer.model_name}/{self.scorer.pretrained}"
                     f"{' + probe' if self.scorer.probe is not None else ' zero-shot'}"
                     f"{' + OCR' if HAS_OCR else ' (không OCR)'}")
        logger.info(f"[CCTV] Bộ phân loại sẵn sàng: {self.name} trên {self.scorer.device}"
                    + (f" | probe: {probe}" if probe else ""))
        if not HAS_OCR:
            logger.warning("[CCTV] Không có Tesseract/pytesseract — bỏ qua OCR "
                           "timestamp, độ chính xác giảm")

    def assess_video(self, path, thumbs_dir=None) -> dict:
        """
        Trả về:
            {'is_cctv': bool, 'label': 'cctv'|'not_cctv'|'uncertain', 'max_prob',
             'cctv_seconds', 'cctv_ratio', 'cctv_intervals', 'top_segments', ...,
             'frame': ảnh đoạn có điểm cao nhất (nằm trong thumbs_dir) | None}
            {'is_cctv': None, 'invalid_image': True, 'error'}  không đọc được video
            {'is_cctv': None, 'error'}                         lỗi (GPU/CLIP...)
        """
        args = copy.copy(self.args)
        args.thumbs = thumbs_dir
        args.thumbs_all = True
        try:
            meta, segs, err = analyze_video(Path(path), self.scorer, args)
        except Exception as e:
            logger.error(f"[CCTV] Lỗi phân tích {path}: {e}", exc_info=True)
            return {"is_cctv": None, "error": f"{type(e).__name__}: {e}"}
        if err:
            return {"is_cctv": None, "invalid_image": True, "error": err}

        base = {"model": self.name}
        if meta["file_cctv"]:
            return {**base, "is_cctv": True, "label": "cctv", "max_prob": 1.0,
                    "note": "file gốc từ đầu ghi / camera"}

        v = video_verdict(meta, segs, args)
        dur = v["dur"]
        top = sorted(segs, key=lambda g: -g["prob"])
        return {
            **base,
            "is_cctv": v["label"] == "cctv",
            "label": v["label"],
            "max_prob": round(v["maxp"], 3),
            "cctv_seconds": round(v["cctv_sec"], 1),
            "duration": fmt_val(dur),
            "cctv_ratio": fmt_val(v["cctv_sec"] / dur if dur else NAN),
            "n_segments": len(segs),
            "n_cctv_segments": len(v["cctv"]),
            "cctv_intervals": " | ".join(
                f"{fmt_time(a)}-{fmt_time(b)} ({p:.2f})"
                for a, b, p in merge_intervals(v["cctv"])),
            "top_segments": [
                {"start": round(g["start"], 1), "end": round(g["end"], 1),
                 "prob": round(g["prob"], 3), "label": g["label"],
                 "reasons": g["reasons"]}
                for g in top[:5]],
            "frame": (top[0].get("thumb") or None) if top else None,
        }


# ---------------------------------------------------------------------- main
def build_parser():
    ap = argparse.ArgumentParser(description="Phát hiện đoạn CCTV trong video")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--clip-model", default="ViT-B-32",
                        help="Kiến trúc CLIP (ViT-L-14 chính xác hơn nhưng chậm hơn)")
    common.add_argument("--clip-pretrained", default="laion2b_s34b_b79k")
    common.add_argument("--device", default=None, help="cuda / cpu (tự chọn nếu bỏ trống)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", parents=[common], help="Quét video")
    s.add_argument("inputs", nargs="+", help="File hoặc thư mục video")
    s.add_argument("-o", "--output", default="cctv_videos.csv")
    s.add_argument("--segments", help="CSV theo đoạn (mặc định <output>_segments.csv)")
    s.add_argument("--probe", help="Bộ phân loại .joblib từ lệnh train")
    s.add_argument("--hi", type=float, default=0.7, help="Ngưỡng kết luận đoạn là CCTV")
    s.add_argument("--lo", type=float, default=0.3, help="Ngưỡng kết luận không phải")
    s.add_argument("--min-cctv-seconds", type=float, default=2.0,
                   help="Tổng thời lượng CCTV tối thiểu để xếp video là cctv")
    s.add_argument("--min-seg", type=float, default=1.0, help="Bỏ qua cảnh ngắn hơn (giây)")
    s.add_argument("--min-shot", type=float, default=0.5, help="Độ dài cảnh tối thiểu khi cắt")
    s.add_argument("--cut-threshold", type=float, default=27.0,
                   help="Ngưỡng cắt cảnh; giảm nếu cắt thiếu, tăng nếu cắt vụn")
    s.add_argument("--frames-per-seg", type=int, default=3)
    s.add_argument("--work-width", type=int, default=960,
                   help="Độ rộng xử lý; tăng lên nếu timestamp chữ nhỏ")
    s.add_argument("--keep-interval", type=float, default=0.5,
                   help="Lưu 1 khung mỗi N giây để phân tích")
    s.add_argument("--no-ocr", action="store_true")
    s.add_argument("--ocr-all", action="store_true", help="OCR mọi đoạn (chậm hơn)")
    s.add_argument("--ocr-min-visual", type=float, default=0.2)
    s.add_argument("--ocr-threads", type=int, default=4)
    s.add_argument("--thumbs", help="Thư mục lưu ảnh các đoạn để xem lại")
    s.add_argument("--thumbs-all", action="store_true", help="Lưu ảnh cả đoạn not_cctv")
    s.add_argument("--sort-into", help="Copy video vào thư mục theo nhãn")
    s.add_argument("--resume", action="store_true")
    s.set_defaults(func=cmd_scan)

    t = sub.add_parser("train", parents=[common], help="Huấn luyện bộ phân loại")
    t.add_argument("--cctv", nargs="+", required=True, help="Thư mục ảnh/clip CCTV")
    t.add_argument("--other", nargs="+", required=True, help="Thư mục ảnh/clip không phải CCTV")
    t.add_argument("-o", "--output", default="cctv_probe.joblib")
    t.add_argument("--frames-per-video", type=int, default=8)
    t.add_argument("--C", type=float, default=1.0, help="Hệ số điều chuẩn")
    t.set_defaults(func=cmd_train)
    return ap


def main():
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
