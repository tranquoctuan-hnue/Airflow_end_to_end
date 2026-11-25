"""
Ghi lại đoạn 5 giây có sự việc từ camera đang phát (RTSP hoặc YouTube live), chấm bằng
model VideoMAE multi-label 368x640 — dùng cho DAG camera_dataset_video_events.

Mỗi camera được xem `minutes` phút rồi chuyển sang camera kế (xoay vòng). Luồng được cắt
thành các đoạn liền nhau dài `segment_s` giây; mỗi đoạn được chấm Y HỆT
~/Documents/VideoMAE/segment_infer.py (16 frame giữa 16 khoảng đều nhau — temporal
sampling "mid" — rồi letterbox 368x640). Đoạn nào có ít nhất 1 đầu ra > `threshold`
(mặc định 0,40) thì giữ file mp4 5 giây của đúng đoạn đó + file annotation
video_classification cạnh nó (mở thẳng bằng công cụ gán nhãn).

Vì sao 1 camera mỗi lúc chứ không chạy song song: ViT-L 368x640 trên GTX 1660 SUPER tốn
3,9 s cho mỗi đoạn 5 s (đo 2026-11-25, fp32, batch 1; batch 2 còn chậm hơn: 4,3 s/clip),
tức GPU chỉ theo kịp ĐÚNG 1 luồng thời gian thực. fp16 trên card này chậm hơn ~5 lần.

Cách lấy đúng đoạn video đã chấm: 1 tiến trình ffmpeg cho mỗi camera, filtergraph
`fps=F,split` thành 2 nhánh cùng frame:
  1. rawvideo RGB → pipe → Python giữ 16 frame model cần của mỗi đoạn;
  2. libx264 → muxer segment, ép keyframe đúng mỗi `segment_s` giây → file thứ k chứa
     ĐÚNG frame [k*N, (k+1)*N) với N = segment_s * F (F làm tròn thành số nguyên để
     N nguyên, vd 29,97 → 30 fps).
Các file đoạn nằm tạm trong RING_DIR (ổ local); đoạn không đạt bị xóa sau ~4 đoạn,
đoạn đạt được chép sang `<output>/<camera>/<YYYY-MM-DD>/`.

Output có thể là NAS (gvfs/cifs). Ghi NAS lỗi (mount rớt, mạng) thì đoạn đó được lưu vào
LOCAL_OUTPUT_DIR thay vì mất — log báo WARNING, chép tay lên NAS sau.

Đọc frame bằng ffmpeg (không decord): decord giải mã nền cả luồng vào RAM (xem
crawler_core/videomae_filter.py), máy .35 chỉ có 15 GB.

Biến môi trường (mặc định trong ngoặc):
    CAMERA_EVENT_MODEL_DIR     models/Video_understanding/vitl_368x640_ml16_231126
                               (checkpoint-best.pth bản gọn + thresholds.json)
    CAMERA_EVENT_CHECKPOINT    <MODEL_DIR>/checkpoint-best.pth
    CAMERA_EVENT_THRESHOLDS    <MODEL_DIR>/thresholds.json — ngưỡng riêng từng đầu ra
                               (best-F1 trên val), chỉ dùng để GHI NHÃN cho file
                               annotation, không quyết định lưu hay không
    CAMERA_EVENT_THRESHOLD     0.40   lưu đoạn khi có đầu ra > ngưỡng này
    CAMERA_EVENT_MINUTES       5      số phút xem mỗi camera trước khi chuyển
    CAMERA_EVENT_SEGMENT_S     5      độ dài đoạn (phải khớp lúc train: 5)
    CAMERA_EVENT_OUTPUT_DIR    data/camera_events — nơi lưu đoạn đạt, đặt được là NAS
    CAMERA_EVENT_RING_DIR      data/camera_events/.ring — file đoạn tạm, ffmpeg ghi liên
                               tục 5s/lần nên PHẢI ở ổ local, không để trên NAS
    CAMERA_EVENT_DEVICE       cuda | cpu (bỏ trống = tự chọn)
    CAMERA_EVENT_CRF           23     chất lượng libx264 của file đoạn lưu lại
    CAMERA_EVENT_MAX_HEIGHT    720    luồng cao hơn được thu nhỏ (YouTube: chọn luồng ≤ mức
                                      này; RTSP: ffmpeg thu nhỏ trước khi chấm và ghi).
                                      Model chỉ nhận 368x640 nên 720 vẫn dư; để 1080 thì
                                      giải mã + x264 1080p30 làm mỗi đoạn chậm lên 5,2–5,7s
                                      và bắt đầu rớt đoạn (đo 2026-11-25, luồng YouTube)
"""

import datetime as dt
import json
import logging
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

import numpy as np

from src.modules.rtsp_credentials import redact, redact_text
from src.modules.time_zone import _now

# Nhánh airflow.task.*: Airflow 3 bỏ log INFO của logger thường (không hiện trong log task,
# kiểm tra 2026-11-25 bằng airflow tasks test); ngoài Airflow vẫn chạy như logger thường.
logger = logging.getLogger('airflow.task.camera_event_recorder')

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
VIDEOMAE_DIR = os.path.join(PROJECT_ROOT, 'crawler_core', 'third_party', 'videomae')


def _env(name, default):
    v = os.environ.get(name, '').strip()
    return v if v else default


MODEL_DIR = _env('CAMERA_EVENT_MODEL_DIR', os.path.join(
    PROJECT_ROOT, 'models', 'Video_understanding', 'vitl_368x640_ml16_231126'))
CHECKPOINT = _env('CAMERA_EVENT_CHECKPOINT', os.path.join(MODEL_DIR, 'checkpoint-best.pth'))
THRESHOLDS = _env('CAMERA_EVENT_THRESHOLDS', os.path.join(MODEL_DIR, 'thresholds.json'))
THRESHOLD = float(_env('CAMERA_EVENT_THRESHOLD', '0.40'))
MINUTES = float(_env('CAMERA_EVENT_MINUTES', '5'))
SEGMENT_S = int(float(_env('CAMERA_EVENT_SEGMENT_S', '5')))
LOCAL_OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'camera_events')
OUTPUT_DIR = _env('CAMERA_EVENT_OUTPUT_DIR', LOCAL_OUTPUT_DIR)
RING_DIR = _env('CAMERA_EVENT_RING_DIR', os.path.join(LOCAL_OUTPUT_DIR, '.ring'))
DEVICE = _env('CAMERA_EVENT_DEVICE', '')
CRF = int(_env('CAMERA_EVENT_CRF', '23'))
MAX_HEIGHT = int(_env('CAMERA_EVENT_MAX_HEIGHT', '720'))

# Mở camera: ffprobe chờ tối đa bao lâu, rớt luồng giữa chừng thì chờ bao lâu rồi nối lại.
PROBE_TIMEOUT_S = 20
RECONNECT_S = 3
# Giữ file đoạn trong .ring thêm bao nhiêu đoạn sau khi đọc xong. Luồng chấm điểm trễ
# tối đa 2 đoạn (1 đang chấm + hàng đợi 1), nên 4 là đủ an toàn.
RING_KEEP = 4


def _die_with_parent():
    """preexec_fn của ffmpeg: tiến trình Python bị kill (systemd-oomd, Airflow timeout) thì
    ffmpeg chết theo. Không có cái này ffmpeg mồ côi chạy mãi, giữ ~1 GB RAM (gặp
    2026-11-25 khi oomd kill bản test)."""
    try:
        import ctypes
        ctypes.CDLL('libc.so.6').prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG
    except Exception:
        pass


def output_size(w, h, max_h=MAX_HEIGHT):
    """Kích thước chẵn (yuv420p cần chẵn) sau khi giới hạn chiều cao, giữ tỉ lệ."""
    if h > max_h:
        nh = max_h - max_h % 2
        return int(round(w * nh / h / 2)) * 2, nh
    return w - w % 2, h - h % 2


def mid_bin_indices(frames_len, num_seg):
    """Frame giữa của mỗi khoảng trong num_seg khoảng đều nhau — bản sao từng dòng của
    mid_bin_indices trong ~/Documents/VideoMAE/kinetics.py (--temporal_sampling mid)."""
    average_dur = int(frames_len / num_seg)
    indices = []
    for i in range(num_seg):
        if average_dur >= 1:
            idx = (average_dur - 1) // 2 + i * average_dur
        else:
            idx = i % frames_len
        indices.append(idx)
    return indices


class VideoMAEMultiLabel:
    """Model VideoMAE multi-label (sigmoid từng đầu ra), cấu hình đọc từ ckpt['args']."""

    def __init__(self, checkpoint=CHECKPOINT, thresholds_path=THRESHOLDS, device=DEVICE):
        import torch
        if VIDEOMAE_DIR not in sys.path:
            sys.path.insert(0, VIDEOMAE_DIR)
        import modeling_finetune  # noqa: F401  (đăng ký vit_* vào timm)
        from clip_preprocess import letterbox_clip
        from multilabel import Taxonomy
        from timm.models import create_model

        if not os.path.exists(checkpoint):
            raise FileNotFoundError(
                f'Không có model {checkpoint}. Chép thư mục vitl_368x640_ml16_231126 vào '
                f'models/Video_understanding/ hoặc đặt CAMERA_EVENT_MODEL_DIR')
        # mmap: checkpoint gốc 3,4 GB có 2/3 là optimizer — chỉ đọc phần trọng số
        ckpt = torch.load(checkpoint, map_location='cpu', weights_only=False, mmap=True)
        a = ckpt['args']
        if not getattr(a, 'multi_label', False) or getattr(a, 'frame_fit', 'crop') != 'letterbox':
            raise ValueError(f'{checkpoint}: DAG này chỉ chạy model multi-label letterbox '
                             f'(multi_label={getattr(a, "multi_label", False)}, '
                             f'frame_fit={getattr(a, "frame_fit", "crop")})')
        if (getattr(a, 'temporal_sampling', None) or 'dense') != 'mid':
            raise ValueError(f'{checkpoint}: cần --temporal_sampling mid, '
                             f'checkpoint có {getattr(a, "temporal_sampling", None)}')
        self.taxonomy = Taxonomy(a.label_taxonomy)
        self.outputs = list(self.taxonomy.outputs)
        self.num_frames = int(a.num_frames)
        self.input_hw = tuple(a.input_hw)
        th = {}
        if thresholds_path and os.path.exists(thresholds_path):
            with open(thresholds_path, encoding='utf-8') as f:
                th = json.load(f)
        else:
            logger.warning(f'[VideoMAE] Không có {thresholds_path}: nhãn annotation dùng ngưỡng 0,5')
        self.thresholds = [float(th.get(n, 0.5)) for n in self.outputs]

        model = create_model(
            a.model, pretrained=False, num_classes=a.nb_classes, all_frames=a.num_frames,
            tubelet_size=getattr(a, 'tubelet_size', 2),
            use_mean_pooling=getattr(a, 'use_mean_pooling', True),
            pool=getattr(a, 'pool', 'mean'), topk_ratio=getattr(a, 'topk_ratio', 0.05),
            init_scale=0.001, drop_path_rate=0.0, img_size=self.input_hw)
        r = model.load_state_dict(ckpt['model'], strict=False)
        if r.missing_keys or r.unexpected_keys:
            raise RuntimeError(f'{checkpoint}: state_dict lệch, thiếu {r.missing_keys[:5]}, '
                               f'thừa {r.unexpected_keys[:5]}')
        del ckpt
        self.device = torch.device(device or ('cuda' if torch.cuda.is_available() else 'cpu'))
        # fp32: GTX 1660 SUPER không có tensor core, fp16 chậm hơn ~5 lần (đo 2026-09-28)
        self.model = model.to(self.device).eval()
        self._torch = torch
        self._letterbox = letterbox_clip
        self.name = (f'{a.model} {self.input_hw[0]}x{self.input_hw[1]}, '
                     f'{len(self.outputs)} đầu ra, pool {getattr(a, "pool", "mean")}')

    def score(self, frames):
        """frames: list num_frames ảnh [H, W, 3] uint8 RGB → xác suất từng đầu ra (numpy)."""
        torch = self._torch
        x = self._letterbox(np.stack(frames, 0), self.input_hw).unsqueeze(0).to(self.device)
        with torch.inference_mode():
            return torch.sigmoid(self.model(x).float())[0].cpu().numpy()

    def label(self, probs, threshold):
        """Nhãn gán tay (theo bảng ưu tiên) mà các đầu ra > threshold giải mã ra."""
        name, _ = self.taxonomy.decode(probs, [threshold] * len(self.outputs))
        return name


def is_youtube(url):
    return bool(re.match(r'https?://(www\.|m\.)?(youtube\.com|youtu\.be)/', url))


def resolve_stream(url):
    """Link YouTube live → link HLS đọc được bằng ffmpeg (mỗi lần mở lại camera vì link
    hết hạn sau vài giờ). Live chỉ có luồng hình RIÊNG (không có định dạng gộp tiếng),
    nên phải chọn bv*: 'best' báo 'Requested format is not available' (gặp 2026-11-25)."""
    if not is_youtube(url):
        return url
    from yt_dlp import YoutubeDL
    fmt = f'bv*[height<={MAX_HEIGHT}]/b[height<={MAX_HEIGHT}]/bv*/b'
    with YoutubeDL({'format': fmt, 'quiet': True, 'no_warnings': True, 'skip_download': True}) as ydl:
        info = ydl.extract_info(url, download=False)
    direct = info.get('url') or next((f['url'] for f in info.get('requested_formats') or [] if f.get('url')), None)
    if not direct:
        raise RuntimeError('yt-dlp không trả về link phát')
    return direct


def camera_name(url):
    """Tên thư mục cho 1 camera, không chứa tài khoản: 192.169.2.179_stream1, yt_<id>."""
    m = re.search(r'(?:v=|youtu\.be/|/live/)([\w-]{11})', url)
    if is_youtube(url) and m:
        return f'yt_{m.group(1)}'
    clean = re.sub(r'^\w+://', '', redact(url)).replace('***@', '')
    clean = re.sub(r'[^0-9A-Za-z._-]+', '_', clean).strip('_')
    return clean[:80] or 'camera'


def probe(url):
    """(width, height, fps) của luồng hình đầu tiên."""
    cmd = ['ffprobe', '-v', 'error']
    if url.startswith('rtsp'):
        cmd += ['-rtsp_transport', 'tcp']
    cmd += ['-select_streams', 'v:0', '-show_entries',
            'stream=width,height,r_frame_rate,avg_frame_rate', '-of', 'json', url]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=PROBE_TIMEOUT_S)
    streams = json.loads(out.stdout or '{}').get('streams') or []
    if not streams:
        raise RuntimeError(f'không đọc được luồng hình: {redact_text(out.stderr.strip()[-200:])}')
    s = streams[0]

    def rate(r):
        n, _, d = str(r or '0/1').partition('/')
        try:
            return float(n) / float(d or 1)
        except (ValueError, ZeroDivisionError):
            return 0.0

    # Ưu tiên avg_frame_rate: camera 14.241.35.248 báo r_frame_rate 100/1 nhưng avg 15/1,
    # đếm thật 15,1 fps (2026-11-25) — tin r_frame_rate thì fps filter nhân frame lên 100
    # (500 frame/đoạn 4K, CPU không kịp). r_frame_rate chỉ dùng khi avg không có (0/0).
    fps = rate(s.get('avg_frame_rate'))
    if not 0 < fps <= 120:
        fps = rate(s.get('r_frame_rate'))
    return int(s['width']), int(s['height']), (fps if 0 < fps <= 120 else 25.0)


class _Reader(threading.Thread):
    """Chạy ffmpeg cho 1 camera tới hết `deadline`, cắt frame thành đoạn N frame, chỉ giữ
    các frame model cần, đẩy đoạn vào hàng đợi (đầy thì bỏ đoạn CŨ: GPU chậm hơn thời
    gian thực thì kết quả vẫn bám sát hiện tại thay vì trễ dần)."""

    def __init__(self, url, w, h, fps, n, order, ring_dir, deadline, out_q):
        super().__init__(daemon=True)
        self.url, self.w, self.h, self.fps, self.n = url, w, h, fps, n
        self.order, self.need = list(order), set(order)   # chỉ số frame model nhận, theo thứ tự
        self.ring_dir, self.deadline, self.q = ring_dir, deadline, out_q
        self.keep = set()            # (conn, k) mà luồng chấm điểm đã chọn lưu
        self.keep_lock = threading.Lock()
        self.proc = None
        self.conn = 0                # lần kết nối thứ mấy trong phiên (mỗi lần 1 thư mục con)
        self.exited = {}             # conn → ffmpeg của lần đó đã thoát hẳn (file đoạn cuối đã đóng)
        self.segments = 0
        self.dropped = 0
        self.reconnects = 0
        self.error = None

    def seg_path(self, conn, k):
        return os.path.join(self.ring_dir, f'c{conn}', f'seg_{k:06d}.mp4')

    def _cmd(self, conn_dir):
        cmd = ['ffmpeg', '-nostdin', '-hide_banner', '-loglevel', 'error']
        if self.url.startswith('rtsp'):
            cmd += ['-rtsp_transport', 'tcp']
        # Thu nhỏ (nếu cao hơn MAX_HEIGHT) + ép kích thước chẵn TRƯỚC split: 2 nhánh cùng
        # frame, cùng kích thước self.w x self.h.
        graph = (f'[0:v:0]fps={self.fps},scale={self.w}:{self.h},split=2[a][b]')
        cmd += ['-i', self.url, '-filter_complex', graph,
                '-map', '[a]', '-pix_fmt', 'rgb24', '-f', 'rawvideo', 'pipe:1',
                '-map', '[b]', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', str(CRF),
                '-pix_fmt', 'yuv420p',
                '-force_key_frames', f'expr:gte(t,n_forced*{SEGMENT_S})',
                '-f', 'segment', '-segment_time', str(SEGMENT_S), '-reset_timestamps', '1',
                '-segment_format', 'mp4', '-segment_format_options', 'movflags=+faststart',
                os.path.join(conn_dir, 'seg_%06d.mp4')]
        return cmd

    def _push(self, item):
        while True:
            try:
                self.q.put_nowait(item)
                return
            except queue.Full:
                try:
                    old = self.q.get_nowait()
                    if old is not None:
                        self.dropped += 1
                        logger.warning(f'[Camera] GPU chậm hơn thời gian thực: bỏ đoạn #{old["k"]} '
                                       f'(đã bỏ {self.dropped})')
                except queue.Empty:
                    pass

    def _clean(self, conn, upto):
        """Xóa file đoạn <= upto của lần kết nối conn, trừ đoạn đã chọn lưu."""
        with self.keep_lock:
            keep = set(self.keep)
        for k in range(max(0, upto - 8), upto + 1):
            p = self.seg_path(conn, k)
            if (conn, k) not in keep and os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def stop_ffmpeg(self):
        """SIGINT để ffmpeg đóng file đoạn đang ghi cho hợp lệ, rồi mới kill nếu treo."""
        p = self.proc
        if p and p.poll() is None:
            try:
                p.send_signal(signal.SIGINT)
                p.wait(timeout=10)
            except Exception:
                p.kill()

    def run(self):
        frame_bytes = self.w * self.h * 3
        try:
            while time.monotonic() < self.deadline:
                conn_dir = os.path.join(self.ring_dir, f'c{self.conn}')
                os.makedirs(conn_dir, exist_ok=True)
                err_path = os.path.join(conn_dir, 'ffmpeg.err')
                with open(err_path, 'wb') as err:
                    self.proc = subprocess.Popen(self._cmd(conn_dir), stdout=subprocess.PIPE,
                                                 stderr=err, bufsize=frame_bytes * 2,
                                                 preexec_fn=_die_with_parent)
                i, k, kept, t0 = 0, 0, {}, None
                while True:
                    if time.monotonic() >= self.deadline and self.proc.poll() is None:
                        self.stop_ffmpeg_async()
                    buf = self.proc.stdout.read(frame_bytes)
                    if len(buf) < frame_bytes:
                        break
                    if i == 0:
                        t0 = _now()
                    if i in self.need:
                        kept[i] = np.frombuffer(buf, np.uint8).reshape(self.h, self.w, 3)
                    i += 1
                    if i == self.n:
                        if time.monotonic() < self.deadline:
                            self._push({'conn': self.conn, 'k': k, 'start': t0, 'end': _now(),
                                        'frames': [kept[j] for j in self.order]})
                            self.segments += 1
                        self._clean(self.conn, k - RING_KEEP)
                        i, k, kept = 0, k + 1, {}
                self.proc.wait()
                self.exited[self.conn] = True
                if time.monotonic() >= self.deadline:
                    break
                try:
                    with open(err_path, 'rb') as f:
                        tail = f.read()[-300:].decode('utf-8', 'replace').strip()
                except OSError:
                    tail = ''
                self.reconnects += 1
                logger.warning(f'[Camera] Luồng dừng/rớt (bỏ {i} frame của đoạn dở), nối lại sau '
                               f'{RECONNECT_S}s' + (f': {redact_text(tail)}' if tail else ''))
                if self.segments == 0 and self.reconnects >= 3:
                    self.error = 'không nhận được đoạn nào sau 3 lần nối' + (
                        f': {redact_text(tail)}' if tail else '')
                    break
                time.sleep(RECONNECT_S)
                self.conn += 1
        except Exception as e:
            self.error = f'{type(e).__name__}: {redact_text(str(e))}'
        finally:
            self.stop_ffmpeg()
            self.exited[self.conn] = True
            # Không dùng q.put(None) chờ: nếu luồng chấm điểm đã dừng vì lỗi thì hàng đợi
            # đầy mãi và thread này kẹt. _push bỏ đoạn cũ để chắc chắn đặt được dấu kết thúc.
            self._push(None)

    def stop_ffmpeg_async(self):
        # Gửi SIGINT nhưng KHÔNG chờ: vòng đọc phải tiếp tục rút pipe, nếu không ffmpeg kẹt
        # ở lệnh ghi pipe và không đóng được file đoạn cuối.
        p = self.proc
        if p and p.poll() is None and not getattr(p, '_sigint_sent', False):
            p.send_signal(signal.SIGINT)
            p._sigint_sent = True


def _wait_segment_file(reader, conn, k, timeout_s=20.0):
    """File đoạn k chỉ hoàn chỉnh khi muxer đã mở đoạn k+1 (hoặc ffmpeg đã thoát)."""
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        if os.path.exists(reader.seg_path(conn, k + 1)) or reader.exited.get(conn):
            return os.path.exists(reader.seg_path(conn, k))
        time.sleep(0.2)
    return False


def _save_event(reader, item, cam, probs, model, threshold, fps, out_root):
    """Chép file đoạn sang thư mục kết quả + ghi annotation + 1 dòng events.jsonl.
    Ghi out_root (có thể là NAS) lỗi thì lưu vào LOCAL_OUTPUT_DIR để không mất đoạn."""
    src = reader.seg_path(item['conn'], item['k'])
    if not _wait_segment_file(reader, item['conn'], item['k']):
        logger.error(f'[Camera] Không có file đoạn #{item["k"]} để lưu ({src})')
        return None
    roots = [out_root] + ([LOCAL_OUTPUT_DIR] if os.path.abspath(out_root) != LOCAL_OUTPUT_DIR else [])
    for root in roots:
        try:
            dst = _write_event(src, root, reader, item, cam, probs, model, threshold, fps)
        except OSError as e:
            logger.warning(f'[Camera] Không ghi được vào {root}: {e}')
            continue
        if root != out_root:
            logger.warning(f'[Camera] Đoạn #{item["k"]} lưu tạm ở {dst} — chép lên {out_root} sau')
        os.remove(src)
        return dst
    logger.error(f'[Camera] Không lưu được đoạn #{item["k"]} ở đâu cả')
    return None


def _write_event(src, out_root, reader, item, cam, probs, model, threshold, fps):
    from crawler_core import annotation

    label = model.label(probs, threshold)
    top = int(np.argmax(probs))
    day_dir = os.path.join(out_root, cam, item['start'].strftime('%Y-%m-%d'))
    os.makedirs(day_dir, exist_ok=True)
    stem = f"{item['start'].strftime('%H%M%S')}_{annotation.dataset_label(label)}_{int(round(probs[top] * 100))}"
    dst = os.path.join(day_dir, stem + '.mp4')
    # copyfile chứ không shutil.move: move sang ổ khác chép cả quyền file, NAS qua gvfs
    # báo "Operation not supported" (thử 2026-11-25). Ghi .part rồi đổi tên → người duyệt
    # trên NAS không bao giờ mở phải file chép dở.
    shutil.copyfile(src, dst + '.part')
    os.replace(dst + '.part', dst)

    n = SEGMENT_S * fps
    order = np.argsort(-probs)
    scores = [(model.outputs[j], float(probs[j])) for j in order]
    window = {
        'start_frame': 0, 'end_frame': n, 'label': label, 'top_label': label,
        'prob': float(probs[top]), 'scores': scores,
    }
    data = annotation.build(
        dst, SEGMENT_S,
        videomae={'windows': [window], 'threshold': threshold,
                  'video': {'fps': fps, 'width': reader.w, 'height': reader.h, 'n_frames': n}},
        model_labels=list(model.taxonomy.labels), model='VideoMAE multi-label')
    if annotation.write(dst, data) is None:  # write() nuốt lỗi, chỉ trả None
        raise OSError(f'không ghi được annotation cạnh {dst}')

    with open(os.path.join(out_root, 'events.jsonl'), 'a', encoding='utf-8') as f:
        f.write(json.dumps({
            'time': item['start'].isoformat(timespec='seconds'), 'camera': cam, 'label': label,
            'file': os.path.relpath(dst, out_root),
            'over_threshold': [nm for nm, p in scores if p > threshold],
            'scores': {nm: round(p, 4) for nm, p in scores},
        }, ensure_ascii=False) + '\n')
    return dst


def record_camera(url, model, minutes=MINUTES, threshold=THRESHOLD, out_root=OUTPUT_DIR):
    """Xem 1 camera trong `minutes` phút, lưu các đoạn có đầu ra > threshold.
    Không ném lỗi camera ra ngoài (camera hỏng không được làm dừng cả vòng xoay)."""
    cam = camera_name(url)
    stats = {'camera': cam, 'segments': 0, 'saved': 0, 'dropped': 0, 'reconnects': 0, 'error': None}
    t_start = time.monotonic()
    try:
        stream = resolve_stream(url)
        w, h, fps_src = probe(stream)
    except Exception as e:
        stats['error'] = f'{type(e).__name__}: {redact_text(str(e))[:300]}'
        logger.error(f'[Camera] {cam}: không mở được — {stats["error"]}')
        return stats

    fps = max(1, int(round(fps_src)))
    n = SEGMENT_S * fps
    w_src, h_src = w, h
    w, h = output_size(w, h)
    order = mid_bin_indices(n, model.num_frames)
    ring_dir = os.path.join(RING_DIR, f'{cam}_{os.getpid()}')
    shutil.rmtree(ring_dir, ignore_errors=True)
    os.makedirs(ring_dir, exist_ok=True)
    logger.info(f'[Camera] {cam}: {w_src}x{h_src} @ {fps_src:g} fps → {w}x{h} @ {fps} fps, {n} frame/đoạn '
                f'{SEGMENT_S}s, xem {minutes:g} phút, lưu khi đầu ra > {threshold:.0%}')

    q = queue.Queue(maxsize=1)
    reader = _Reader(stream, w, h, fps, n, order, ring_dir, t_start + minutes * 60, q)
    reader.start()
    try:
        while True:
            item = q.get()
            if item is None:
                break
            t = time.monotonic()
            probs = model.score(item['frames'])
            took = time.monotonic() - t
            stats['segments'] += 1
            top = int(np.argmax(probs))
            over = [f'{model.outputs[j]} {probs[j]:.0%}' for j in np.argsort(-probs) if probs[j] > threshold]
            msg = (f'[Camera] {cam} {item["start"]:%H:%M:%S} #{item["k"]:<3d} '
                   + (' | '.join(over) if over else f'({model.outputs[top]} {probs[top]:.0%})')
                   # Ghi rõ "chấm" — trước đây chỉ "[3.9s]", bị đọc nhầm là clip dài 4s (11-25)
                   + f'  [đoạn {SEGMENT_S}s, chấm {took:.1f}s]')
            if over:
                with reader.keep_lock:
                    reader.keep.add((item['conn'], item['k']))
                dst = _save_event(reader, item, cam, probs, model, threshold, fps, out_root)
                if dst:
                    stats['saved'] += 1
                    msg += (f' → lưu {os.path.relpath(dst, out_root)}'
                            if dst.startswith(out_root) else f' → lưu tạm {dst}')
            logger.info(msg)
    finally:
        reader.deadline = 0
        reader.stop_ffmpeg()
        reader.join(timeout=30)
        shutil.rmtree(ring_dir, ignore_errors=True)
    stats.update(dropped=reader.dropped, reconnects=reader.reconnects, error=reader.error)
    if reader.error:
        logger.error(f'[Camera] {cam}: {reader.error}')
    logger.info(f'[Camera] {cam}: xong {stats["segments"]} đoạn, lưu {stats["saved"]}, '
                f'bỏ {stats["dropped"]} (GPU chậm), nối lại {stats["reconnects"]} lần')
    return stats


def rotate(urls, model, run_minutes, minutes=MINUTES, threshold=THRESHOLD, out_root=OUTPUT_DIR):
    """Xoay vòng camera, mỗi camera `minutes` phút, tới khi đã chạy >= run_minutes
    (xong camera đang xem mới dừng). Trả về thống kê theo camera."""
    if not urls:
        raise ValueError('Danh sách camera rỗng')
    deadline = time.monotonic() + run_minutes * 60
    totals = {}
    i = 0
    while True:
        url = urls[i % len(urls)]
        s = record_camera(url, model, minutes, threshold, out_root)
        t = totals.setdefault(s['camera'], {'visits': 0, 'segments': 0, 'saved': 0,
                                            'dropped': 0, 'errors': 0})
        t['visits'] += 1
        t['errors'] += int(bool(s['error']) and s['segments'] == 0)
        for key in ('segments', 'saved', 'dropped'):
            t[key] += s[key]
        i += 1
        if time.monotonic() >= deadline:
            break
        if i % len(urls) == 0 and all(v['segments'] == 0 for v in totals.values()):
            # Cả vòng không camera nào mở được (mất mạng, sai tài khoản): nghỉ thay vì
            # thử lại liên tục từng giây.
            logger.error('[Camera] Cả vòng không camera nào mở được — nghỉ 60s')
            time.sleep(60)
    return totals
