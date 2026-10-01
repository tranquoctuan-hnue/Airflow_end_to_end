"""
VideoMAEEventFilter — bộ lọc "có sự việc" bằng model VideoMAE tự fine-tune.

Điều kiện: video ĐẠT khi có ÍT NHẤT 1 cửa sổ (mặc định 5 giây) mà một nhãn KHÁC
Normal có xác suất >= ngưỡng (mặc định 0.5). Chạy SAU mọi bộ lọc khác trong
crawler_core.pipeline.VideoPipeline.

Vì sao chấm theo cửa sổ chứ không cả video: model train trên clip 16 frame ×
sampling 4 (~2 giây). Lấy trung bình cả video thì 1 sự việc 3 giây trong video 2
phút bị pha loãng bởi các đoạn Normal → loại nhầm. Cách chia 5 giây giống
~/Documents/VideoMAE/segment_infer.py.

Mỗi đoạn 5 giây được chấm Y HỆT segment_infer.py → infer.make_views(): 5 vị trí
thời gian (clip 16 frame × sampling 4, rải đều trong đoạn) × 3 crop không gian =
15 view, tiền xử lý bằng chính make_view() của repo (resize cạnh ngắn 224, crop,
chuẩn hóa ImageNet) → trung bình logits → softmax. Đoạn ngắn hơn 1 clip thì dùng
1 clip lặp frame cuối × 3 crop, như make_views(). Đoạn đuôi ngắn vẫn được chấm.
Khác segment_infer.py ở chỗ KHÔNG cắt từng đoạn ra file mà giải mã cả video 1 lượt.

Đọc frame: MẶC ĐỊNH ffmpeg giải mã tuần tự, thu nhỏ luôn về cạnh ngắn 224 lúc giải mã,
chấm xong đoạn 5s nào thả đoạn đó → RAM không phụ thuộc độ dài video.
KHÔNG dùng decord mặc định nữa (đo 2026-09-29 máy 192.169.1.168): sau get_batch, decord
tự GIẢI MÃ CẢ VIDEO Ở ĐỘ PHÂN GIẢI GỐC vào RAM ở luồng nền (~1,1 GB/giây, cả H.264
lẫn VP9; video 1080p 32s đứng ở 5,4 GB = 32s×30fps×1920×1080×3). Video Facebook 12 phút
1080p → task vượt 13,7 GB → hệ điều hành kill (SIGKILL/OOM). Cùng video đó bằng ffmpeg:
RAM đứng 1,1 GB, đỉnh 2,65 GB, 145 đoạn trong 231s.
Kết quả ffmpeg so với decord trên 3 video có nhãn tay: 1172 Road Accident 0,764 = 0,764;
1310 Fighting 0,789 so với 0,797; 1726 không đạt (Normal 0,805) — cùng kết luận.
Muốn so sánh lại với decord: VIDEOMAE_DECODER=decord (chỉ dùng cho video ngắn).

Cấu hình model (kiến trúc, số lớp, số frame, sampling rate, input size) đọc từ
chính checkpoint (ckpt['args']) để không lệch với lúc train. Override bằng env:

    VIDEOMAE_MODEL_DIR    models/Video_understanding/vitl_224x224_240926
                          (thư mục chứa checkpoint-best.pth + labels.txt)
    VIDEOMAE_CHECKPOINT   <MODEL_DIR>/checkpoint-best.pth   (ghi đè riêng file model)
    VIDEOMAE_LABELS       <MODEL_DIR>/labels.txt  (mỗi dòng 1 tên theo thứ tự lớp,
                          hoặc dạng "chỉ_số tên")
    VIDEOMAE_REPO_DIR     crawler_core/third_party/videomae  (chứa modeling_finetune.py —
                          bản vendored từ repo VideoMAE lúc train, CC BY-NC 4.0)
    VIDEOMAE_THRESHOLD    0.5
    VIDEOMAE_WINDOW_S     5
    VIDEOMAE_AGGREGATE    max    cách gộp các vị trí thời gian trong 1 đoạn:
                          mean = trung bình logits mọi view (y hệt segment_infer.py)
                          max  = đoạn đạt nếu BẤT KỲ clip nào (~2s) trong đoạn có
                                 nhãn khác Normal >= ngưỡng — nhạy hơn với sự việc
                                 ngắn hơn 5s, nhưng dễ nhận nhầm hơn
    VIDEOMAE_TEMPORAL_VIEWS auto số vị trí clip mỗi đoạn. auto = vừa đủ để các clip PHỦ KÍN
                          đoạn 5s không chồng lấn: ceil(số frame đoạn / 64) — 2 ở 25fps,
                          3 ở 30fps, 5 ở 60fps (1 clip = 16 frame × sampling 4 = 64 frame).
                          Đặt số (vd 5) để cố định như segment_infer.py
    VIDEOMAE_NUM_CROPS    1      1 = crop giữa; 3 = trái/giữa/phải như segment_infer.py
                          (chậm gấp 3; video vuông thì 3 crop trùng nhau, vô ích)

Tốc độ ViT-L trên GTX 1660 SUPER, fp32: ~0,5–0,9 s/view. Mặc định auto×1 cho KẾT QUẢ
GIỐNG HỆT 5×3 trên 3 video mẫu có nhãn tay nhưng nhanh hơn ~8 lần (đo 2026-09-28:
video 38s mất 15s so với 123s) — 5×3 thì video 6 phút mất ~16 phút, không crawl nổi.
    VIDEOMAE_NORMAL_LABEL Normal
    VIDEOMAE_DEVICE       cuda | cpu (bỏ trống = tự chọn)
    VIDEOMAE_BATCH        4      số view mỗi lượt GPU (tự giảm một nửa khi hết VRAM)
    VIDEOMAE_FP16         false  suy luận fp16 trên GPU. ⚠ Trên GTX 1660 SUPER (không có
                                 tensor core) fp16 CHẬM HƠN fp32 ~5 lần (đo 2026-09-28:
                                 4,4 s/view so với 0,9 s/view) — chỉ bật trên GPU RTX

Checkpoint ViT-L nặng 3,6GB vì chứa cả optimizer: nạp bằng mmap nên chỉ phần
trọng số model (~1,2GB) thực sự được đọc vào RAM.
"""

import logging
import os
import sys

import numpy as np

logger = logging.getLogger(__name__)

_HOME = os.path.expanduser('~')
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_MODEL_DIR = os.path.join(
    _REPO_ROOT, 'models', 'Video_understanding', 'vitl_224x224_240926')
DEFAULT_REPO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'third_party', 'videomae')

IMAGENET_MEAN = [0.485, 0.456, 0.406]
IMAGENET_STD  = [0.229, 0.224, 0.225]


def make_view(frames, temporal_start, temporal_len, spatial_crop, input_size):
    """
    1 view [C, T, H, W] từ list frame RGB — CHÉP NGUYÊN từ infer.py của repo VideoMAE
    lúc đánh giá model (resize cạnh ngắn = input_size, crop trái/giữa/phải theo
    spatial_crop 0/1/2, chuẩn hóa ImageNet). Giữ y hệt để không lệch tiền xử lý.
    """
    import torch
    from PIL import Image
    from torchvision import transforms

    clip = frames[temporal_start: temporal_start + temporal_len]
    if len(clip) < temporal_len:
        clip = clip + [clip[-1]] * (temporal_len - len(clip))

    resized = []
    for frame in clip:
        img = Image.fromarray(frame)
        w, h = img.size
        if w < h:
            new_w, new_h = input_size, int(round(h * input_size / w))
        else:
            new_h, new_w = input_size, int(round(w * input_size / h))
        resized.append(img.resize((new_w, new_h), Image.Resampling.BILINEAR))

    h, w = resized[0].height, resized[0].width
    if h >= w:
        max_offset = h - input_size
        top, left = [0, max_offset // 2, max_offset][spatial_crop], 0
    else:
        max_offset = w - input_size
        top, left = 0, [0, max_offset // 2, max_offset][spatial_crop]
    cropped = [img.crop((left, top, left + input_size, top + input_size)) for img in resized]

    to_tensor = transforms.ToTensor()
    normalize = transforms.Normalize(IMAGENET_MEAN, IMAGENET_STD)
    tensor = torch.stack([normalize(to_tensor(img)) for img in cropped], dim=0)
    return tensor.permute(1, 0, 2, 3).contiguous()


def _env(name, default):
    return os.environ.get(name, '').strip() or default


def _env_float(name, default):
    try:
        return float(_env(name, default))
    except ValueError:
        return default


def load_labels(path: str) -> list[str]:
    """labels.txt dạng '0 Armed_suspect' (hoặc chỉ tên mỗi dòng) → list theo chỉ số."""
    names = {}
    with open(path, encoding='utf-8') as f:
        for i, line in enumerate(x.strip() for x in f):
            if not line:
                continue
            head, _, rest = line.partition(' ')
            if head.isdigit() and rest.strip():
                names[int(head)] = rest.strip()
            else:
                names[i] = line
    return [names[i] for i in sorted(names)]


class VideoMAEEventFilter:
    def __init__(self, checkpoint=None, labels_file=None, repo_dir=None,
                 device=None, threshold=None, window_s=None, num_crops=None):
        import torch

        model_dir       = _env('VIDEOMAE_MODEL_DIR', DEFAULT_MODEL_DIR)
        self.checkpoint = checkpoint or _env(
            'VIDEOMAE_CHECKPOINT', os.path.join(model_dir, 'checkpoint-best.pth'))
        labels_file     = labels_file or _env(
            'VIDEOMAE_LABELS', os.path.join(model_dir, 'labels.txt'))
        repo_dir        = repo_dir or _env('VIDEOMAE_REPO_DIR', DEFAULT_REPO_DIR)
        self.threshold  = threshold if threshold is not None else _env_float('VIDEOMAE_THRESHOLD', 0.5)
        self.window_s   = window_s if window_s is not None else _env_float('VIDEOMAE_WINDOW_S', 5.0)
        self.num_crops  = int(num_crops or _env_float('VIDEOMAE_NUM_CROPS', 1))
        tv = _env('VIDEOMAE_TEMPORAL_VIEWS', 'auto').lower()
        self.temporal_views = 0 if tv == 'auto' else max(1, int(float(tv)))   # 0 = auto
        self.aggregate = _env('VIDEOMAE_AGGREGATE', 'max').lower()
        if self.aggregate not in ('mean', 'max'):
            raise ValueError(f"VIDEOMAE_AGGREGATE={self.aggregate!r} — chỉ nhận 'mean' hoặc 'max'")
        normal          = _env('VIDEOMAE_NORMAL_LABEL', 'Normal')

        for what, p in (('checkpoint', self.checkpoint), ('labels', labels_file),
                        ('repo VideoMAE', repo_dir)):
            if not os.path.exists(p):
                raise FileNotFoundError(
                    f'[VideoMAE] Không thấy {what}: {p} — đặt env VIDEOMAE_* cho đúng, '
                    f'hoặc tắt bộ lọc (filter_videomae = off)')

        # modeling_finetune đăng ký kiến trúc vào timm (bản vendored, đúng code train);
        # make_view (đầu file) = tiền xử lý y hệt infer.py lúc đánh giá model.
        if repo_dir not in sys.path:
            sys.path.insert(0, repo_dir)
        import modeling_finetune  # noqa: F401
        from timm.models import create_model
        self._make_view = make_view

        # mmap: chỉ đọc phần tensor thực sự dùng (bỏ qua optimizer trong checkpoint)
        ckpt = torch.load(self.checkpoint, map_location='cpu', weights_only=False,
                          mmap=True)
        a = ckpt.get('args') if isinstance(ckpt, dict) else None
        self.model_name    = getattr(a, 'model', 'vit_small_patch16_224')
        self.nb_classes    = int(getattr(a, 'nb_classes', 15))
        self.num_frames    = int(getattr(a, 'num_frames', 16))
        self.sampling_rate = int(getattr(a, 'sampling_rate', 4))
        self.input_size    = int(getattr(a, 'input_size', 224))
        tubelet            = int(getattr(a, 'tubelet_size', 2))

        self.labels = load_labels(labels_file)
        if len(self.labels) != self.nb_classes:
            raise ValueError(f'[VideoMAE] labels.txt có {len(self.labels)} nhãn nhưng '
                             f'checkpoint có {self.nb_classes} lớp')
        if normal not in self.labels:
            raise ValueError(f'[VideoMAE] Không có nhãn "{normal}" trong {labels_file}')
        self.normal_idx = self.labels.index(normal)

        model = create_model(
            self.model_name, pretrained=False, num_classes=self.nb_classes,
            all_frames=self.num_frames, tubelet_size=tubelet, drop_rate=0.0,
            drop_path_rate=0.0, attn_drop_rate=0.0, use_checkpoint=False,
            use_mean_pooling=True, init_scale=0.001,
        )
        state = ckpt['model'] if isinstance(ckpt, dict) and 'model' in ckpt else ckpt
        result = model.load_state_dict(state, strict=False)
        if result.missing_keys:
            raise RuntimeError(f'[VideoMAE] Checkpoint thiếu {len(result.missing_keys)} '
                               f'tham số: {result.missing_keys[:5]}')

        dev = device or _env('VIDEOMAE_DEVICE', '') or (
            'cuda' if torch.cuda.is_available() else 'cpu')
        self.device = torch.device(dev)
        self.fp16 = (self.device.type == 'cuda'
                     and _env('VIDEOMAE_FP16', 'false').lower() == 'true')
        self.model = (model.half() if self.fp16 else model).to(self.device).eval()
        del ckpt, state
        self.batch = max(1, int(_env_float('VIDEOMAE_BATCH', 4)))
        self._torch = torch
        self.name = (f'videomae:{self.model_name} ({self.nb_classes} lớp, '
                     f'ngưỡng {self.threshold:.0%}, đoạn {self.window_s:g}s, '
                     f"{self.temporal_views or 'auto'}×{3 if self.num_crops >= 3 else 1} view, "
                     f'gộp {self.aggregate})')
        logger.info(f'[VideoMAE] Sẵn sàng: {self.name} trên {self.device} | {self.checkpoint}')

    # ── API cho VideoPipeline ─────────────────────────────────────────

    def assess_video(self, path: str) -> dict:
        """
        {'is_event': bool, 'best': {label, prob, start_s, end_s}, 'event_windows',
         'top_windows', 'n_windows', ...}
        {'is_event': None, 'error': ...}  — không đọc được video / lỗi GPU
        """
        self._last_video = None          # không để sót thông số của video trước
        try:
            windows = self._score_windows(path)
        except Exception as e:
            logger.error(f'[VideoMAE] Lỗi {path}: {e}', exc_info=True)
            return {'is_event': None, 'error': f'{type(e).__name__}: {e}'}
        if not windows:
            return {'is_event': None, 'error': 'video không có frame nào'}

        meta = getattr(self, '_last_video', None) or {}
        # 'scores' (điểm đủ các lớp) chỉ cần cho file annotation, không lưu vào DB:
        # video 12 phút = 144 đoạn, ghi hết vào filter_detail thì DB phình vô ích.
        slim = [{k: v for k, v in w.items() if k != 'scores'} for w in windows]
        ranked = sorted(slim, key=lambda w: -w['prob'])
        events = [w for w in slim if w['prob'] >= self.threshold]
        return {
            'model':         self.name,
            'is_event':      bool(events),
            'threshold':     self.threshold,
            'n_windows':     len(windows),
            'best':          ranked[0],
            'event_windows': events[:20],
            'top_windows':   ranked[:5],
            # Pipeline lấy 2 khóa này ra (pop) để ghi annotation, KHÔNG vào DB
            'windows':       windows,
            'video':         meta,
        }

    # ── Nội bộ ─────────────────────────────────────────────────────────

    def _view_indices(self, n_w: int) -> list[np.ndarray]:
        """Chỉ số frame (tương đối trong đoạn) cho từng view thời gian — y hệt make_views()."""
        total = self.num_frames * self.sampling_rate
        if n_w <= total:
            return [np.clip(np.arange(0, total, self.sampling_rate), 0, n_w - 1)]
        max_start = n_w - total
        k = self.temporal_views or int(np.ceil(n_w / total))    # auto: phủ kín đoạn
        starts = ([max_start // 2] if k == 1 else
                  np.linspace(0, max_start, num=k, dtype=np.int64).tolist())
        return [s + np.arange(0, total, self.sampling_rate) for s in starts]

    def _clip_logits(self, clips: list[list]):
        """clips: list các clip (mỗi clip num_frames frame RGB) → logits [clip, crop, lớp]."""
        torch = self._torch
        crops = [0, 1, 2] if self.num_crops >= 3 else [1]
        views = [self._make_view(c, temporal_start=0, temporal_len=self.num_frames,
                                 spatial_crop=k, input_size=self.input_size)
                 for c in clips for k in crops]
        logits, i = [], 0
        with torch.inference_mode():
            while i < len(views):
                batch = torch.stack(views[i:i + self.batch]).to(self.device)
                if self.fp16:
                    batch = batch.half()
                try:
                    logits.append(self.model(batch).float().cpu())
                except torch.cuda.OutOfMemoryError:
                    if self.batch == 1:
                        raise
                    del batch
                    torch.cuda.empty_cache()
                    self.batch = max(1, self.batch // 2)
                    logger.warning(f'[VideoMAE] Hết VRAM — giảm batch còn {self.batch}')
                    continue
                i += len(batch)
        return torch.cat(logits).view(len(clips), len(crops), -1)

    def aggregate_probs(self, logits, mode: str | None = None) -> np.ndarray:
        """logits [clip, crop, lớp] → xác suất của cả đoạn theo cách gộp đã chọn."""
        torch = self._torch
        mode = mode or self.aggregate
        if mode == 'mean':          # = infer.predict(): trung bình logits → softmax
            return torch.softmax(logits.reshape(-1, logits.shape[-1]).mean(0), -1).numpy()
        per_clip = torch.softmax(logits.mean(1), -1)          # [clip, lớp]
        return per_clip.max(0).values.numpy()

    def _score_segment(self, clips: list[list]) -> np.ndarray:
        return self.aggregate_probs(self._clip_logits(clips))

    def _result(self, probs: np.ndarray, ws: int, we: int, fps: float) -> dict:
        probs_ev = probs.copy()
        probs_ev[self.normal_idx] = -1.0                 # chỉ xét nhãn khác Normal
        k = int(probs_ev.argmax())
        top = np.argsort(-probs)[:4]                     # 4 lớp cao nhất, kể cả Normal
        return {'start_s': round(ws / fps, 1), 'end_s': round(we / fps, 1),
                'start_frame': int(ws), 'end_frame': int(we),
                'label': self.labels[k], 'prob': round(float(probs[k]), 3),
                'normal_prob': round(float(probs[self.normal_idx]), 3),
                'scores': [(self.labels[int(i)], round(float(probs[i]), 4)) for i in top]}

    def _score_windows(self, path: str) -> list[dict]:
        if os.environ.get('VIDEOMAE_DECODER', 'ffmpeg').strip().lower() != 'decord':
            return self._score_windows_ffmpeg(path)
        try:
            from decord import VideoReader, cpu
            vr = VideoReader(path, num_threads=1, ctx=cpu(0))
        except Exception as e:
            logger.info(f'[VideoMAE] decord không đọc được ({str(e).splitlines()[-1][:120]}) '
                        f'— giải mã bằng ffmpeg: {os.path.basename(path)}')
            return self._score_windows_ffmpeg(path)

        n = len(vr)
        fps = float(vr.get_avg_fps() or 25.0)
        h0, w0 = vr[0].shape[:2]
        self._last_video = {'fps': fps, 'width': int(w0), 'height': int(h0), 'n_frames': n}
        win = max(1, int(round(self.window_s * fps)))
        out = []
        for ws in range(0, n, win):
            we = min(ws + win, n)
            rel = self._view_indices(we - ws)
            idx = np.unique(np.concatenate(rel)) + ws
            frames = dict(zip(idx.tolist(), vr.get_batch(idx.astype(np.int64)).asnumpy()))
            clips = [[frames[int(i) + ws] for i in r] for r in rel]
            out.append(self._result(self._score_segment(clips), ws, we, fps))
        return out

    def _score_windows_ffmpeg(self, path: str) -> list[dict]:
        """Giải mã tuần tự bằng ffmpeg (thu nhỏ về cạnh ngắn = input_size), chấm từng đoạn."""
        import json
        import subprocess

        info = json.loads(subprocess.run(
            ['ffprobe', '-v', 'quiet', '-select_streams', 'v:0', '-show_entries',
             'stream=width,height,avg_frame_rate,r_frame_rate', '-of', 'json', path],
            capture_output=True, text=True, timeout=60).stdout or '{}')
        st = (info.get('streams') or [{}])[0]
        W, H = int(st.get('width') or 0), int(st.get('height') or 0)
        if not (W and H):
            raise RuntimeError('ffprobe không đọc được kích thước video')
        fps = 25.0
        for key in ('avg_frame_rate', 'r_frame_rate'):
            num, _, den = str(st.get(key, '0/0')).partition('/')
            try:
                if float(den or 1) and 0 < float(num) / float(den or 1) <= 120:
                    fps = float(num) / float(den or 1)
                    break
            except ValueError:
                pass
        scale = self.input_size / min(W, H)
        w, h = max(2, int(round(W * scale / 2)) * 2), max(2, int(round(H * scale / 2)) * 2)
        size = w * h * 3
        win = max(1, int(round(self.window_s * fps)))

        proc = subprocess.Popen(
            ['ffmpeg', '-v', 'error', '-nostdin', '-i', path, '-map', '0:v:0', '-an', '-sn',
             '-vf', f'scale={w}:{h}:flags=bilinear', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        out, buf, ws = [], [], 0

        def flush():
            rel = self._view_indices(len(buf))
            clips = [[buf[int(i)] for i in r] for r in rel]
            out.append(self._result(self._score_segment(clips), ws, ws + len(buf), fps))

        try:
            while True:
                raw = proc.stdout.read(size)
                if len(raw) < size:
                    break
                buf.append(np.frombuffer(raw, np.uint8).reshape(h, w, 3))
                if len(buf) == win:
                    flush()
                    ws += win
                    buf = []
            if buf:
                flush()
        finally:
            proc.stdout.close()
            proc.wait()
        self._last_video = {'fps': fps, 'width': W, 'height': H, 'n_frames': ws + len(buf)}
        return out
