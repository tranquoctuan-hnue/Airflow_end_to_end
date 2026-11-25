# Vendored từ ~/Documents/VideoMAE/clip_preprocess.py (2026-11-25): letterbox giống hệt lúc train
# model 368x640 (giữ tỉ lệ, viền đen đã chuẩn hóa).

"""Letterbox preprocessing shared by training (kinetics.py), segment_infer.py and
compare_models.py, so a clip is resized and padded identically everywhere.

Letterbox: scale the whole frame by min(H/h, W/w) so it fits inside the H x W model
input without cropping or distorting it, centre it, and fill the rest with black.
Resizing is done on the normalised tensor. Bilinear resizing with weights that sum to 1
commutes with the per-channel affine normalisation, so this equals "resize the uint8
frame, then normalise". The padding is normalised black, (0 - mean) / std.
"""

import math
import random

import numpy as np
import torch
import torch.nn.functional as F

MEAN = (0.485, 0.456, 0.406)
STD = (0.229, 0.224, 0.225)


def pad_value(device=None, dtype=torch.float32):
    """Normalised black, shape [C, 1, 1, 1]."""
    return torch.tensor([-m / s for m, s in zip(MEAN, STD)], device=device, dtype=dtype).view(3, 1, 1, 1)


def normalize_uint8(frames):
    """[T, H, W, C] uint8 numpy/tensor -> [C, T, H, W] float, ImageNet-normalised."""
    x = torch.as_tensor(np.ascontiguousarray(frames)).float().div_(255.0)
    x = x.permute(3, 0, 1, 2)
    mean = torch.tensor(MEAN).view(3, 1, 1, 1)
    std = torch.tensor(STD).view(3, 1, 1, 1)
    return (x - mean) / std


def letterbox(x, out_hw):
    """[C, T, H, W] normalised -> [C, T, out_h, out_w], aspect ratio kept, centred, black pad."""
    C, T, H, W = x.shape
    oh, ow = out_hw
    scale = min(oh / H, ow / W)
    nh, nw = min(oh, max(1, round(H * scale))), min(ow, max(1, round(W * scale)))
    if (nh, nw) != (H, W):
        # [C, T, H, W] -> [T, C, H, W]: frames are the batch dimension
        y = F.interpolate(x.permute(1, 0, 2, 3), size=(nh, nw), mode="bilinear",
                          align_corners=False, antialias=scale < 1.0).permute(1, 0, 2, 3)
    else:
        y = x
    if (nh, nw) == (oh, ow):
        return y.contiguous()
    out = pad_value(x.device, x.dtype).expand(C, T, oh, ow).clone()
    top, left = (oh - nh) // 2, (ow - nw) // 2
    out[:, :, top:top + nh, left:left + nw] = y
    return out


def random_crop_keep_aspect(x, scale=(0.6, 1.0), rel_ratio=(3 / 4, 4 / 3), attempts=10):
    """Training jitter before letterboxing: a random crop covering `scale` of the frame
    area, with an aspect ratio within `rel_ratio` of the frame's own. Much milder than
    VideoMAE's RandomResizedCrop (8-100 % area), which would often crop the knife, or
    the event itself, out of a surveillance frame."""
    _, _, H, W = x.shape
    area = H * W
    for _ in range(attempts):
        target = area * random.uniform(*scale)
        log_r = (math.log(rel_ratio[0]), math.log(rel_ratio[1]))
        r = (W / H) * math.exp(random.uniform(*log_r))
        w = int(round(math.sqrt(target * r)))
        h = int(round(math.sqrt(target / r)))
        if 0 < w <= W and 0 < h <= H:
            top = random.randint(0, H - h)
            left = random.randint(0, W - w)
            return x[:, :, top:top + h, left:left + w]
    return x


def letterbox_clip(frames, out_hw):
    """[T, H, W, C] uint8 -> model input [C, T, out_h, out_w]."""
    return letterbox(normalize_uint8(frames), out_hw)
