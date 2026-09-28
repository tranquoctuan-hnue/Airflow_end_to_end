"""
Tạo bản checkpoint VideoMAE CHỈ CÓ TRỌNG SỐ để chép sang máy khác cho nhẹ.

checkpoint-best.pth lúc train chứa cả optimizer (AdamW lưu 2 tensor/tham số) nên
ViT-L nặng ~3,5GB. Suy luận chỉ cần 'model' + 'args' (~1,2GB). Bản gọn KHÔNG train
tiếp được (mất optimizer) — giữ bản gốc ở máy train.

    python scripts/slim_checkpoint.py models/Video_understanding/vitl_224x224_240926/checkpoint-best.pth \\
        /duong/dan/moi/checkpoint-best.pth
"""
import os
import sys

import torch

if len(sys.argv) != 3:
    sys.exit(__doc__)
src, dst = sys.argv[1], sys.argv[2]
if os.path.abspath(src) == os.path.abspath(dst):
    sys.exit('Đích phải khác nguồn (không ghi đè checkpoint gốc)')

ckpt = torch.load(src, map_location='cpu', weights_only=False, mmap=True)
slim = {'model': ckpt['model'], 'args': ckpt.get('args'), 'epoch': ckpt.get('epoch')}
os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
torch.save(slim, dst)
print(f'{os.path.getsize(src) / 2**30:.2f} GB → {os.path.getsize(dst) / 2**30:.2f} GB: {dst}')
