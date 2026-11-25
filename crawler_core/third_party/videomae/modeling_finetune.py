# Vendored từ https://github.com/MCG-NJU/VideoMAE (commit 14ef8d8) — modeling_finetune.py, cập nhật 2026-11-25,
# CÓ các sửa đổi cục bộ của dự án (bản ở ~/Documents/VideoMAE lúc train model).
# Giấy phép gốc: CC BY-NC 4.0 (chỉ dùng phi thương mại) — xem LICENSE cùng thư mục.
# Đưa vào repo để máy clone về chạy được bộ lọc VideoMAE mà không cần repo VideoMAE.
# Từ 2026-11-25 thêm: lưới vị trí cho input khác 224 (get_grid_sinusoid_table, vd 368x640),
# pool='topk' (model multi-label camera_dataset_video_events). Ở 224 + pool mean kết quả
# giống hệt bản cũ (đã so trên vitl_224x224_240926).

from functools import partial
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from timm.models.layers import drop_path, to_2tuple, trunc_normal_
from timm.models.registry import register_model
import torch.utils.checkpoint as checkpoint


def _cfg(url='', **kwargs):
    return {
        'url': url,
        'num_classes': 400, 'input_size': (3, 224, 224), 'pool_size': None,
        'crop_pct': .9, 'interpolation': 'bicubic',
        'mean': (0.5, 0.5, 0.5), 'std': (0.5, 0.5, 0.5),
        **kwargs
    }


class DropPath(nn.Module):
    """Drop paths (Stochastic Depth) per sample  (when applied in main path of residual blocks).
    """
    def __init__(self, drop_prob=None):
        super(DropPath, self).__init__()
        self.drop_prob = drop_prob

    def forward(self, x):
        return drop_path(x, self.drop_prob, self.training)
    
    def extra_repr(self) -> str:
        return 'p={}'.format(self.drop_prob)


class Mlp(nn.Module):
    def __init__(self, in_features, hidden_features=None, out_features=None, act_layer=nn.GELU, drop=0.):
        super().__init__()
        out_features = out_features or in_features
        hidden_features = hidden_features or in_features
        self.fc1 = nn.Linear(in_features, hidden_features)
        self.act = act_layer()
        self.fc2 = nn.Linear(hidden_features, out_features)
        self.drop = nn.Dropout(drop)

    def forward(self, x):
        x = self.fc1(x)
        x = self.act(x)
        # x = self.drop(x)
        # commit this for the orignal BERT implement 
        x = self.fc2(x)
        x = self.drop(x)
        return x


class Attention(nn.Module):
    def __init__(
            self, dim, num_heads=8, qkv_bias=False, qk_scale=None, attn_drop=0.,
            proj_drop=0., attn_head_dim=None):
        super().__init__()
        self.num_heads = num_heads
        head_dim = dim // num_heads
        if attn_head_dim is not None:
            head_dim = attn_head_dim
        all_head_dim = head_dim * self.num_heads
        self.scale = qk_scale or head_dim ** -0.5

        self.qkv = nn.Linear(dim, all_head_dim * 3, bias=False)
        if qkv_bias:
            self.q_bias = nn.Parameter(torch.zeros(all_head_dim))
            self.v_bias = nn.Parameter(torch.zeros(all_head_dim))
        else:
            self.q_bias = None
            self.v_bias = None

        self.attn_drop = nn.Dropout(attn_drop)
        self.proj = nn.Linear(all_head_dim, dim)
        self.proj_drop = nn.Dropout(proj_drop)

    def forward(self, x):
        B, N, C = x.shape
        qkv_bias = None
        if self.q_bias is not None:
            qkv_bias = torch.cat((self.q_bias, torch.zeros_like(self.v_bias, requires_grad=False), self.v_bias))
        # qkv = self.qkv(x).reshape(B, N, 3, self.num_heads, C // self.num_heads).permute(2, 0, 3, 1, 4)
        qkv = F.linear(input=x, weight=self.qkv.weight, bias=qkv_bias)
        qkv = qkv.reshape(B, N, 3, self.num_heads, -1).permute(2, 0, 3, 1, 4)
        q, k, v = qkv[0], qkv[1], qkv[2]   # make torchscript happy (cannot use tensor as tuple)

        # Fused / memory-efficient attention. Mathematically identical to the
        # explicit q@k.T -> softmax -> @v below, but never materialises the
        # N x N attention matrix (N = 1568 for 16x224x224), which is what makes
        # batch size > 1 fit on a 6 GB card.
        x = F.scaled_dot_product_attention(
            q, k, v, dropout_p=self.attn_drop.p if self.training else 0.0,
            scale=self.scale)
        x = x.transpose(1, 2).reshape(B, N, -1)
        x = self.proj(x)
        x = self.proj_drop(x)
        return x


class Block(nn.Module):

    def __init__(self, dim, num_heads, mlp_ratio=4., qkv_bias=False, qk_scale=None, drop=0., attn_drop=0.,
                 drop_path=0., init_values=None, act_layer=nn.GELU, norm_layer=nn.LayerNorm,
                 attn_head_dim=None):
        super().__init__()
        self.norm1 = norm_layer(dim)
        self.attn = Attention(
            dim, num_heads=num_heads, qkv_bias=qkv_bias, qk_scale=qk_scale,
            attn_drop=attn_drop, proj_drop=drop, attn_head_dim=attn_head_dim)
        # NOTE: drop path for stochastic depth, we shall see if this is better than dropout here
        self.drop_path = DropPath(drop_path) if drop_path > 0. else nn.Identity()
        self.norm2 = norm_layer(dim)
        mlp_hidden_dim = int(dim * mlp_ratio)
        self.mlp = Mlp(in_features=dim, hidden_features=mlp_hidden_dim, act_layer=act_layer, drop=drop)

        if init_values > 0:
            self.gamma_1 = nn.Parameter(init_values * torch.ones((dim)),requires_grad=True)
            self.gamma_2 = nn.Parameter(init_values * torch.ones((dim)),requires_grad=True)
        else:
            self.gamma_1, self.gamma_2 = None, None

    def forward(self, x):
        if self.gamma_1 is None:
            x = x + self.drop_path(self.attn(self.norm1(x)))
            x = x + self.drop_path(self.mlp(self.norm2(x)))
        else:
            x = x + self.drop_path(self.gamma_1 * self.attn(self.norm1(x)))
            x = x + self.drop_path(self.gamma_2 * self.mlp(self.norm2(x)))
        return x


class PatchEmbed(nn.Module):
    """ Image to Patch Embedding
    """
    def __init__(self, img_size=224, patch_size=16, in_chans=3, embed_dim=768, num_frames=16, tubelet_size=2):
        super().__init__()
        img_size = to_2tuple(img_size)
        patch_size = to_2tuple(patch_size)
        self.tubelet_size = int(tubelet_size)
        num_patches = (img_size[1] // patch_size[1]) * (img_size[0] // patch_size[0]) * (num_frames // self.tubelet_size)
        self.img_size = img_size
        self.patch_size = patch_size
        self.num_patches = num_patches
        self.proj = nn.Conv3d(in_channels=in_chans, out_channels=embed_dim, 
                            kernel_size = (self.tubelet_size,  patch_size[0],patch_size[1]), 
                            stride=(self.tubelet_size,  patch_size[0],  patch_size[1]))

    def forward(self, x, **kwargs):
        B, C, T, H, W = x.shape
        # FIXME look at relaxing size constraints
        assert H == self.img_size[0] and W == self.img_size[1], \
            f"Input image size ({H}*{W}) doesn't match model ({self.img_size[0]}*{self.img_size[1]})."
        x = self.proj(x).flatten(2).transpose(1, 2)
        return x
    
# sin-cos position encoding
# https://github.com/jadore801120/attention-is-all-you-need-pytorch/blob/master/transformer/Models.py#L31
def get_sinusoid_encoding_table(n_position, d_hid): 
    ''' Sinusoid position encoding table ''' 
    # TODO: make it with torch instead of numpy 
    def get_position_angle_vec(position): 
        return [position / np.power(10000, 2 * (hid_j // 2) / d_hid) for hid_j in range(d_hid)] 

    sinusoid_table = np.array([get_position_angle_vec(pos_i) for pos_i in range(n_position)]) 
    sinusoid_table[:, 0::2] = np.sin(sinusoid_table[:, 0::2]) # dim 2i 
    sinusoid_table[:, 1::2] = np.cos(sinusoid_table[:, 1::2]) # dim 2i+1 

    return  torch.tensor(sinusoid_table,dtype=torch.float, requires_grad=False).unsqueeze(0)


def get_grid_sinusoid_table(grid, d_hid, base_hw=(14, 14)):
    """Fixed position table for a (T, H, W) token grid.

    The table above is 1-D over the flattened tokens. Pretraining and every checkpoint
    so far used the 224 grid, (8, 14, 14), so position n means
    (t, h, w) = divmod chain of n with rows wrapping every 14 tokens. Rebuilding the 1-D
    table for another grid (e.g. 23 x 40 at 368x640) would give the same n a different
    (h, w) and throw away what the weights learned about positions. Instead, build the
    table on the base grid and resize it spatially to the new one, as is usual for
    learned ViT position embeddings. On the base grid this returns the original table
    unchanged.
    """
    T, H, W = grid
    bh, bw = base_hw
    table = get_sinusoid_encoding_table(T * bh * bw, d_hid)
    if (H, W) == (bh, bw):
        return table
    t = table.reshape(T, bh, bw, d_hid).permute(0, 3, 1, 2)  # T, C, bh, bw
    t = F.interpolate(t, size=(H, W), mode='bicubic', align_corners=False)
    return t.permute(0, 2, 3, 1).reshape(1, T * H * W, d_hid)


def topk_pool(token_logits, valid, ratio):
    """Clip logits [B, C] = per class, the mean of the k highest token logits [B, N, C],
    k = ceil(ratio * valid tokens of that clip). Tokens with valid False never count."""
    valid = valid | ~valid.any(1, keepdim=True)  # an all-padding clip: use every token
    k = (valid.sum(1).float() * ratio).ceil().long().clamp_min(1)  # [B]
    s = token_logits.masked_fill(~valid[..., None], float('-inf'))
    top = s.topk(int(k.max()), dim=1).values  # [B, kmax, C], sorted high to low
    keep = torch.arange(top.shape[1], device=top.device)[None, :] < k[:, None]
    top = top.masked_fill(~keep[..., None], 0.0)
    return top.sum(1) / k[:, None].to(top.dtype)


class VisionTransformer(nn.Module):
    """ Vision Transformer with support for patch or hybrid CNN input stage
    """
    def __init__(self, 
                 img_size=224, 
                 patch_size=16, 
                 in_chans=3, 
                 num_classes=1000, 
                 embed_dim=768, 
                 depth=12,
                 num_heads=12, 
                 mlp_ratio=4., 
                 qkv_bias=False, 
                 qk_scale=None, 
                 fc_drop_rate=0., 
                 drop_rate=0., 
                 attn_drop_rate=0.,
                 drop_path_rate=0., 
                 norm_layer=nn.LayerNorm, 
                 init_values=0.,
                 use_learnable_pos_emb=False, 
                 init_scale=0.,
                 all_frames=16,
                 tubelet_size=2,
                 use_checkpoint=False,
                 use_mean_pooling=True,
                 pool='mean',
                 topk_ratio=0.05):
        super().__init__()
        assert pool in ('mean', 'topk'), pool
        # 'topk' (weakly supervised localisation, multiple-instance learning): the head scores
        # every token, and a clip's logit is the mean of its top-k token logits, so the
        # token logits themselves are a map of where each class is (forward_token_logits).
        # Same parameters as 'mean', so checkpoints load either way.
        assert pool == 'mean' or use_mean_pooling, "pool='topk' needs fc_norm (use_mean_pooling)"
        self.pool, self.topk_ratio = pool, topk_ratio
        self.num_classes = num_classes
        self.num_features = self.embed_dim = embed_dim  # num_features for consistency with other models
        self.tubelet_size = tubelet_size
        self.patch_embed = PatchEmbed(
            img_size=img_size, patch_size=patch_size, in_chans=in_chans, embed_dim=embed_dim, num_frames=all_frames, tubelet_size=self.tubelet_size)
        num_patches = self.patch_embed.num_patches
        self.use_checkpoint = use_checkpoint

        if use_learnable_pos_emb:
            self.pos_embed = nn.Parameter(torch.zeros(1, num_patches, embed_dim))
        else:
            # sine-cosine positional embeddings is on the way
            img_hw = self.patch_embed.img_size
            p = self.patch_embed.patch_size
            grid = (all_frames // self.tubelet_size, img_hw[0] // p[0], img_hw[1] // p[1])
            self.pos_embed = get_grid_sinusoid_table(grid, embed_dim)

        self.pos_drop = nn.Dropout(p=drop_rate)


        dpr = [x.item() for x in torch.linspace(0, drop_path_rate, depth)]  # stochastic depth decay rule
        self.blocks = nn.ModuleList([
            Block(
                dim=embed_dim, num_heads=num_heads, mlp_ratio=mlp_ratio, qkv_bias=qkv_bias, qk_scale=qk_scale,
                drop=drop_rate, attn_drop=attn_drop_rate, drop_path=dpr[i], norm_layer=norm_layer,
                init_values=init_values)
            for i in range(depth)])
        self.norm = nn.Identity() if use_mean_pooling else norm_layer(embed_dim)
        self.fc_norm = norm_layer(embed_dim) if use_mean_pooling else None
        self.fc_dropout = nn.Dropout(p=fc_drop_rate) if fc_drop_rate > 0 else nn.Identity()
        self.head = nn.Linear(embed_dim, num_classes) if num_classes > 0 else nn.Identity()

        if use_learnable_pos_emb:
            trunc_normal_(self.pos_embed, std=.02)

        trunc_normal_(self.head.weight, std=.02)
        self.apply(self._init_weights)

        self.head.weight.data.mul_(init_scale)
        self.head.bias.data.mul_(init_scale)

    def _init_weights(self, m):
        if isinstance(m, nn.Linear):
            trunc_normal_(m.weight, std=.02)
            if isinstance(m, nn.Linear) and m.bias is not None:
                nn.init.constant_(m.bias, 0)
        elif isinstance(m, nn.LayerNorm):
            nn.init.constant_(m.bias, 0)
            nn.init.constant_(m.weight, 1.0)

    def get_num_layers(self):
        return len(self.blocks)

    @torch.jit.ignore
    def no_weight_decay(self):
        return {'pos_embed', 'cls_token'}

    def get_classifier(self):
        return self.head

    def reset_classifier(self, num_classes, global_pool=''):
        self.num_classes = num_classes
        self.head = nn.Linear(self.embed_dim, num_classes) if num_classes > 0 else nn.Identity()

    def forward_tokens(self, x):
        """Input [B, 3, T, H, W] -> last-block tokens [B, N, D], N = T/2 * H/16 * W/16."""
        x = self.patch_embed(x)
        B, _, _ = x.size()

        if self.pos_embed is not None:
            x = x + self.pos_embed.expand(B, -1, -1).type_as(x).to(x.device).clone().detach()
        x = self.pos_drop(x)

        if self.use_checkpoint:
            for blk in self.blocks:
                # use_reentrant=False is REQUIRED: with the reentrant version, if the
                # blocks before this one are frozen (x.requires_grad is False), the
                # recomputed graph is detached and the trainable blocks silently get
                # grad=None -- training runs but only the head ever updates.
                x = checkpoint.checkpoint(blk, x, use_reentrant=False)
        else:   
            for blk in self.blocks:
                x = blk(x)

        return self.norm(x)

    def forward_features(self, x):
        x = self.forward_tokens(x)
        if self.fc_norm is not None:
            return self.fc_norm(x.mean(1))
        else:
            return x[:, 0]

    def token_grid(self, x):
        """(T', H', W') of the token grid for input x."""
        p, t = self.patch_embed.patch_size, self.tubelet_size
        return x.shape[2] // t, x.shape[3] // p[0], x.shape[4] // p[1]

    def valid_tokens(self, x):
        """[B, N] bool, False for tokens that are letterbox padding only (every pixel of the
        2x16x16 tubelet is normalised black), so top-k pooling cannot pick the border."""
        from clip_preprocess import pad_value
        B = x.shape[0]
        T, H, W = self.token_grid(x)
        p, t = self.patch_embed.patch_size, self.tubelet_size
        pad = (x - pad_value(x.device, x.dtype)).abs().amax(1) < 1e-2  # [B, T, H, W]
        pad = pad.view(B, T, t, H, p[0], W, p[1]).all(6).all(4).all(2)
        return ~pad.flatten(1)

    def forward_token_logits(self, tokens):
        """Last-block tokens [B, N, D] -> per-token logits [B, N, num_classes]."""
        return self.head(self.fc_dropout(self.fc_norm(tokens)))

    def logits_from_tokens(self, tokens, valid=None):
        """Last-block tokens -> clip logits, pooled the way the model was trained."""
        if self.pool == 'topk':
            if valid is None:
                valid = torch.ones(tokens.shape[:2], dtype=torch.bool, device=tokens.device)
            return topk_pool(self.forward_token_logits(tokens), valid, self.topk_ratio)
        x = self.fc_norm(tokens.mean(1)) if self.fc_norm is not None else tokens[:, 0]
        return self.head(self.fc_dropout(x))

    def forward(self, x):
        if self.pool == 'topk':
            return self.logits_from_tokens(self.forward_tokens(x), self.valid_tokens(x))
        x = self.forward_features(x)
        x = self.head(self.fc_dropout(x))
        return x


def _drop_timm_kwargs(kwargs):
    """timm >= 0.9 injects its own bookkeeping kwargs into every model factory
    (pretrained_cfg, pretrained_cfg_overlay, cache_dir, ...). These models are not
    in timm's hub, so just discard them instead of forwarding them to __init__."""
    for k in ('pretrained_cfg', 'pretrained_cfg_overlay', 'cache_dir'):
        kwargs.pop(k, None)
    return kwargs


@register_model
def vit_small_patch16_224(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        patch_size=16, embed_dim=384, depth=12, num_heads=6, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_base_patch16_224(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        patch_size=16, embed_dim=768, depth=12, num_heads=12, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_base_patch16_384(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        img_size=384, patch_size=16, embed_dim=768, depth=12, num_heads=12, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_large_patch16_224(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        patch_size=16, embed_dim=1024, depth=24, num_heads=16, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_large_patch16_384(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        img_size=384, patch_size=16, embed_dim=1024, depth=24, num_heads=16, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_large_patch16_512(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        img_size=512, patch_size=16, embed_dim=1024, depth=24, num_heads=16, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model


@register_model
def vit_huge_patch16_224(pretrained=False, **kwargs):
    _drop_timm_kwargs(kwargs)
    model = VisionTransformer(
        patch_size=16, embed_dim=1280, depth=32, num_heads=16, mlp_ratio=4, qkv_bias=True,
        norm_layer=partial(nn.LayerNorm, eps=1e-6), **kwargs)
    model.default_cfg = _cfg()
    return model
