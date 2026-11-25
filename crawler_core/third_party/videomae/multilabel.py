# Vendored từ ~/Documents/VideoMAE/multilabel.py (2026-11-25): bảng nhãn → đầu ra multi-label
# và Taxonomy.decode (đầu ra → nhãn theo bảng ưu tiên). Giấy phép như modeling_finetune.py.

"""Multi-label training on single-label, priority-annotated segments.

Each segment carries ONE annotation label, picked from a danger-priority table
(classes_full_understarding_mota.md). The model instead predicts independent outputs
(Gun, Robbery, Fighting, Fall, ...) with a sigmoid each. The taxonomy JSON
(configs/understanding_multilabel_v1.json) says, per annotation label, which outputs are
certainly present ("positives"). The priority table then gives certain negatives for
free: if every label that carries output F ranks above label L, then a segment
annotated L cannot contain F, or the annotator would have chosen that higher label.
All remaining outputs are unknown, and their loss is masked out.

List lines are "<path> <labels> [full]":
    a.mp4 16            one label: positives from the taxonomy, negatives from the priority rule
    a.mp4 16,9          several labels seen in the segment: union of their positives, negatives
                        from the priority rule of the highest-priority one
    a.mp4 16,9 full     the annotator listed EVERYTHING in the segment: the union is positive
                        and every other output is a certain negative (no masking)
The dataset turns <labels> [full] into a membership vector [n_labels + 1] (last entry =
full), and Taxonomy.rows() turns that into the target and mask rows.
"""

import json

import numpy as np
import torch
import torch.nn.functional as F


class Taxonomy:
    def __init__(self, cfg):
        self.cfg = cfg
        self.outputs = [o["name"] for o in cfg["outputs"]]
        self.labels = [l["name"] for l in cfg["labels"]]
        self.priority = [float(l["priority"]) for l in cfg["labels"]]
        self.display = [l.get("display", l["name"]) for l in cfg["labels"]]
        out_idx = {n: i for i, n in enumerate(self.outputs)}
        assert len(out_idx) == len(self.outputs), "duplicate output names"
        assert len(set(self.labels)) == len(self.labels), "duplicate label names"

        A, K = len(self.labels), len(self.outputs)
        self.positives = []
        for l in cfg["labels"]:
            for n in l["positives"] + l.get("unknown", []):
                assert n in out_idx, f"label {l['name']!r} names unknown output {n!r}"
            self.positives.append([out_idx[n] for n in l["positives"]])

        # carriers[k] = priorities of the labels that have output k as a positive
        carriers = [[self.priority[a] for a in range(A) if k in self.positives[a]] for k in range(K)]
        for k, c in enumerate(carriers):
            assert c, f"output {self.outputs[k]!r} is not a positive of any label: it can never be learned"

        target = np.zeros((A, K), np.float32)
        mask = np.zeros((A, K), np.float32)
        for a, l in enumerate(cfg["labels"]):
            for k in range(K):
                if k in self.positives[a]:
                    target[a, k], mask[a, k] = 1.0, 1.0
                elif all(p < self.priority[a] for p in carriers[k]):
                    mask[a, k] = 1.0  # certain negative
            for n in l.get("unknown", []):
                k = out_idx[n]
                assert k not in self.positives[a], f"{l['name']}: {n} is both positive and unknown"
                mask[a, k] = 0.0
        self.target = torch.from_numpy(target)
        self.mask = torch.from_numpy(mask)

    @classmethod
    def from_file(cls, path):
        with open(path, encoding="utf-8") as f:
            return cls(json.load(f))

    def spec_to_vec(self, spec):
        """'16' / '16,9' / '16,9 full' / '16,9|full' -> int64 membership vector [n_labels + 1]."""
        spec = spec.strip().replace("|", " ")
        labels, _, flag = spec.partition(" ")
        flag = flag.strip()
        assert flag in ("", "full"), f"unknown flag {flag!r} in label spec {spec!r}"
        vec = np.zeros(len(self.labels) + 1, np.int64)
        for tok in labels.split(","):
            a = int(tok)
            assert 0 <= a < len(self.labels), f"label {a} outside the {len(self.labels)} annotation labels: {spec!r}"
            vec[a] = 1
        vec[-1] = int(flag == "full")
        return vec

    @staticmethod
    def vec_to_spec(vec):
        """Inverse of spec_to_vec, with no spaces: '16,9|full'."""
        vec = np.asarray(vec)
        s = ",".join(str(i) for i in np.flatnonzero(vec[:-1]))
        return s + "|full" if vec[-1] else s

    def rows(self, vecs):
        """Membership vectors [N, n_labels + 1] (or plain label indices [N]) ->
        (target [N, K], mask [N, K]) as float32 numpy arrays."""
        vecs = np.asarray(vecs)
        if vecs.ndim == 1:  # plain annotation-label indices
            idx = vecs.astype(np.int64)
            return self.target[idx].numpy(), self.mask[idx].numpy()
        T, M = self.target.numpy(), self.mask.numpy()
        target = np.zeros((len(vecs), T.shape[1]), np.float32)
        mask = np.zeros_like(target)
        for i, v in enumerate(vecs):
            labels = np.flatnonzero(v[:-1])
            if len(labels) == 1 and not v[-1]:
                target[i], mask[i] = T[labels[0]], M[labels[0]]
                continue
            pos = T[labels].max(0)
            if v[-1]:
                mask[i] = 1.0
            else:
                primary = min(labels, key=lambda a: self.priority[a])
                mask[i] = np.maximum(M[primary], pos)
            target[i] = pos
        return target, mask

    def lookup(self, labels, device=None):
        """Batch labels (membership vectors [B, n_labels + 1] or indices [B]) ->
        (target [B, K], mask [B, K]) tensors."""
        t, m = self.rows(labels.cpu().numpy() if torch.is_tensor(labels) else labels)
        t, m = torch.from_numpy(t), torch.from_numpy(m)
        return (t.to(device), m.to(device)) if device is not None else (t, m)

    def decode(self, probs, thresholds):
        """Per-output probabilities -> (display label, [positive output names]).

        A label "fires" when all of its positives are predicted; the highest-priority
        fired label wins, which is the same rule the annotators follow. Labels with no
        positives (Normal, and the ones without enough data yet) never fire, so a
        segment with nothing above threshold is Normal.
        """
        on = {k for k, p in enumerate(probs) if p >= thresholds[k]}
        best = None
        for a, pos in enumerate(self.positives):
            if pos and all(k in on for k in pos):
                if best is None or self.priority[a] < self.priority[best]:
                    best = a
        name = self.display[best] if best is not None else "Normal"
        return name, [self.outputs[k] for k in sorted(on)]

    def describe(self):
        """Human-readable table: per label, its positives / negatives / unknowns."""
        rows = []
        for a, name in enumerate(self.labels):
            pos = [self.outputs[k] for k in range(len(self.outputs)) if self.target[a, k] > 0]
            neg = [self.outputs[k] for k in range(len(self.outputs)) if self.mask[a, k] > 0 and self.target[a, k] == 0]
            unk = [self.outputs[k] for k in range(len(self.outputs)) if self.mask[a, k] == 0]
            rows.append(f"{a:2d} {name:22s} +[{', '.join(pos)}]  -[{', '.join(neg)}]  ?[{', '.join(unk)}]")
        return "\n".join(rows)


def masked_bce(logits, target, mask):
    """Mean BCE over the known (mask == 1) entries only."""
    loss = F.binary_cross_entropy_with_logits(logits.float(), target, reduction="none")
    return (loss * mask).sum() / mask.sum().clamp_min(1.0)


def mix_multilabel(mixup_fn, x, target, mask):
    """Mixup / CutMix for multi-label targets. Mixes x in place exactly like Mixup.__call__,
    then mixes the target rows with the same lam. An output stays known only when it is
    known in both mixed samples."""
    assert len(x) % 2 == 0, "Batch size should be even when using this"
    if mixup_fn.mode == "elem":
        lam = mixup_fn._mix_elem(x)
    elif mixup_fn.mode == "pair":
        lam = mixup_fn._mix_pair(x)
    else:
        lam = mixup_fn._mix_batch(x)
    target = target * lam + target.flip(0) * (1.0 - lam)
    mask = mask * mask.flip(0)
    return x, target, mask


def average_precision(scores, target, mask):
    """AP of one output over the samples where it is known. NaN without positives."""
    keep = mask > 0
    s, t = scores[keep], target[keep]
    if t.sum() == 0:
        return float("nan")
    order = np.argsort(-s, kind="stable")
    t = t[order]
    tp = np.cumsum(t)
    precision = tp / np.arange(1, len(t) + 1)
    return float((precision * t).sum() / t.sum())


def best_f1_threshold(scores, target, mask):
    keep = mask > 0
    s, t = scores[keep], target[keep]
    if t.sum() == 0:
        return 0.5, float("nan")
    best = (0.5, -1.0)
    for th in np.arange(0.05, 0.96, 0.01):
        pred = s >= th
        tp = float((pred & (t > 0)).sum())
        fp = float((pred & (t == 0)).sum())
        fn = float((~pred & (t > 0)).sum())
        f1 = 2 * tp / max(2 * tp + fp + fn, 1.0)
        if f1 > best[1]:
            best = (float(round(th, 2)), f1)
    return best


def evaluate(probs, label_idx, tax):
    """probs [N, K] sigmoid scores, label_idx [N] annotation labels (or membership
    vectors [N, n_labels + 1]) -> metrics dict."""
    probs = np.asarray(probs, dtype=np.float64)
    target, mask = tax.rows(label_idx)
    per_class = []
    for k, name in enumerate(tax.outputs):
        ap = average_precision(probs[:, k], target[:, k], mask[:, k])
        th, f1 = best_f1_threshold(probs[:, k], target[:, k], mask[:, k])
        per_class.append({"name": name, "ap": ap, "threshold": th, "f1": f1,
                          "positives": int((target[:, k] * mask[:, k]).sum()),
                          "known": int(mask[:, k].sum())})
    aps = [c["ap"] for c in per_class if not np.isnan(c["ap"])]
    return {"mAP": 100.0 * float(np.mean(aps)) if aps else float("nan"), "per_class": per_class}


def format_report(metrics):
    lines = [f"mAP {metrics['mAP']:.2f}",
             f"{'output':16s} {'AP':>6s} {'F1':>6s} {'thr':>5s} {'pos':>6s} {'known':>6s}"]
    for c in metrics["per_class"]:
        ap = "  n/a" if np.isnan(c["ap"]) else f"{100 * c['ap']:6.1f}"
        f1 = "  n/a" if np.isnan(c["f1"]) else f"{100 * c['f1']:6.1f}"
        lines.append(f"{c['name']:16s} {ap} {f1} {c['threshold']:5.2f} {c['positives']:6d} {c['known']:6d}")
    return "\n".join(lines)
