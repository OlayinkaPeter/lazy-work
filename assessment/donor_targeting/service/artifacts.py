"""Exact centroid cosine via sparse content membership, without n_people x 1024 arrays."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from scipy import sparse

FILES = ("people.npy", "embeddings.npy", "weights.npz", "norms.npy")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def build_norms(weights, embeddings):
    """||sum embeddings|| using the tiny content Gram matrix, one content at a time."""
    gram = embeddings @ embeddings.T
    squared = np.zeros(weights.shape[0], dtype=np.float32)
    for col in range(weights.shape[1]):
        rows = weights[:, col].nonzero()[0]
        if len(rows):
            squared[rows] += np.asarray(weights[rows] @ gram[:, col]).ravel()
    return np.sqrt(np.maximum(squared, 0))


def write_bundle(path, ids, embeddings, weights, snapshot_at, policy, **metadata):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=False)
    weights = weights.astype(np.float32).tocsr()
    np.save(path / "people.npy", np.asarray(ids, dtype=np.int64), allow_pickle=False)
    np.save(path / "embeddings.npy", np.asarray(embeddings, dtype=np.float32), allow_pickle=False)
    sparse.save_npz(path / "weights.npz", weights, compressed=False)
    np.save(path / "norms.npy", build_norms(weights, embeddings), allow_pickle=False)
    manifest = {
        "version": 1,
        "snapshot_at": snapshot_at.isoformat(),
        "policy": policy,
        "count": len(ids),
        "checksums": {name: sha256(path / name) for name in FILES},
        **metadata,
    }
    (path / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


class Bundle:
    def __init__(self, path):
        self.path = Path(path)
        self.manifest = json.loads((self.path / "manifest.json").read_text())
        if self.manifest["version"] != 1:
            raise ValueError("unsupported artifact version")
        for name in FILES:
            if sha256(self.path / name) != self.manifest["checksums"][name]:
                raise ValueError(f"artifact checksum mismatch: {name}")
        self.ids = np.load(self.path / "people.npy", allow_pickle=False)
        self.embeddings = np.load(self.path / "embeddings.npy", allow_pickle=False)
        self.weights = sparse.load_npz(self.path / "weights.npz").tocsr()
        self.norms = np.load(self.path / "norms.npy", allow_pickle=False)
        self.snapshot = datetime.fromisoformat(self.manifest["snapshot_at"])
        self.fraction = float(self.manifest["policy"]["fraction"])
        n = len(self.ids)
        if (
            self.snapshot.tzinfo is None
            or not 0 < self.fraction <= 1
            or self.weights.shape != (n, len(self.embeddings))
            or self.embeddings.shape[1:] != (1024,)
            or self.norms.shape != (n,)
            or len(np.unique(self.ids)) != n
            or not 0 < n <= 250_000
            or n != self.manifest["count"]
            or np.any(self.ids <= 0)
            or not np.isfinite(self.weights.data).all()
            or np.any(self.norms < 0)
            or not np.isfinite(self.embeddings).all()
            or not np.isfinite(self.norms).all()
        ):
            raise ValueError("invalid artifact dimensions, policy, or values")

    def check_fresh(self, now: datetime, max_age_hours=26):
        age = now - self.snapshot
        if age < timedelta(0) or age > timedelta(hours=max_age_hours):
            raise ValueError("audience snapshot is stale or in the future")

    def scores(self, embedding):
        query = np.asarray(embedding, dtype=np.float32)
        query /= np.linalg.norm(query)
        numerator = self.weights @ (self.embeddings @ query)
        scores = np.full(len(self.ids), -2, dtype=np.float32)
        np.divide(numerator, self.norms, out=scores, where=self.norms > 1e-7)
        return scores

    def select(self, embedding):
        scores = self.scores(embedding)
        # Deterministic random-looking ties, including the no-engagement cohort.
        # Avoid favouring low ids (which can correlate with subscription age).
        hashed = self.ids.astype(np.uint64) * np.uint64(11400714819323198485)
        order = np.lexsort((hashed, -scores))
        k = math.ceil(len(order) * self.fraction)
        return self.ids[order[:k]]
