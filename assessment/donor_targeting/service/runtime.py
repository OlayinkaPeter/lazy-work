"""Single bounded worker, versioned snapshots, and explicit CRM ambiguity handling."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from donor_targeting.mocks.crm import MockCRM
from donor_targeting.service.artifacts import Bundle
from donor_targeting.service.store import Store

LOG = logging.getLogger(__name__)


@dataclass
class Settings:
    artifacts: Path = Path("data/artifacts")
    state: Path = Path("data/service")
    outbox: Path = Path("data/crm_outbox")
    max_age_hours: float = 26
    replay_at: datetime | None = None
    deadline_seconds: float = 180
    capacity: int = 8

    @classmethod
    def from_env(cls):
        replay = os.getenv("REPLAY_AT")
        return cls(
            artifacts=Path(os.getenv("ARTIFACT_DIR", "data/artifacts")),
            state=Path(os.getenv("STATE_DIR", "data/service")),
            outbox=Path(os.getenv("CRM_OUTBOX_DIR", "data/crm_outbox")),
            replay_at=datetime.fromisoformat(replay) if replay else None,
        )


class Runtime:
    def __init__(self, settings, crm=None):
        self.settings = settings
        if settings.replay_at is not None and settings.replay_at.tzinfo is None:
            raise ValueError("REPLAY_AT must include a timezone")
        self.store = Store(settings.state / "jobs.sqlite3")
        self.crm = crm if crm is not None else MockCRM(settings.outbox)
        self.bundle = None
        self.version = None
        self.bundle_lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.lock_file = None
        self.last_worker_error = None

    def start(self):
        self.lock_file = (self.settings.state / "worker.lock").open("a")
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock_file.close()
            raise RuntimeError("only one service process may use this state directory") from None
        self.store.recover(time.time())
        self.thread = threading.Thread(target=self.run, name="audience-worker", daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=30)
            if self.thread.is_alive():
                # Keep ownership until process termination; never let another worker recover live work.
                LOG.error("worker did not stop within grace period")
                return
        if self.lock_file:
            self.lock_file.close()

    def audience(self):
        with self.bundle_lock:
            version = (self.settings.artifacts / "CURRENT").read_text().strip()
            if not version or Path(version).name != version or version in (".", ".."):
                raise ValueError("invalid snapshot pointer")
            if version != self.version:
                loaded = Bundle(self.settings.artifacts / version)
                self.bundle, self.version = loaded, version
            now = self.settings.replay_at or datetime.now(timezone.utc)
            self.bundle.check_fresh(now, self.settings.max_age_hours)
            if self.bundle.manifest["policy"].get("approved") is not True:
                raise ValueError("audience policy has not passed holdout")
            return self.bundle, version

    def process(self, job):
        started = time.perf_counter()
        delivering = False
        result = None
        try:
            if time.time() - job["accepted"] > self.settings.deadline_seconds - 5:
                raise ValueError("deadline budget exhausted before delivery")
            bundle, version = self.audience()
            payload = json.loads(job["payload"])
            members = bundle.select(payload["content"]["dense_embedding"])
            audience_path = self.settings.state / f"{job['id']}.npy"
            temp_path = audience_path.with_suffix(".tmp")
            with temp_path.open("wb") as stream:
                np.save(stream, members, allow_pickle=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, audience_path)
            if time.time() - job["accepted"] > self.settings.deadline_seconds - 5:
                raise ValueError("deadline budget exhausted during scoring")
            result = {
                "member_count": len(members),
                "artifact_version": version,
                "artifact_source": bundle.manifest.get("source", "unknown"),
                "snapshot_at": bundle.snapshot.isoformat(),
                "fraction": bundle.fraction,
                "audience_file": audience_path.name,
                "replay_mode": self.settings.replay_at is not None,
            }
            self.store.finish(job["id"], "delivering", time.time(), result=result)
            delivering = True
            list_id = self.crm.create_list(
                payload["destination"]["list_name"],
                members,
                content_id=payload["content"]["content_id"],
            )
            result.update(
                list_id=list_id,
                elapsed_seconds=time.time() - job["accepted"],
                processing_seconds=time.perf_counter() - started,
            )
            result["deadline_met"] = result["elapsed_seconds"] <= self.settings.deadline_seconds
            self.store.finish(job["id"], "succeeded", time.time(), result=result)
            LOG.info(
                "job_completed id=%s members=%d elapsed=%.3f deadline_met=%s",
                job["id"],
                len(members),
                result["elapsed_seconds"],
                result["deadline_met"],
            )
        except Exception:
            LOG.exception("job_failed id=%s delivering=%s", job["id"], delivering)
            status = "delivery_unknown" if delivering else "failed"
            error = (
                "CRM outcome uncertain; reconcile outbox before resubmitting"
                if delivering
                else "Audience processing failed; inspect service logs; no CRM call made"
            )
            self.store.finish(job["id"], status, time.time(), result=result, error=error)

    def run(self):
        needs_recovery = False
        while not self.stop.is_set():
            try:
                if needs_recovery:
                    self.store.recover(time.time())
                    needs_recovery = False
                job = self.store.claim(time.time())
                if job:
                    self.process(job)
                else:
                    self.stop.wait(0.1)
                self.last_worker_error = None
            except Exception:
                needs_recovery = True
                self.last_worker_error = "worker storage failure"
                LOG.exception("worker loop failed")
                self.stop.wait(1)
