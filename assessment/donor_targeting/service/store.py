"""Small durable queue. All claims and idempotency checks run in SQLite transactions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path


class Conflict(Exception):
    pass


class QueueFull(Exception):
    pass


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, org_id TEXT NOT NULL, request_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
                accepted REAL NOT NULL, updated REAL NOT NULL, result TEXT, error TEXT,
                UNIQUE(org_id,request_id))""")

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA synchronous=FULL")
        try:
            yield con
        finally:
            con.close()

    def enqueue(self, payload, now, capacity=8):
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(body.encode()).hexdigest()
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT * FROM jobs WHERE org_id=? AND request_id=?",
                (payload["org_id"], payload["request_id"]),
            ).fetchone()
            if existing:
                con.commit()
                if existing["fingerprint"] != fingerprint:
                    raise Conflict("request_id already used with different content")
                return dict(existing), False
            pending = con.execute(
                "SELECT count(*) FROM jobs WHERE status IN ('queued','scoring','delivering')"
            ).fetchone()[0]
            if pending >= capacity:
                con.rollback()
                raise QueueFull("queue capacity reached; retry later")
            job_id = uuid.uuid4().hex
            con.execute(
                "INSERT INTO jobs VALUES (?,?,?,?,?,'queued',?,?,NULL,NULL)",
                (job_id, payload["org_id"], payload["request_id"], fingerprint, body, now, now),
            )
            con.commit()
        return self.get(job_id), True

    def get(self, job_id):
        with self.connect() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def recover(self, now):
        with self.connect() as con:
            con.execute("UPDATE jobs SET status='queued',updated=? WHERE status='scoring'", (now,))
            con.execute(
                """UPDATE jobs SET status='delivery_unknown',updated=?,
                error='Worker stopped during CRM delivery; reconcile outbox before resubmitting'
                WHERE status='delivering'""",
                (now,),
            )

    def claim(self, now):
        with self.connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM jobs WHERE status='queued' ORDER BY accepted,id LIMIT 1"
            ).fetchone()
            if row:
                con.execute(
                    "UPDATE jobs SET status='scoring',updated=? WHERE id=?", (now, row["id"])
                )
            con.commit()
            return dict(row) if row else None

    def finish(self, job_id, status, now, result=None, error=None):
        with self.connect() as con:
            con.execute(
                "UPDATE jobs SET status=?,updated=?,result=?,error=? WHERE id=?",
                (status, now, json.dumps(result) if result is not None else None, error, job_id),
            )

    def counts(self):
        with self.connect() as con:
            return {
                row[0]: row[1]
                for row in con.execute("SELECT status,count(*) FROM jobs GROUP BY status")
            }
