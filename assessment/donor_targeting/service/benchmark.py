"""HTTP -> durable queue -> complete CRM file benchmark, including cold artifact loading."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from scipy import sparse

from donor_targeting.service.artifacts import write_bundle


def synthetic(root, people=250_000):
    """Capacity fixture, NOT financial validation or a deployable policy."""
    rng = np.random.default_rng(42)
    embeddings = rng.standard_normal((52, 1024), dtype=np.float32)
    embeddings /= np.linalg.norm(embeddings, axis=1, keepdims=True)
    ids = np.arange(1, people + 1, dtype=np.int64)
    # Nine recent distinct contents per person, more than many real subscribers.
    rows = np.repeat(np.arange(people), 9)
    cols = ((rng.integers(0, 52, people)[:, None] + np.arange(9)) % 52).ravel()
    weights = sparse.csr_matrix(
        (np.ones(len(rows), dtype=np.float32), (rows, cols)), shape=(people, 52)
    )
    at = datetime.now(timezone.utc)
    artifacts = root / "artifacts"
    write_bundle(
        artifacts / "synthetic",
        ids,
        embeddings,
        weights,
        at,
        {"fraction": 1.0, "approved": True},
        source="synthetic-capacity-fixture",
    )
    (artifacts / "CURRENT").write_text("synthetic")
    return artifacts, {
        "request_id": "capacity",
        "org_id": "demo-charity",
        "requested_at": at.isoformat(),
        "content": {
            "content_id": 2003,
            "subject": "Capacity fixture",
            "program_area": "clean_water",
            "dense_embedding": embeddings[0].tolist(),
        },
        "audience": {"type": "all_active_subscribers"},
        "destination": {"crm": "mock", "list_name": "capacity benchmark"},
    }


def http(port, method, path, payload=None):
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def rss_kib(pid):
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
        return max([int(line.split()[1]) for line in lines if line.startswith("VmHWM:")] or [0])
    except FileNotFoundError:
        return 0


def cgroup_values():
    result = {}
    for name in ("memory.max", "memory.peak", "cpu.max", "memory.events"):
        path = Path("/sys/fs/cgroup") / name
        if path.exists():
            result[name] = path.read_text().strip()
    return result


def benchmark(args):
    limits = cgroup_values()
    memory = limits.get("memory.max", "max")
    cpu = limits.get("cpu.max", "max").split()
    limits_verified = (
        memory.isdecimal()
        and int(memory) <= 2 * 1024**3
        and len(cpu) == 2
        and cpu[0].isdecimal()
        and int(cpu[0]) / int(cpu[1]) <= 4
    )
    if args.require_limits and not limits_verified:
        raise RuntimeError("benchmark requires cgroup memory <= 2 GiB and CPU quota <= 4")
    root = Path(args.state) / uuid.uuid4().hex
    root.mkdir(parents=True)
    environment = os.environ.copy()
    environment.update(
        STATE_DIR=str(root / "service"),
        CRM_OUTBOX_DIR=str(root / "outbox"),
        OPENBLAS_NUM_THREADS="1",
        OMP_NUM_THREADS="1",
    )
    environment.pop("REPLAY_AT", None)
    if args.synthetic:
        artifacts, payload = synthetic(root, args.people)
        payloads = [dict(payload, request_id=f"capacity-{i}") for i in range(args.concurrency)]
    else:
        artifacts = Path(args.artifacts).resolve()
        payloads = [
            json.loads(path.read_text()) for path in sorted(Path(args.requests).glob("*.json"))
        ]
        if len(payloads) != 12:
            raise ValueError("real assessment replay requires all 12 request files")
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        people_table = pq.read_table(
            Path(args.warehouse) / "people.parquet", columns=["id", "status"]
        )
        active_ids = people_table.filter(pc.equal(people_table["status"], "active"))[
            "id"
        ].to_numpy()
        environment["REPLAY_AT"] = "2026-09-01T16:00:00+00:00"
    environment["ARTIFACT_DIR"] = str(artifacts.resolve())
    log_path = root / "service.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "donor_targeting.service.api:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(args.port),
                "--log-level",
                "info",
                "--no-access-log",
            ],
            env=environment,
            stdout=log,
            stderr=log,
        )
        try:
            startup_at = time.perf_counter()
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError(log_path.read_text())
                try:
                    http(args.port, "GET", "/healthz")
                    break
                except (OSError, urllib.error.URLError):
                    if time.monotonic() > deadline:
                        raise TimeoutError("service startup timeout") from None
                    time.sleep(0.05)
            startup_seconds = time.perf_counter() - startup_at
            rows = []
            for batch in range(args.batches):
                pending = []
                for payload in payloads:
                    payload = dict(
                        payload,
                        request_id=f"{payload['request_id']}-{batch}-{uuid.uuid4().hex[:8]}",
                    )
                    started = time.perf_counter()
                    while True:
                        try:
                            job = http(args.port, "POST", "/v1/audiences", payload)
                            break
                        except urllib.error.HTTPError as exc:
                            if exc.code != 429 or time.perf_counter() - started > 150:
                                raise
                            time.sleep(0.2)
                    pending.append((started, job["job_id"]))
                for started, job_id in pending:
                    deadline = time.monotonic() + 180
                    while True:
                        job = http(args.port, "GET", f"/v1/jobs/{job_id}")
                        if job["status"] not in ("queued", "scoring", "delivering"):
                            break
                        if time.monotonic() > deadline:
                            raise TimeoutError(f"job {job_id} exceeded benchmark timeout")
                        time.sleep(0.02)
                    if job["status"] != "succeeded":
                        raise RuntimeError(f"job failed: {job}; log: {log_path.read_text()}")
                    result = job["result"]
                    members = np.load(
                        root / "service" / result["audience_file"], allow_pickle=False
                    )
                    csv = np.loadtxt(
                        root / "outbox" / f"{result['list_id']}.csv",
                        skiprows=1,
                        dtype=np.int64,
                        ndmin=1,
                    )
                    if not np.array_equal(csv, members) or len(np.unique(csv)) != len(csv):
                        raise AssertionError(
                            "CRM list differs from selected audience or has duplicate ids"
                        )
                    if not args.synthetic and not np.isin(csv, active_ids).all():
                        raise AssertionError("CRM list contains an inactive or unknown person")
                    if args.synthetic and len(csv) != args.people:
                        raise AssertionError("did not deliver the complete capacity audience")
                    if not result["deadline_met"]:
                        raise AssertionError("CRM delivery exceeded 180 seconds")
                    rows.append(
                        {
                            "job_id": job_id,
                            **result,
                            "observed_seconds": time.perf_counter() - started,
                        }
                    )
            evidence = {
                "kind": "synthetic_capacity" if args.synthetic else "assessment_12_request_replay",
                "limits_verified": limits_verified,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "candidates": args.people if args.synthetic else len(active_ids),
                "batches": args.batches,
                "requests": len(rows),
                "jobs": rows,
                "server_peak_rss_mib": rss_kib(process.pid) / 1024,
                "cold_start_seconds": rows[0]["observed_seconds"] if rows else None,
                "service_startup_seconds": startup_seconds,
                "cgroup": cgroup_values(),
                "log": str(log_path),
            }
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.with_suffix(".log").write_text(log_path.read_text())
            evidence["log"] = str(output.with_suffix(".log"))
            output.write_text(json.dumps(evidence, indent=2))
            print(json.dumps(evidence, indent=2))
            return evidence
        finally:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--require-limits", action="store_true")
    parser.add_argument("--people", type=int, default=250_000)
    parser.add_argument("--batches", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--artifacts", default="data/artifacts")
    parser.add_argument("--requests", default="data/requests")
    parser.add_argument("--warehouse", default="data/warehouse")
    parser.add_argument("--state", default="data/benchmark")
    parser.add_argument("--output", default="evidence/benchmark.json")
    benchmark(parser.parse_args())


if __name__ == "__main__":
    main()
