"""Build an immutable serving snapshot; publish CURRENT only after validation."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
from scipy import sparse

from donor_targeting.config import SNAPSHOT_AT, WAREHOUSE_DIR
from donor_targeting.service.artifacts import Bundle, write_bundle


def prepare(warehouse, output, policy_path, snapshot_at):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    policy = json.loads(Path(policy_path).read_text())
    if policy.get("approved") is not True:
        raise ValueError("policy must pass the independent holdout before publishing")
    warehouse = Path(warehouse).resolve()
    con = duckdb.connect(config={"memory_limit": "1GB", "threads": 4})
    try:
        for table in ("people", "content", "timeline"):
            con.read_parquet(str(warehouse / f"{table}.parquet")).create_view(table)
        people = con.sql("SELECT id FROM people WHERE status = 'active' ORDER BY id").fetchnumpy()[
            "id"
        ]
        content = con.sql("SELECT id, dense_embedding FROM content ORDER BY id").fetchdf()
        # Matches the supplied pipeline: distinct content, first engagement time.
        pairs = con.execute(
            """
            SELECT t.person_id, t.content_id FROM timeline t JOIN people p ON p.id=t.person_id
            WHERE p.status='active' AND t.type IN ('opened','clicked')
              AND t.occurred_at < ?
            GROUP BY t.person_id, t.content_id HAVING min(t.occurred_at) >= ?
        """,
            [snapshot_at, snapshot_at - timedelta(days=30)],
        ).fetchnumpy()
        rows = np.searchsorted(people, pairs["person_id"])
        columns = np.searchsorted(content.id.to_numpy(), pairs["content_id"])
        weights = sparse.csr_matrix(
            (np.ones(len(rows), dtype=np.float32), (rows, columns)),
            shape=(len(people), len(content)),
        )
        embeddings = np.stack(content.dense_embedding).astype(np.float32)
        version = uuid.uuid4().hex
        path = output / version
        manifest = write_bundle(
            path,
            people,
            embeddings,
            weights,
            snapshot_at,
            policy,
            content_ids=content.id.tolist(),
            source="warehouse",
        )
        Bundle(path)
        temp = output / f".CURRENT-{version}"
        temp.write_text(version)
        os.replace(temp, output / "CURRENT")
        return manifest
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE_DIR)
    parser.add_argument("--output", type=Path, default=Path("data/artifacts"))
    parser.add_argument("--policy", type=Path, default=Path("reports/policy.json"))
    # Explicit simulated snapshot. Production must pass the actual warehouse watermark.
    parser.add_argument("--snapshot-at", type=datetime.fromisoformat, default=SNAPSHOT_AT)
    args = parser.parse_args()
    # Serving snapshots are mounted read-only by an unprivileged container user.
    os.umask(0o022)
    print(json.dumps(prepare(args.warehouse, args.output, args.policy, args.snapshot_at), indent=2))


if __name__ == "__main__":
    main()
