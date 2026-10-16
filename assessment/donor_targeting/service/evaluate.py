"""Campaign-level, out-of-time policy selection. Never tune against the final holdout."""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path

import duckdb
import numpy as np
from scipy import sparse

from donor_targeting.config import HISTORY_START, SNAPSHOT_AT, WAREHOUSE_DIR
from donor_targeting.service.artifacts import build_norms

FRACTIONS = np.round(np.arange(0.25, 1.001, 0.01), 2)
HOLDOUT = "2026-08-01"


def pick_fraction(calibration, safety_target=1.05):
    totals = calibration.sum(axis=0)  # fractions x [sent, donors, dollars, unsubs]
    full = totals[-1]
    retained = totals[:, 1:3] / (0.75 * full[1:3])
    eligible = np.where((retained >= safety_target).all(axis=1))[0]
    if not len(eligible):
        raise ValueError("no policy meets the calibration target")
    return int(eligible[0])


def relative_metrics(totals):
    selected, full = totals
    return {
        "audience_fraction": float(selected[0] / full[0]),
        "donations_vs_bau": float(selected[1] / (0.75 * full[1])),
        "dollars_vs_bau": float(selected[2] / (0.75 * full[2])),
        "unsubscribes_avoided_vs_bau": float(1 - selected[3] / (0.75 * full[3])),
    }


def evaluate(warehouse=WAREHOUSE_DIR, output=Path("reports")):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": "1GB", "threads": 4})
    for name in ("timeline", "content", "transactions"):
        con.read_parquet(str(Path(warehouse).resolve() / f"{name}.parquet")).create_view(name)
    con.sql("""CREATE TABLE engagement AS SELECT person_id, content_id,
        min(occurred_at) engaged_at FROM timeline
        WHERE type IN ('opened','clicked') GROUP BY person_id,content_id""")
    con.sql("""CREATE TABLE sends AS SELECT id, person_id, content_id, occurred_at sent_at
        FROM timeline WHERE type='sent'""")
    con.sql("""CREATE TABLE amounts AS SELECT s.id, sum(t.amount) amount
        FROM sends s JOIN transactions t USING (person_id,content_id)
        WHERE t.transaction_date >= s.sent_at AND t.transaction_date < s.sent_at + INTERVAL 7 DAY
        GROUP BY s.id""")
    con.sql("""CREATE TABLE unsubs AS SELECT person_id,content_id,1 unsubscribed
        FROM timeline WHERE type='unsubscribed'""")
    content = con.sql("SELECT * FROM content ORDER BY id").fetchdf()
    ids = content.id.to_numpy()
    emb = np.stack(content.dense_embedding).astype(np.float32)
    minimum = str(HISTORY_START + timedelta(days=30))
    maximum = SNAPSHOT_AT - timedelta(days=7)
    campaigns, dates, cubes = [], [], []
    for campaign in content.itertuples():
        frame = con.execute(
            """SELECT s.person_id, s.sent_at, coalesce(a.amount,0) amount,
            coalesce(u.unsubscribed,0) unsubscribed FROM sends s
            LEFT JOIN amounts a ON s.id=a.id LEFT JOIN unsubs u USING(person_id,content_id)
            WHERE s.content_id=? AND s.sent_at >= ?::TIMESTAMPTZ AND s.sent_at < ?
            ORDER BY s.person_id""",
            [campaign.id, minimum, maximum],
        ).fetchdf()
        if frame.empty:
            continue
        at = frame.sent_at.iloc[0].to_pydatetime()
        pairs = con.execute(
            """SELECT e.person_id,e.content_id FROM engagement e
            JOIN sends s ON s.person_id=e.person_id AND s.content_id=?
            WHERE e.engaged_at >= s.sent_at - INTERVAL 30 DAY AND e.engaged_at < s.sent_at
              AND s.sent_at >= ?::TIMESTAMPTZ AND s.sent_at < ?""",
            [campaign.id, minimum, maximum],
        ).fetchnumpy()
        people = frame.person_id.to_numpy()
        weights = sparse.csr_matrix(
            (
                np.ones(len(pairs["person_id"]), dtype=np.float32),
                (
                    np.searchsorted(people, pairs["person_id"]),
                    np.searchsorted(ids, pairs["content_id"]),
                ),
            ),
            shape=(len(people), len(ids)),
        )
        norms = build_norms(weights, emb)
        scores = np.full(len(people), -2, dtype=np.float32)
        np.divide(
            weights @ (emb @ emb[np.searchsorted(ids, campaign.id)]),
            norms,
            out=scores,
            where=norms > 1e-7,
        )
        hashed = people.astype(np.uint64) * np.uint64(11400714819323198485)
        order = np.lexsort((hashed, -scores))
        outcomes = np.column_stack(
            (
                np.ones(len(frame)),
                frame.amount.to_numpy() > 0,
                frame.amount.to_numpy(),
                frame.unsubscribed.to_numpy(),
            )
        )
        cumulative = outcomes[order].cumsum(axis=0)
        values = cumulative[np.ceil(FRACTIONS * len(frame)).astype(int) - 1]
        campaigns.append(int(campaign.id))
        dates.append(at.isoformat())
        cubes.append(values)
        print(f"backtest content={campaign.id}, sends={len(frame):,}", flush=True)
    con.close()
    cube = np.asarray(cubes)
    test = np.array([date[:10] >= HOLDOUT for date in dates])
    if test.sum() < 2 or (~test).sum() < 2:
        raise ValueError("need at least two calibration and two holdout campaigns")
    index = pick_fraction(cube[~test])
    selected = cube[test][:, [index, -1], :]
    metrics = relative_metrics(selected.sum(axis=0))
    rng = np.random.default_rng(42)
    bootstrap = [
        relative_metrics(selected[rng.integers(0, len(selected), len(selected))].sum(axis=0))
        for _ in range(2000)
    ]
    intervals = {
        key: np.quantile([b[key] for b in bootstrap], [0.025, 0.975]).tolist()
        for key in metrics
        if key != "audience_fraction"
    }
    approved = (
        metrics["donations_vs_bau"] >= 1
        and metrics["dollars_vs_bau"] >= 1
        and metrics["unsubscribes_avoided_vs_bau"] > 0
    )
    policy = {
        "fraction": float(FRACTIONS[index]),
        "approved": bool(approved),
        "target_vs_bau": 1.0,
        "calibration_safety_target": 1.05,
        "holdout_from": HOLDOUT,
        "holdout_campaigns": int(test.sum()),
        "calibration_campaigns": int((~test).sum()),
        "holdout_metrics": metrics,
        "holdout_totals": {
            "selected": selected.sum(axis=0)[0].tolist(),
            "full_observed": selected.sum(axis=0)[1].tolist(),
            "columns": ["sent", "donors", "dollars", "unsubs"],
        },
        "holdout_by_campaign": [
            {"content_id": campaigns[row], **relative_metrics(cube[row, [index, -1], :])}
            for row in np.where(test)[0]
        ],
        "campaign_bootstrap_95pct": intervals,
        "method": "minimum per-campaign cosine top-share meeting both calibration targets",
    }
    (output / "policy.json").write_text(json.dumps(policy, indent=2))
    (output / "backtest_campaigns.json").write_text(
        json.dumps(
            {
                "campaign_ids": campaigns,
                "dates": dates,
                "fractions": FRACTIONS.tolist(),
                "columns": ["sent", "donors", "dollars", "unsubs"],
                "outcomes": cube.tolist(),
            }
        )
    )
    print(json.dumps(policy, indent=2))
    if not approved:
        raise SystemExit(
            "Independent holdout misses retention or churn reduction. Policy is NOT approved; do not publish."
        )
    return policy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse", type=Path, default=WAREHOUSE_DIR)
    parser.add_argument("--output", type=Path, default=Path("reports"))
    args = parser.parse_args()
    evaluate(args.warehouse, args.output)


if __name__ == "__main__":
    main()
