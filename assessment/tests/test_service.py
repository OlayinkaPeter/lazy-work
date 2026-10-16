"""Behavioral tests for the serving path, independent of the remote assessment dataset."""

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from scipy import sparse

from donor_targeting.mocks.crm import MockCRM
from donor_targeting.service.api import create_app
from donor_targeting.service.artifacts import Bundle, write_bundle
from donor_targeting.service.evaluate import FRACTIONS, pick_fraction, relative_metrics
from donor_targeting.service.prepare import prepare
from donor_targeting.service.runtime import Runtime, Settings
from donor_targeting.service.schema import TargetRequest
from donor_targeting.service.store import Conflict, QueueFull, Store

NOW = datetime(2026, 9, 1, 16, tzinfo=timezone.utc)
SNAPSHOT = NOW - timedelta(hours=11)
PEOPLE = np.array([10, 20, 30, 40], dtype=np.int64)
EMBEDDINGS = np.eye(3, 1024, dtype=np.float32)
MEMBERSHIP = np.array([[1, 0, 0], [1, 1, 0], [0, 0, 0], [0, 0, 1]], dtype=np.float32)
POLICY = {"fraction": 0.5, "approved": True}
REQUEST = {
    "request_id": "req_test",
    "org_id": "demo-charity",
    "requested_at": NOW.isoformat(),
    "content": {
        "content_id": 2003,
        "subject": "Clean water",
        "program_area": "clean_water",
        "dense_embedding": EMBEDDINGS[0].tolist(),
    },
    "audience": {"type": "all_active_subscribers"},
    "destination": {"crm": "mock", "list_name": "Test audience"},
}


def request_copy():
    return json.loads(json.dumps(REQUEST))


@pytest.fixture
def configured(tmp_path):
    settings = Settings(
        artifacts=tmp_path / "artifacts",
        state=tmp_path / "state",
        outbox=tmp_path / "outbox",
        replay_at=NOW,
    )
    path = settings.artifacts / "v1"
    write_bundle(path, PEOPLE, EMBEDDINGS, sparse.csr_matrix(MEMBERSHIP), SNAPSHOT, POLICY)
    (settings.artifacts / "CURRENT").write_text("v1")
    return settings


def await_job(client, job_id):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        job = client.get(f"/v1/jobs/{job_id}").json()
        if job["status"] not in ("queued", "scoring", "delivering"):
            return job
        time.sleep(0.01)
    pytest.fail("worker did not reach terminal state")


@pytest.mark.parametrize(
    "query_index,fraction,expected",
    [
        (0, 0.5, [10, 20]),
        (1, 0.25, [20]),
        (2, 1.0, [40, 10, 20, 30]),
    ],
    ids=["aligned-top-half", "mixed-top-quarter", "all-includes-no-engagement"],
)
def test_compact_scores_match_dense(configured, query_index, fraction, expected):
    bundle = Bundle(configured.artifacts / "v1")
    query = EMBEDDINGS[query_index]
    dense = MEMBERSHIP @ EMBEDDINGS
    norms = np.linalg.norm(dense, axis=1)
    expected_scores = np.full(len(PEOPLE), -2, dtype=np.float32)
    np.divide(dense @ query, norms, out=expected_scores, where=norms > 0)
    np.testing.assert_allclose(bundle.scores(query), expected_scores, atol=1e-6)
    bundle.fraction = fraction
    assert bundle.select(query).tolist() == expected


@pytest.mark.parametrize(
    "change,status",
    [
        ("valid", 202),
        ("dimension", 422),
        ("zero", 422),
        ("nan", 422),
        ("json-nan", 422),
        ("program", 422),
        ("org", 422),
        ("naive-time", 422),
        ("audience", 422),
        ("unknown-field", 422),
        ("oversized", 413),
    ],
    ids=[
        "valid",
        "wrong-dimension",
        "zero-vector",
        "non-finite",
        "raw-json-nan",
        "unknown-program",
        "wrong-tenant",
        "timezone-required",
        "unsupported-audience",
        "extra-field",
        "body-limit",
    ],
)
def test_request_contract(configured, change, status):
    payload = request_copy()
    if change == "dimension":
        payload["content"]["dense_embedding"] = [1.0]
    elif change == "zero":
        payload["content"]["dense_embedding"] = [0.0] * 1024
    elif change == "nan":
        payload["content"]["dense_embedding"][0] = "NaN"
    elif change == "json-nan":
        payload["content"]["dense_embedding"][0] = float("nan")
    elif change == "program":
        payload["content"]["program_area"] = "unknown"
    elif change == "org":
        payload["org_id"] = "another-charity"
    elif change == "naive-time":
        payload["requested_at"] = "2026-09-01T16:00:00"
    elif change == "audience":
        payload["audience"]["type"] = "everyone"
    elif change == "unknown-field":
        payload["extra"] = "no"
    elif change == "oversized":
        payload["content"]["subject"] = "x" * 70_000
    with TestClient(create_app(configured, MockCRM(configured.outbox, latency_s=0))) as client:
        response = client.post(
            "/v1/audiences",
            content=json.dumps(payload),
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == status, response.text


@pytest.mark.parametrize(
    "problem",
    [("missing",), ("stale",), ("future",), ("corrupt",), ("unapproved",)],
    ids=["missing", "stale", "future", "checksum", "unapproved-policy"],
)
def test_unready_never_delivers(configured, problem):
    problem = problem[0]
    if problem == "missing":
        (configured.artifacts / "CURRENT").unlink()
    elif problem == "corrupt":
        (configured.artifacts / "v1" / "people.npy").write_bytes(b"corrupt")
    else:
        manifest_path = configured.artifacts / "v1" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        if problem == "unapproved":
            manifest["policy"]["approved"] = False
        else:
            at = NOW - timedelta(days=3) if problem == "stale" else NOW + timedelta(hours=1)
            manifest["snapshot_at"] = at.isoformat()
        manifest_path.write_text(json.dumps(manifest))
    with TestClient(create_app(configured)) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 503
        assert client.post("/v1/audiences", json=REQUEST).status_code == 503
    assert not list(configured.outbox.glob("*.csv"))


def test_end_to_end_and_idempotency_survive_restart(configured):
    crm = MockCRM(configured.outbox, latency_s=0)
    with TestClient(create_app(configured, crm)) as client:
        first = client.post("/v1/audiences", json=REQUEST)
        assert first.status_code == 202
        job_id = first.json()["job_id"]
        job = await_job(client, job_id)
        assert job["status"] == "succeeded"
        assert job["result"]["deadline_met"]
        list_id = job["result"]["list_id"]
        assert (configured.outbox / f"{list_id}.csv").read_text() == "person_id\n10\n20\n"
        assert np.load(configured.state / f"{job_id}.npy").tolist() == [10, 20]
    with TestClient(create_app(configured, crm)) as client:
        again = client.post("/v1/audiences", json=REQUEST)
        assert again.status_code == 200
        assert again.json()["job_id"] == job_id
        changed = request_copy()
        changed["destination"]["list_name"] = "Different"
        assert client.post("/v1/audiences", json=changed).status_code == 409
        assert client.get("/v1/jobs/missing").status_code == 404
    assert len(list(configured.outbox.glob("*.csv"))) == 1


@pytest.mark.parametrize(
    "initial,expected",
    [
        ("queued", "queued"),
        ("scoring", "queued"),
        ("delivering", "delivery_unknown"),
        ("succeeded", "succeeded"),
        ("failed", "failed"),
    ],
    ids=["queued", "requeue-scoring", "uncertain-delivery", "keep-success", "keep-failure"],
)
def test_restart_recovery(tmp_path, initial, expected):
    store = Store(tmp_path / "jobs.sqlite3")
    job, _ = store.enqueue(REQUEST, time.time())
    store.finish(job["id"], initial, time.time())
    store.recover(time.time())
    assert store.get(job["id"])["status"] == expected


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("expired", "failed"),
        ("stale-after-accept", "failed"),
        ("crm", "delivery_unknown"),
    ],
    ids=["queue-deadline", "snapshot-expired-while-queued", "ambiguous-crm-error"],
)
def test_worker_failure_boundaries(configured, failure, expected):
    class FailingCRM:
        calls = 0

        def create_list(self, *args, **kwargs):
            self.calls += 1
            raise TimeoutError("response lost")

    crm = FailingCRM()
    runtime = Runtime(configured, crm)
    accepted = time.time() - (180 if failure == "expired" else 0)
    job, _ = runtime.store.enqueue(REQUEST, accepted)
    if failure == "stale-after-accept":
        configured.replay_at = NOW + timedelta(days=4)
    runtime.process(job)
    assert runtime.store.get(job["id"])["status"] == expected
    assert crm.calls == (1 if failure == "crm" else 0)


def test_concurrent_idempotency_and_capacity(tmp_path):
    store = Store(tmp_path / "jobs.sqlite3")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.enqueue(REQUEST, time.time()), range(24)))
    assert len({job["id"] for job, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    changed = request_copy()
    changed["content"]["subject"] = "different"
    with pytest.raises(Conflict):
        store.enqueue(changed, time.time())
    changed["request_id"] = "second"
    with pytest.raises(QueueFull):
        store.enqueue(changed, time.time(), capacity=1)


def test_snapshot_swap_is_atomic_and_old_jobs_pin_version(configured):
    runtime = Runtime(configured)
    old, version = runtime.audience()
    assert version == "v1"
    write_bundle(
        configured.artifacts / "v2",
        PEOPLE,
        EMBEDDINGS,
        sparse.csr_matrix(MEMBERSHIP),
        SNAPSHOT,
        {"fraction": 1.0, "approved": True},
    )
    (configured.artifacts / "CURRENT").write_text("v2")
    new, version = runtime.audience()
    assert version == "v2"
    assert old.fraction == 0.5
    assert new.fraction == 1.0


def test_only_one_worker_owns_state_directory(configured):
    first, second = Runtime(configured), Runtime(configured)
    first.start()
    try:
        with pytest.raises(RuntimeError, match="only one"):
            second.start()
    finally:
        first.close()


def test_prepare_filters_eligibility_and_lookback(tmp_path):
    warehouse, artifacts = tmp_path / "warehouse", tmp_path / "artifacts"
    warehouse.mkdir()
    pq.write_table(
        pa.table({"id": PEOPLE, "status": ["active", "unsubscribed", "active", "bounced"]}),
        warehouse / "people.parquet",
    )
    pq.write_table(
        pa.table({"id": [1, 2, 3], "dense_embedding": EMBEDDINGS.tolist()}),
        warehouse / "content.parquet",
    )
    # Repeated click must not double-count content. Exact left boundary is included.
    pq.write_table(
        pa.table(
            {
                "person_id": [10, 10, 20, 30, 30],
                "content_id": [1, 1, 2, 2, 3],
                "type": ["opened", "clicked", "opened", "opened", "opened"],
                "occurred_at": [
                    SNAPSHOT - timedelta(days=30),
                    SNAPSHOT - timedelta(days=1),
                    SNAPSHOT - timedelta(days=1),
                    SNAPSHOT - timedelta(days=31),
                    SNAPSHOT,
                ],
            }
        ),
        warehouse / "timeline.parquet",
    )
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(POLICY))
    prepare(warehouse, artifacts, policy_path, SNAPSHOT)
    version = (artifacts / "CURRENT").read_text()
    bundle = Bundle(artifacts / version)
    assert bundle.ids.tolist() == [10, 30]
    np.testing.assert_array_equal(bundle.weights.toarray(), [[1, 0, 0], [0, 0, 0]])
    previous = version
    policy_path.write_text(json.dumps({"fraction": 0.5, "approved": False}))
    with pytest.raises(ValueError, match="holdout"):
        prepare(warehouse, artifacts, policy_path, SNAPSHOT)
    assert (artifacts / "CURRENT").read_text() == previous


def test_policy_calibration_and_bau_denominator():
    cube = np.zeros((3, len(FRACTIONS), 4))
    for j, fraction in enumerate(FRACTIONS):
        cube[:, j, :] = [1000 * fraction, 100 * fraction, 5000 * fraction, 20 * fraction]
    index = pick_fraction(cube)
    assert FRACTIONS[index] == 0.79  # 0.79 / 0.75 > 1.05; 0.78 / 0.75 < 1.05
    metrics = relative_metrics(cube.sum(axis=0)[[index, -1]])
    assert metrics["donations_vs_bau"] == pytest.approx(0.79 / 0.75)
    assert metrics["unsubscribes_avoided_vs_bau"] == pytest.approx(1 - 0.79 / 0.75)


def test_request_model_preserves_valid_example():
    example = Path("examples/requests/2003_clean_water.json")
    assert TargetRequest.model_validate_json(example.read_text()).content.content_id == 2003


@pytest.mark.parametrize(
    "churn_pattern,expected_approval",
    [("tail", True), ("uniform", False)],
    ids=["meets-business-goal", "rejects-increased-churn"],
)
def test_full_backtest_uses_time_split_and_attribution(tmp_path, churn_pattern, expected_approval):
    from donor_targeting.service.evaluate import evaluate

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    dates = [
        datetime(2026, month, day, 14, tzinfo=timezone.utc)
        for month, day in [(6, 2), (7, 2), (8, 4), (8, 20)]
    ]
    content_ids = [1001, 1002, 1003, 1004]
    pq.write_table(
        pa.table({"id": content_ids, "dense_embedding": np.tile(EMBEDDINGS[0], (4, 1)).tolist()}),
        warehouse / "content.parquet",
    )
    events, gifts = [], []
    person_ids = np.arange(1, 101)
    hashes = person_ids.astype(np.uint64) * np.uint64(11400714819323198485)
    tail = set(person_ids[np.argsort(hashes)[-20:]].tolist())
    for content_id, at in zip(content_ids, dates, strict=True):
        for person in range(1, 101):
            event_id = len(events) + 1
            events.append(
                {
                    "id": event_id,
                    "person_id": person,
                    "content_id": content_id,
                    "type": "sent",
                    "occurred_at": at,
                }
            )
            gifts.append(
                {
                    "id": len(gifts) + 1,
                    "person_id": person,
                    "content_id": content_id,
                    "transaction_date": at + timedelta(days=1),
                    "amount": 10.0,
                }
            )
            # Excluded at the exact end of the seven-day attribution window.
            gifts.append(
                {
                    "id": len(gifts) + 1,
                    "person_id": person,
                    "content_id": content_id,
                    "transaction_date": at + timedelta(days=7),
                    "amount": 1000.0,
                }
            )
            if person in tail if churn_pattern == "tail" else person % 5 == 0:
                events.append(
                    {
                        "id": len(events) + 1,
                        "person_id": person,
                        "content_id": content_id,
                        "type": "unsubscribed",
                        "occurred_at": at + timedelta(hours=1),
                    }
                )
    pq.write_table(pa.Table.from_pylist(events), warehouse / "timeline.parquet")
    pq.write_table(pa.Table.from_pylist(gifts), warehouse / "transactions.parquet")
    if expected_approval:
        policy = evaluate(warehouse, tmp_path / "reports")
    else:
        with pytest.raises(SystemExit, match="NOT approved"):
            evaluate(warehouse, tmp_path / "reports")
        policy = json.loads((tmp_path / "reports" / "policy.json").read_text())
    assert policy["approved"] is expected_approval
    assert policy["fraction"] == 0.79
    assert policy["holdout_campaigns"] == 2
    assert policy["calibration_campaigns"] == 2
    campaigns = json.loads((tmp_path / "reports" / "backtest_campaigns.json").read_text())
    assert np.asarray(campaigns["outcomes"])[:, -1, 2].tolist() == [1000.0] * 4


@pytest.mark.parametrize(
    "operation,method,path",
    [
        ("counts", "GET", "/readyz"),
        ("enqueue", "POST", "/v1/audiences"),
        ("get", "GET", "/v1/jobs/missing"),
    ],
    ids=["readiness-storage-failure", "submission-storage-failure", "status-storage-failure"],
)
def test_storage_failure_is_explicit(configured, monkeypatch, operation, method, path):
    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError("disk unavailable")

    with TestClient(create_app(configured)) as client:
        monkeypatch.setattr(client.app.state.runtime.store, operation, unavailable)
        response = client.request(method, path, json=REQUEST if method == "POST" else None)
        assert response.status_code == 503
    assert not list(configured.outbox.glob("*.csv"))
