# Donor audience service

FastAPI ingress, a durable SQLite job queue, one scoring worker, and compact nightly audience snapshots. Built from [the assessment starter](https://github.com/chorusai-wave/donor-targeting-take-home/tree/47eff8f7a37f768db7c1f4530194af69bd5948c6). The original brief is preserved in [ASSESSMENT.md](ASSESSMENT.md); see [DESIGN.md](DESIGN.md) for decisions and [evidence/README.md](evidence/README.md) for measured results and outstanding checks.

## Setup and run

Requires Python 3.12, uv 0.12.23, Docker Engine and the compose plugin; allow roughly 1 GB of warehouse/build space plus the container image. Offline starter feature generation recommends 8 GB RAM. Serving is limited to 2 GB and 4 CPUs.

On macOS use **Colima**, not Docker Desktop:

```sh
brew install colima docker docker-buildx docker-compose
colima start --cpu 4 --memory 4
```

Ensure Docker can discover the buildx and compose CLI plugins using Homebrew's installation instructions. BuildKit must be available; export `DOCKER_BUILDKIT=1` if your CLI is using the legacy builder.

From this directory:

```sh
make all          # locked install, checksum-verified data, starter features/model/baselines
make prepare      # full historical backtest, policy gate, immutable serving snapshot
make check        # lint, format, complete test suite
make deploy       # one-command container startup, 2 GB / 4 CPUs, waits for readiness
```

`make all` uses the inherited Makefile flow; installation uses the committed uv lockfile. No cloud credentials or paid services are required. The warehouse download requires HTTPS access to `chorus-ai-ds-takehome.s3.us-east-2.amazonaws.com` in addition to package registries. The service never downloads data at request time.

`make prepare` stops if the independent holdout misses either retention target or does not reduce unsubscribes. It will not publish an unvalidated policy or silently switch to random targeting. Investigate that result before deployment; do not tune on the holdout until it passes.

The supplied warehouse is dated September 1, 2026. Compose explicitly sets `REPLAY_AT=2026-09-01T16:00:00+00:00` to replay it. The readiness and job responses disclose replay mode. For a live warehouse remove this override, pass its actual watermark to preparation, and publish fresh snapshots nightly. `requested_at` never controls server freshness or deadlines.

For host development after preparation:

```sh
REPLAY_AT=2026-09-01T16:00:00+00:00 make serve
```

In the managed cloud shell, use `export UV_CACHE_DIR=/tmp/donor-uv-cache` if the default home cache is read-only. If Docker's isolated network cannot resolve the configured cloud proxy, use `DOCKER_CONFIG=/tmp/donor-docker python scripts/cloud-build.py`, then `docker compose up -d --wait` using that same Docker config. The helper forwards existing proxy settings and mounts the platform CA bundle as a BuildKit secret. It does not disable verification or persist proxy credentials. Normal local builds need no helper.

## API

```sh
curl -fsS -X POST http://127.0.0.1:8000/v1/audiences \
  -H 'Content-Type: application/json' \
  --data-binary @examples/requests/2003_clean_water.json
# 202; response includes job_id; Location points at /v1/jobs/<job_id>
curl -fsS http://127.0.0.1:8000/v1/jobs/<job_id>
```

`GET /healthz` checks process liveness. `GET /readyz` checks snapshot freshness, policy approval, and worker health. OpenAPI is at `/docs` while running.

- Same organization and `request_id`, same payload: return the existing job, HTTP 200, no duplicate CRM call.
- Same identity, different payload: 409. Invalid request: 422. Body over 64 KiB: 413. POST requires Content-Length; streaming uploads are unnecessary here.
- Missing/stale artifacts or unavailable worker: 503, no job accepted. Eight outstanding jobs maximum; further new requests get 429 and Retry-After.
- States: `queued -> scoring -> delivering -> succeeded`; pre-delivery failures become `failed`. Interrupted or failed CRM calls become `delivery_unknown`, requiring reconciliation before resubmission.
- Success includes the CRM list id, member count, snapshot version, queue-inclusive latency, and deadline outcome. Read status until terminal; an accepted request is not a completed delivery.

The demo is single-tenant (`demo-charity`) and binds to localhost. It assumes a trusted routing-agent gateway; do not expose the unauthenticated API directly to the Internet. A real ingress must authenticate the caller and bind its organization server-side.

## Benchmarks and persistence

```sh
make benchmark       # hard-limited container; 12 jobs in bursts of four, 250k delivered ids each
make benchmark-real  # hard-limited container; all 12 assessment requests after make prepare
```

Capacity fixtures select everyone deliberately to exercise the maximum CRM payload. They do not establish donation/churn performance. Both benchmarks start the real HTTP server, poll durable job status, verify every CRM file against its selected audience, reject duplicates, and fail on deadline misses. Raw JSON is written to `evidence/`.

Snapshots in `data/artifacts/<version>/` are immutable, hash-checked, and selected by an atomic `CURRENT` pointer. The worker pins a version for each job. Host runs retain SQLite and audience files in `data/service/` and CRM files in `data/crm_outbox/`. Compose retains those in separate named volumes; `docker compose down` preserves them, `down -v` deletes them. Never remove state volumes to resolve an uncertain CRM delivery. `make clean` removes offline inputs and reports but preserves serving artifacts, jobs and CRM receipts.

The preparation CLI creates container-readable snapshot files; never put credentials in these artifacts. Preparation can safely run repeatedly; it publishes a new version only after validation. Retain old versions while referenced by jobs. The snapshot date must be the warehouse watermark, never the build time. The supplied CRM keeps its lookup index in memory; durable reconciliation reads its CSV/JSON outbox files.

## Submission

Keep `DESIGN.md` at the root of the submitted assessment folder, along with code, evidence and `ai_sessions/`. Exclude `.venv/`, `data/`, caches and generated models/reports; `make all` and `make prepare` rebuild them. The parent `lazy-work/AGENTS.md` records collaboration rules, not a runtime dependency. CI in the parent repository runs the setup, full checks, policy gate and both constrained benchmarks.
