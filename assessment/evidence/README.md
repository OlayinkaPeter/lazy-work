# Validation evidence

Measured October 16, 2026. These results separate serving capacity from financial validation. **The assessment is not yet fully validated:** the cloud proxy denies the warehouse download hostname.

## Constrained end-to-end capacity

`capacity.json` and `capacity.log` record a real HTTP server, durable queue, audience selection and complete mock CRM writes. Run with:

```sh
make benchmark
```

The cloud run used the optional `scripts/cloud-build.py` helper for proxy DNS and CA trust, followed by the exact Docker run in that target. TLS and locked package integrity checks remained enabled.

| Check | Measured result |
|---|---:|
| Memory limit, read from cgroup | 2,147,483,648 bytes |
| CPU quota, read from cgroup | 4 CPUs |
| Candidates per request | 250,000 |
| Delivered members per request | 250,000 |
| Requests | 12, in three bursts of four |
| Receipt-to-CRM delivery | 2.13 to 8.50 seconds |
| Server peak RSS | 156.8 MiB |
| Whole-container peak memory | 242.4 MiB |
| OOM events / kills | 0 / 0 |

Every list was compared in full with its persisted selected audience, checked for duplicates and checked against the deadline. Artifact preparation uses nine distinct engagements per person and 52 random unit content embeddings. The policy deliberately selects 100% to exercise a full 250k CRM payload. This is a **capacity fixture**, not an approved real-world targeting policy, active-subscriber audit against the warehouse, or evidence of donation/churn improvement. Container peak includes the benchmark driver and fixture preparation as well as the service. Twelve observations establish a useful smoke benchmark, not a production p99 estimate.

`compose-smoke.json` records a separate check of the shipped Compose configuration with a synthetic read-only artifact mount: healthy startup under the same limits, a complete 250k list, process restart, the same request returning the same completed job, and exactly one CRM CSV after restart. Only this check's temporary containers and named volumes were removed afterward. This validates the deployment mechanics and persistent idempotency, not the unavailable warehouse policy.

## Complete checks

`checks.txt` is the actual `make check` output, including the nonzero result:

- Lockfile consistency: passed.
- Ruff lint: passed.
- Ruff formatting: passed.
- Pytest: **41 passed, 2 setup errors, 0 skipped**.
- Both setup errors are the starter's real-warehouse feature tests: `no data/warehouse/: run make data first`.
- A Starlette warning notes deprecated httpx support in its test client. Tests still execute; the warning is not suppressed.

The passing checks exercise sparse/dense score equivalence, API validation including non-finite JSON, active-only preparation and window boundaries, attribution, independent time splits, rejection of increased churn, request identity conflicts, concurrent idempotency, queue bounds, stale/corrupt artifacts, snapshot swaps, deadline failures, worker ownership, restart recovery, ambiguous CRM failure, storage unavailability, and HTTP-to-CRM completion.

## Required remaining validation

The request to the upstream data archive failed with a proxy CONNECT `403 Forbidden`:

`https://chorus-ai-ds-takehome.s3.us-east-2.amazonaws.com/donor-targeting/data-2026-09-01.tar.gz`

The precise hostname was added to the environment configuration draft. The running machine has not received that allowance. No data was substituted, verification disabled, test skipped, policy fabricated or real donation/churn result claimed.

After saving the network allowance in environment settings, continue from this directory:

```sh
export UV_CACHE_DIR=/tmp/donor-uv-cache  # only needed for this cloud shell's read-only home
make all
make prepare
make check
make benchmark-real
make deploy
```

In this cloud shell, build using `DOCKER_CONFIG=/tmp/donor-docker python scripts/cloud-build.py` when needed, and use that Docker config for subsequent Docker/Compose commands. Normal local environments use the Makefile unchanged.

`make all` must finish starter feature generation, model training and the sample baseline. `make prepare` must finish the full warehouse backtest, approve both retention ratios plus churn reduction on untouched August campaigns, and publish the active-subscriber artifact. Copy the resulting policy and backtest reports into evidence and update DESIGN.md with actual selected share, absolute counts, uncertainty and business estimates. If the holdout fails, leave the policy unapproved and explain the result; do not tune away the failure on that same holdout.

`make benchmark-real` must replay all 12 actual request files and verify delivered ids against the active snapshot. The current real-data replay, business policy approval and real-data deployment are **unrun**, not passing. A new cloud task's restoration and environment publication are also unverified.
