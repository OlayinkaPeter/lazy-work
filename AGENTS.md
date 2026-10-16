# Working with this user

- Investigations stay read-only until the user asks for code. Before behavioral changes, explain options and tradeoffs and obtain the user's decision.
- Verify claims against source and git history. Separate measured evidence from estimates and assumptions.
- Before calling work done, run the complete CI-style checks and test suite, inspect their output, and report skipped or blocked checks.
- Keep this file updated with new user rules and lessons.
- Tests covering the same flow must be a single `pytest.mark.parametrize` test. Use plain tuples and a separate `ids` list, never `pytest.param`. Hoist shared literals into module constants. Use optional parameters and conditional assertions for differences between cases.
- Messages drafted for the user to post: short, casual, direct; no em dash or right-arrow glyph. Use commas/semicolons and `->`. Avoid polished marketing language or long bullet essays.
- On macOS use Colima, not Docker Desktop. Install Docker buildx and the compose plugin. For SSH BuildKit builds, ensure buildx exists and export DOCKER_BUILDKIT=1 if necessary.
- Use the existing checkout in this isolated cloud task. Do not create worktrees unless explicitly requested.

## Assessment decisions (2026-10-16)

- Build inside `assessment/`, using the supplied chorusai-wave/donor-targeting-take-home starter.
- FastAPI ingress, durable jobs, bounded scoring concurrency, compact nightly artifacts.
- Audience policy: target at least 100% of estimated BAU donations AND dollars in backtesting; distinguish estimates from guarantees.
- Stale or unavailable audience data: fail explicitly, without CRM delivery. No random-audience fallback.

## Cloud setup findings

- Home directories can be read-only. Use `UV_CACHE_DIR=/tmp/donor-uv-cache` and `DOCKER_CONFIG=/tmp/donor-docker` in this cloud shell.
- Docker build networking needs the existing proxy's DNS mapping and platform CA bundle here. `assessment/scripts/cloud-build.py` handles those with a BuildKit secret; never disable TLS verification.
- Pre-create/chown named-volume mount directories in the image before switching to its unprivileged user. A fresh Docker volume mounted at an absent path otherwise prevented SQLite startup.
- The warehouse S3 hostname is blocked by the runtime proxy. Its exact allowance is saved in the environment draft; real backtest and two starter feature tests remain blocked until it takes effect.
- Keep synthetic capacity evidence separate from real-data financial evaluation. Do not mark the assessment complete until the full checks, holdout policy gate and all 12 real request replays have run.
