PY := uv run --frozen python

.PHONY: all setup data features train baseline test clean

all: setup data features train baseline  ## everything, from a fresh clone

setup:  ## install dependencies into .venv
	uv sync --frozen

data:  ## download the warehouse snapshot + upcoming requests (130 MB)
	$(PY) -m donor_targeting.download_data

features:  ## the DS team's feature sample -> data/warehouse/features.parquet
	$(PY) -m donor_targeting.features

train:  ## the existing model -> models/
	$(PY) -m donor_targeting.train

baseline:  ## BAU vs centroid precision/recall -> reports/baseline_report.md
	$(PY) -m donor_targeting.baseline

test:
	uv run pytest -q

clean:
	# Keep serving snapshots, durable jobs and CRM receipts for reconciliation.
	rm -rf data/warehouse data/requests data/benchmark models reports
	rm -f data/.snapshot_sha256 data/snapshot.tar.gz.part

.PHONY: policy prepare serve check benchmark benchmark-real deploy
policy:
	$(PY) -m donor_targeting.service.evaluate

prepare: policy
	$(PY) -m donor_targeting.service.prepare

serve:
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 $(PY) -m uvicorn donor_targeting.service.api:app --host 127.0.0.1 --port 8000 --workers 1

check:
	uv lock --check
	uv run --frozen ruff check .
	uv run --frozen ruff format --check .
	uv run --frozen pytest -q

benchmark:
	docker build -t donor-targeting:local .
	docker run --rm --memory=2g --cpus=4 --user "$$(id -u):$$(id -g)" -v "$$(pwd)/evidence:/app/evidence" donor-targeting:local python -m donor_targeting.service.benchmark --require-limits --synthetic --state /tmp/benchmark --output evidence/capacity.json

benchmark-real:
	docker run --rm --memory=2g --cpus=4 --user "$$(id -u):$$(id -g)" -v "$$(pwd)/data:/app/data" -v "$$(pwd)/evidence:/app/evidence" donor-targeting:local python -m donor_targeting.service.benchmark --require-limits --batches 1 --output evidence/assessment-replay.json

deploy:
	docker compose up --build -d --wait
