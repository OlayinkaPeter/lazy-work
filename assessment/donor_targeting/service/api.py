"""Run with one Uvicorn process. The worker and HTTP ingress share a durable queue."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from donor_targeting.service.runtime import Runtime, Settings
from donor_targeting.service.schema import TargetRequest
from donor_targeting.service.store import Conflict, QueueFull

MAX_BODY_BYTES = 64 * 1024


def public_job(job):
    return {
        "job_id": job["id"],
        "request_id": job["request_id"],
        "status": job["status"],
        "accepted_at": job["accepted"],
        "updated_at": job["updated"],
        "result": json.loads(job["result"]) if job["result"] else None,
        "error": job["error"],
    }


def create_app(settings=None, crm=None):
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
        )
        runtime = Runtime(settings, crm)
        runtime.start()
        app.state.runtime = runtime
        try:
            yield
        finally:
            runtime.close()

    app = FastAPI(title="Donor audience service", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        # Do not echo content or non-finite numbers into the JSON error response.
        errors = [
            {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return JSONResponse({"detail": errors}, status_code=422)

    @app.middleware("http")
    async def limit_body(request: Request, call_next):
        request.state.received_at = time.time()
        if request.method == "POST":
            size = request.headers.get("content-length")
            if size is None or not size.isdecimal():
                return JSONResponse(
                    {"detail": "valid Content-Length required; no chunked uploads"}, status_code=411
                )
            if int(size) > MAX_BODY_BYTES:
                return JSONResponse({"detail": "request body too large"}, status_code=413)
        return await call_next(request)

    @app.get("/healthz")
    def health():
        return {"status": "alive"}

    @app.get("/readyz")
    def ready(request: Request):
        runtime = request.app.state.runtime
        try:
            bundle, version = runtime.audience()
            if not runtime.thread.is_alive() or runtime.last_worker_error:
                raise ValueError("worker unavailable")
            return {
                "status": "ready",
                "artifact_version": version,
                "active_people": len(bundle.ids),
                "snapshot_at": bundle.snapshot.isoformat(),
                "artifact_source": bundle.manifest.get("source", "unknown"),
                "replay_mode": settings.replay_at is not None,
                "jobs": runtime.store.counts(),
            }
        except (OSError, ValueError, KeyError, sqlite3.Error):
            raise HTTPException(503, "audience snapshot or worker unavailable") from None

    @app.post("/v1/audiences")
    def submit(payload: TargetRequest, request: Request):
        runtime = request.app.state.runtime
        # Idempotent repeats remain readable even if the next snapshot is not ready.
        try:
            # Check existing identity before readiness and queue capacity.
            body = payload.model_dump(mode="json")
            with runtime.store.connect() as con:
                existing = con.execute(
                    "SELECT id FROM jobs WHERE org_id=? AND request_id=?",
                    (payload.org_id, payload.request_id),
                ).fetchone()
            if not existing:
                ready(request)
            job, created = runtime.store.enqueue(body, request.state.received_at, settings.capacity)
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None
        except QueueFull as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "2"}) from None
        except sqlite3.Error:
            raise HTTPException(503, "job storage unavailable") from None
        return JSONResponse(
            public_job(job),
            status_code=202 if created else 200,
            headers={"Location": f"/v1/jobs/{job['id']}"},
        )

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str, request: Request):
        try:
            job = request.app.state.runtime.store.get(job_id)
        except sqlite3.Error:
            raise HTTPException(503, "job storage unavailable") from None
        if job is None:
            raise HTTPException(404, "job not found")
        return public_job(job)

    return app


app = create_app()
