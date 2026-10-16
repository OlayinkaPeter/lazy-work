FROM python:3.12.15-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
ENV PYTHONUNBUFFERED=1 UV_LINK_MODE=copy UV_CACHE_DIR=/tmp/uv-cache \
    PATH=/app/.venv/bin:$PATH OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
WORKDIR /app
# Optional corporate/cloud CA trust. Verification stays enabled; the bundle is not retained.
RUN --mount=type=secret,id=ca_bundle \
    if [ -f /run/secrets/ca_bundle ]; then export PIP_CERT=/run/secrets/ca_bundle; fi; \
    pip install --no-cache-dir uv==0.12.23
COPY pyproject.toml uv.lock README.md ./
COPY donor_targeting ./donor_targeting
RUN --mount=type=secret,id=ca_bundle --mount=type=cache,target=/tmp/uv-cache \
    if [ -f /run/secrets/ca_bundle ]; then export SSL_CERT_FILE=/run/secrets/ca_bundle; fi; \
    uv sync --frozen --no-dev --no-editable && chmod -R a+rX /app \
    && useradd --uid 10001 --create-home app \
    && mkdir -p /app/data/service /app/data/crm_outbox /app/evidence && chown -R app:app /app/data /app/evidence
USER app
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/readyz')"
CMD ["uvicorn", "donor_targeting.service.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--timeout-graceful-shutdown", "30"]
