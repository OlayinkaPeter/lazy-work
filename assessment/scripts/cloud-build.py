"""Optional cloud proxy support. Normal local builds use `docker build` unchanged.

Forward proxy settings without persisting credentials, resolve only the configured proxy,
and supply the platform CA bundle through a temporary BuildKit secret. Never disable TLS.
"""

import os
import socket
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

args = [
    "docker",
    "build",
    "--build-arg",
    "HTTP_PROXY",
    "--build-arg",
    "HTTPS_PROXY",
    "--build-arg",
    "NO_PROXY",
]
seen = set()
for key in ("HTTP_PROXY", "HTTPS_PROXY"):
    host = urlsplit(os.getenv(key, "")).hostname
    if host and host not in seen:
        seen.add(host)
        address = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
        args.extend(["--add-host", f"{host}:{address}"])
cert = os.getenv("SSL_CERT_FILE")
if cert and Path(cert).is_file():
    args.extend(["--secret", f"id=ca_bundle,src={cert}"])
args.extend(["-t", "donor-targeting:local", "."])
raise SystemExit(subprocess.run(args, check=False).returncode)
