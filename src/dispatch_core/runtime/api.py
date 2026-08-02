from __future__ import annotations

import uvicorn

from dispatch_core.api import create_app
from dispatch_core.api.settings import Settings

from .factory import build_transports


def factory():
    settings = Settings()  # type: ignore[call-arg]
    transports = build_transports(settings, allowed_modes={"webhook"})
    return create_app(settings, transports=transports)


def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    options = {
        "factory": True,
        "host": settings.api_host,
        "port": settings.api_port,
        "proxy_headers": bool(settings.trusted_proxy_ips),
        "server_header": False,
    }
    if settings.trusted_proxy_ips:
        options["forwarded_allow_ips"] = settings.trusted_proxy_ips
    uvicorn.run("dispatch_core.runtime.api:factory", **options)
