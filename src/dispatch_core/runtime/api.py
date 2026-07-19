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
    uvicorn.run(
        "dispatch_core.runtime.api:factory",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        proxy_headers=False,
        server_header=False,
    )
