from __future__ import annotations

from urllib.parse import quote


def public_tracking_url(base_url: str, token: str) -> str:
    """Build a capability URL without putting the bearer token in HTTP requests."""
    if not token or len(token) < 43:
        raise ValueError("public tracking token is invalid")
    return f"{base_url.rstrip('/')}/track#{quote(token, safe='')}"


def location_submission_url(base_url: str, token: str) -> str:
    """Build the browser GPS sender URL with its bearer token in the fragment."""
    if not token or len(token) < 43:
        raise ValueError("location submission token is invalid")
    return f"{base_url.rstrip('/')}/track/share#{quote(token, safe='')}"


def intake_address_url(base_url: str, token: str) -> str:
    """Build a one-time address picker URL without leaking its token in logs."""
    if not token or len(token) < 43:
        raise ValueError("intake address token is invalid")
    return f"{base_url.rstrip('/')}/address#{quote(token, safe='')}"
