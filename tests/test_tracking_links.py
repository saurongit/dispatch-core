from __future__ import annotations

import pytest

from dispatch_core.application.tracking_links import (
    intake_address_url,
    location_submission_url,
    public_tracking_url,
)


@pytest.mark.parametrize("token", ["", "short"])
@pytest.mark.parametrize(
    "builder", [public_tracking_url, location_submission_url, intake_address_url]
)
def test_tracking_link_builders_reject_weak_tokens(builder, token: str) -> None:
    with pytest.raises(ValueError, match="token"):
        builder("https://dispatch.example", token)


def test_tracking_link_builders_strip_trailing_base_slash() -> None:
    token = "t" * 43
    assert public_tracking_url("https://dispatch.example/", token) == (
        f"https://dispatch.example/track#{token}"
    )
    assert location_submission_url("https://dispatch.example/", token) == (
        f"https://dispatch.example/track/share#{token}"
    )
    assert intake_address_url("https://dispatch.example/", token) == (
        f"https://dispatch.example/address#{token}"
    )
