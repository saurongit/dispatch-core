from __future__ import annotations

_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_SHORT_BLOCK_SIZE = 1_000
_SHORT_BLOCK_COUNT = 26
_SHORT_CAPACITY = _SHORT_BLOCK_SIZE * _SHORT_BLOCK_COUNT


def format_order_number(sequence: int) -> str:
    """Render a monotonically allocated sequence as a compact public number."""
    if sequence < 1:
        raise ValueError("order number sequence must be positive")
    offset = sequence - 1
    if offset < _SHORT_CAPACITY:
        prefix = chr(ord("A") + offset // _SHORT_BLOCK_SIZE)
        return f"{prefix}{offset % _SHORT_BLOCK_SIZE:03d}"
    return "X" + _base36(sequence).rjust(6, "0")


def _base36(value: int) -> str:
    digits: list[str] = []
    while value:
        value, remainder = divmod(value, len(_ALPHABET))
        digits.append(_ALPHABET[remainder])
    return "".join(reversed(digits))
