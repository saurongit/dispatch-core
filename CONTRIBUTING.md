# Contributing

[Русский](CONTRIBUTING.ru.md) | **English**

Dispatch Core keeps business rules independent from provider payloads and UI
frameworks. Changes should preserve that boundary.

## Development check

```bash
python -m venv .venv
.venv/bin/pip install -e '.[server,dev]'
.venv/bin/ruff check .
.venv/bin/pytest
```

Set `TEST_DATABASE_URL` to run PostgreSQL integration and concurrency tests.
`make coverage` enforces the configured branch-coverage floor when the database
tests are available.

## Change rules

- Add or update tests for every domain transition and failure/retry branch.
- Put domain invariants in aggregates, not Telegram/MAX handlers.
- Never perform provider network I/O inside a database transaction.
- Keep migrations forward-only and idempotent.
- Preserve organisation scoping in every query.
- Never commit real tokens, private keys, `.env`, session files or dumps.
- Document user-visible capability and compatibility changes.

Discuss broad lifecycle changes before coding them. A new industry normally
needs a pack or adapter, not a fork or another parallel state machine.
