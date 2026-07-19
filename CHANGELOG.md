# Changelog

All notable changes are recorded here. The project has not published a stable
release yet.

## 0.1.0.dev0 — unreleased

- Added guarded WorkOrder and TrackingSession domain models.
- Added curated, first-claim and direct assignment policies.
- Added PostgreSQL migrations, durable inbox/outbox/outbound queues and
  race-preventing constraints.
- Added FastAPI commands and actor identity configuration.
- Added Telegram and MAX polling/webhook transports.
- Added evidence-backed completion and append-only, source-idempotent tracking.
- Added non-root Docker packaging, CI across Python 3.12–3.14 and a 90% branch
  coverage gate.
- Licensed the public repository under AGPL-3.0-only.
