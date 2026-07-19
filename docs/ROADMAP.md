# Roadmap

## Implemented — portfolio-grade server core

- guarded WorkOrder and TrackingSession aggregates;
- optional coordinator, direct assignment, curated and first-claim pools;
- configurable photo/comment/signature/customer-code completion evidence;
- PostgreSQL migrations, optimistic locking and race-preventing constraints;
- durable inbox/cursors, transactional outbox and durable outbound queue;
- Telegram and MAX polling/webhook transports, buttons, photos and locations;
- FastAPI command/configuration surface and health endpoints;
- Docker Compose packaging and file-based provider secrets;
- 470+ tests, live PostgreSQL race/E2E tests and a 90% branch-coverage gate.

This milestone is usable as an integration backend and a technical portfolio.
It is not yet a self-service product for a non-technical dispatcher.

## Next — one real pilot without a source-code fork

- small dispatcher web board: create, filter, inspect, assign and cancel;
- configuration UI for actors, work types, fields and evidence requirements;
- customer/requester intake as an optional API/web/bot module;
- attachment object storage, retention settings and authorised download;
- visible queue/dead-letter health, structured logs and basic metrics;
- backup/restore command plus a tested recovery exercise;
- invitation/onboarding flow instead of raw actor API calls;
- release versioning, licence, threat model and operator runbook.

Exit criterion: a small non-technical team runs real work for a week without
editing YAML, environment variables or source code.

## Then — desktop and reusable packs

- durable local profile for an office PC or mini-PC;
- tray/launcher installer and guided first-run wizard;
- field service, local delivery, property and tourism starter packs;
- route/customer status view with explicit retention and access policy;
- signed, versioned connectivity bundle delivery with rollback;
- import/export and upgrade-safe backup migration.

Exit criterion: three unlike pilots use one core and declarative packs, not
three branded source forks.

## Only after pilots prove demand

- separate hybrid edge for office-hosted installations;
- scheduled/recurring work and crew assignment;
- Web/PWA executor transport;
- customer history and warranty/repeat work module;
- metrics, SLA analytics and operational alerting;
- provisioning for multiple managed installations.

## Deliberately postponed

Native mobile apps, AI configuration, SaaS billing, warehouse, payroll, 1C,
telephony, fuel/CAN/OBD telematics, general BPMN and automatic route
optimisation. These are expensive product branches, not prerequisites for a
credible dispatch core or the first paid deployment.
