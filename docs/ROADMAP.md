# Roadmap

[Русский](ROADMAP.ru.md) | **English**

## Implemented — portfolio-grade server core

- guarded WorkOrder and TrackingSession aggregates;
- optional coordinator, direct assignment, curated and first-claim pools;
- configurable photo/comment/signature/customer-code completion evidence;
- PostgreSQL migrations, optimistic locking and race-preventing constraints;
- durable inbox/cursors, transactional outbox and durable outbound queue;
- Telegram and MAX polling/webhook transports, buttons, photos and locations;
- native/map/manual client address intake with an atomic one-time map
  capability;
- read-only client map, native Telegram/MAX location requests and a browser GPS
  fallback with independent read/write capabilities;
- durable, role-scoped `/start -> code` staff enrollment with expiry and an
  attempt limit;
- frontend-isolated inbox/cursors and the two-bot MAX topology with a durable
  admin/operator/master selector in the shared staff bot;
- operator/master workspaces with master creation, role-safe removal, active
  order navigation and paired Telegram/MAX PostgreSQL E2E;
- FastAPI command/configuration surface and health endpoints;
- Docker Compose packaging and file-based provider secrets;
- worker heartbeat, tenant-scoped queue health, bounded terminal-record
  retention and a disposable PostgreSQL restore drill;
- 730 tests, including 55 live PostgreSQL race/E2E tests, and a 90% branch
  coverage gate.

This milestone is usable as an integration backend and a technical portfolio.
It is not yet a self-service product for a non-technical dispatcher.

## Next — messenger production parity without a source-code fork

The behavioural source of truth is `dez_core_dr`; `../ANCHOR.md` records the
non-negotiable product contract. All client, operator, master and admin
workplaces are implemented in Telegram/MAX.

- keep the now-green PostgreSQL integration suite mandatory in CI;
- port curated pool preview/diagnostics, deliberate assignment, calls and chat
  from `core_dr` into the implemented operator workspace;
- port master travel/location/report flow and operator report approval/final
  close;
- port the remaining client chat, status and review flows around the implemented
  tracking link;
- support explicitly assigned multi-role actors and the nano onboarding preset;
- add automatic card/button/FSM parity checks between Telegram and MAX;
- add attachment storage and an explicit business/GPS retention policy;

Exit criterion: a non-technical team configures an IndustryPack in the admin
bot and completes client -> operator -> master -> report -> operator close for a
week entirely through role bots, without YAML edits or a source fork.

## Then — packaging and reusable packs

- durable local profile for an office PC or mini-PC;
- tray/launcher installer and guided first-run wizard that provisions messenger
  frontends;
- field service, local delivery, property and tourism starter packs;
- configurable tile provider and explicit GPS retention/erasure policy;
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

Native mobile apps, SaaS billing, warehouse, payroll, 1C,
telephony, fuel/CAN/OBD telematics, general BPMN and automatic route
optimisation. These are expensive product branches, not prerequisites for a
credible dispatch core or the first paid deployment.
