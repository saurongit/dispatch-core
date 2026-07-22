# Dispatch Core

[Русский](README.md) | **English**

Dispatch Core is a self-hosted, messenger-first dispatch backend for small
field teams. It accepts work, offers or assigns it to an executor, records the
route and requires evidence before completion.

```text
request -> pool/direct assignment -> accept -> travel -> work -> report -> close
```

It is deliberately a focused operational core, not an unfinished attempt to
clone every CRM feature. Client, operator, master and administrator workplaces
live in messenger frontends; a dispatcher web board is outside the target
product. Sites, phone intake and external systems remain optional request
generators.

## Why this repository exists

The project demonstrates a production-oriented FastAPI/PostgreSQL design:

- explicit domain state machines rather than status strings changed by bots;
- a curated pool where interest does **not** assign the job and an operator
  deliberately chooses the executor;
- an optional `first_claim` pool for equal interchangeable crews;
- an optional coordinator: direct assignment and unattended first-claim flows
  do not require one;
- tenant-scoped rows and identities from the first migration;
- optimistic concurrency plus database constraints for race safety;
- transactional domain outbox, durable provider inbox and durable outbound
  queue;
- `FOR UPDATE SKIP LOCKED` consumers, bounded retries and dead-letter states;
- idempotent API creation and retry-safe messenger callbacks;
- append-only PostgreSQL GPS history;
- Telegram and MAX polling/webhook parsing, buttons, location and photo reports;
- address intake through native location, a protected map or manual text, with
  an explicit VPN/GPS-spoofing accuracy warning;
- a read-only customer map plus continuous browser GPS fallback when native
  messenger location is unavailable;
- separate 256-bit read/write capabilities kept out of query strings and
  revoked when work completes or is cancelled;
- non-root, read-only application containers with file-based Docker secrets.

The source and Git history are clean and independent from the private system
that inspired the workflow. No production credentials, customer data, fonts or
branding are included.

This is a public reference edition: it includes a runnable vertical slice for
architectural review and independent integration work. Managed provisioning,
customer infrastructure profiles, connectivity bundles, site generation and a
future self-service installer remain outside this repository and may be
delivered separately.

## Implemented vertical slice

```text
                         FastAPI / site / customer bot
                                     |
                                     v
Telegram or MAX -> durable inbox -> application commands
                                     |
                                     v
                PostgreSQL <- WorkOrder + TrackingSession
                     |               |
                     |        same transaction
                     |               v
                     +---------- domain outbox
                                     |
                         notification projector
                                     |
                              outbound queue
                                     |
                               Telegram / MAX
```

The guarded lifecycle is:

```text
SUBMITTED --publish curated----------> POOL_OPEN --operator assigns--> ASSIGNED
    |                                      |
    |                                      +--first claim------------> ASSIGNED
    +--direct assignment---------------------------------------------> ASSIGNED

ASSIGNED --accept--> ACCEPTED --travel (optional)--> EN_ROUTE
                              \-----------------------> IN_PROGRESS
EN_ROUTE --------------------------------------------> IN_PROGRESS
IN_PROGRESS --valid evidence-------------------------> COMPLETED

ASSIGNED --reject--> POOL_OPEN or SUBMITTED
any non-terminal state --cancel----------------------> CANCELLED
```

Completion requirements are data: minimum photos, comment, signature and
customer code can be independently enabled. The core contains no payroll or
revenue-share calculation.

## Quick checks

Python 3.12+ is supported.

```bash
python -m venv .venv
.venv/bin/pip install -e '.[server,dev]'
.venv/bin/python -m dispatch_core
.venv/bin/pytest
```

The in-memory demo completes a curated operator/executor flow without external
services. PostgreSQL integration tests run when `TEST_DATABASE_URL` is set.
The verified suite currently contains 670 passing tests, including 44 live
PostgreSQL integration tests. CI enforces 90% branch coverage across Python
3.12, 3.13 and 3.14.

## Run with Docker Compose

1. Copy `.env.example` to `.env` and replace every `change-me` value.
2. Start the base API and database:

```bash
docker compose up --build -d
curl http://127.0.0.1:8080/health/ready
```

The base profile deliberately disables messenger providers. To enable them,
create private token files and add the transport override:

```bash
mkdir -p secrets
for name in telegram_{client,operator,master,admin}_bot_token \
            max_{client,staff}_bot_token; do
  install -m 600 /dev/null "secrets/$name"
done
# Put the corresponding tokens in the files; never commit them.
docker compose -f compose.yaml -f compose.transports.example.yaml up --build -d
```

Telegram uses separate client/operator/master/admin role bots. MAX uses two
physical bots: one client bot and one shared staff bot. On `/start`, the staff
bot offers only the roles actually granted to that person.

In the normal flow an administrator creates a staff member and that person
binds a Telegram/MAX account with a one-time code. `POST /v1/actors` remains
available for provisioning and development fixtures; work can be created
through `POST /v1/orders`. Set `DISPATCH_ENVIRONMENT=development` to expose
`/docs` locally; schema endpoints are disabled in the default production
environment. See
[operations](docs/OPERATIONS.md) for exact examples, webhook registration,
backup and limitations.

Set `DISPATCH_PUBLIC_BASE_URL` to the public HTTPS origin for tracking. On
travel start, the master receives a native location request and a browser GPS
fallback link; the client receives a separate read-only map link. Both bearer
capabilities stay in URL fragments and are sent to same-origin APIs only in
headers.

Customer intake accepts an address through messenger-native location, a
one-time `/address#…` map link or text entry. The map uses a tap-to-unlock,
tap-to-select and relock interaction so it does not trap page scrolling. Saving
the point atomically consumes the capability. A VPN normally changes IP rather
than GPS, but the UI asks the customer to verify the point and use map/manual
entry when they are away from the service location or use GPS spoofing.

## Reliability semantics

- A webhook returns success only after the raw provider event is in PostgreSQL.
- A polling batch and its next cursor commit in one transaction.
- A domain change and its domain event commit in one transaction.
- Projection creates callback tokens/outbound messages and completes the outbox
  event in one transaction.
- Network delivery happens outside database transactions; success or retry is
  persisted afterward.
- Duplicate provider events and outbound messages are rejected by stable unique
  keys.
- A worker crash leaves claimed rows recoverable after a stale timeout.
- Unexpected loop failures use bounded exponential backoff instead of killing
  the whole worker.

This is at-least-once processing with idempotent boundaries, not a dishonest
claim of magical exactly-once networking.

## Product boundaries

Available now as a foundation: domain/application core, PostgreSQL, FastAPI,
Telegram/MAX adapters, frontend-isolated durable messaging, tracking, industry
packs, guided client intake, controlled staff binding, shared-MAX role
selection and operator/master workspaces. An operator can create, inspect and
safely disable an idle master; a master can navigate assigned active work and
the lifecycle actions valid for its current state.

Still pending: complete order cards and assignment from the operator menu,
operator report approval, the remaining customer conversations, installer UI,
SQLite desktop profile, attachment object storage and a separate hybrid edge.
A dispatcher web UI, route optimisation, billing, inventory, payroll and a
full sales CRM are deliberately outside the current product boundary. The
module catalog reports these boundaries explicitly.

- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Application catalog](docs/APPLICATIONS.md)
- [Connectivity](docs/CONNECTIVITY.md)
- [Roadmap](docs/ROADMAP.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## License

Dispatch Core is licensed under
[GNU Affero General Public License v3.0 only](LICENSE). If you modify it and
provide the resulting program as a network service, AGPL requires offering the
corresponding source to that service's users. Contact the repository owner if
you need commercial terms that do not use AGPL.
