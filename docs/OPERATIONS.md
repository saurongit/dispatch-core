# Operations

## Configuration

Copy `.env.example` to `.env`. Generate independent random values for the
PostgreSQL password, admin API key and callback signing secret. Use a URL-safe
alphanumeric/hex PostgreSQL password because Compose interpolates it into a
connection URI. Do not reuse a bot token as a webhook or admin secret.

The base `compose.yaml` starts PostgreSQL, the API and the worker with messenger
receivers disabled. `compose.transports.example.yaml` is an example override;
keep the actual override and token files private.

```bash
docker compose up --build -d
docker compose ps
curl http://127.0.0.1:8080/health/live
curl http://127.0.0.1:8080/health/ready
```

`/health/live` only proves that the API process answers. `/health/ready` also
checks PostgreSQL.

## Register actors

The normal messenger flow does not require copying an external ID. The
implemented admin flow creates an operator and returns a short-lived four-digit
code; the operator opens the correct Telegram role bot or the shared MAX staff
bot, presses `/start` and enters it. From the operator workspace, the operator
can create a master by entering a name and phone number; the master binds with
the generated code through the Telegram master bot or the same MAX staff bot.
A code is one-time, frontend-scoped and protected by a 15-minute/five-attempt
input session. Retrying the final creation message reuses the same durable
creation request and cannot create a duplicate master.

The admin API remains useful for development fixtures:

```bash
curl -X POST http://127.0.0.1:8080/v1/actors \
  -H 'Authorization: Bearer YOUR_ADMIN_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "actor_id": "executor-1",
    "role": "master",
    "display_name": "Executor One",
    "provider": "telegram",
    "external_user_id": "123456789"
  }'
```

Roles are `admin`, `operator`, `master` and `client`. One actor may hold several
explicit roles and separate Telegram/MAX bindings.

## Create a request

Every create call requires a stable `Idempotency-Key`. Retrying the same key and
body returns the same order; reusing it with another body returns `409`.

```bash
curl -X POST http://127.0.0.1:8080/v1/orders \
  -H 'Authorization: Bearer YOUR_ADMIN_KEY' \
  -H 'Idempotency-Key: customer-request-2026-0001' \
  -H 'Content-Type: application/json' \
  -d '{
    "work_type": "lift_repair",
    "source": "phone",
    "requester_id": "resident-7",
    "details": {
      "summary": "Lift stopped",
      "address": "Demo street 7",
      "asset": "lift-42"
    },
    "evidence": {
      "minimum_photos": 1,
      "comment_required": true
    }
  }'
```

The API returns the order ID. In a local development environment, set
`DISPATCH_ENVIRONMENT=development` to enable `/docs` for pool, assignment,
tracking, completion and cancellation schemas. Production disables `/docs`,
`/redoc` and `/openapi.json`.

## Telegram

Polling needs no public endpoint. Set `TELEGRAM_RECEIVE_MODE=polling`, mount the
token file and start the transport override.

For webhook mode, publish `https://YOUR_DOMAIN/webhooks/telegram` through a
trusted TLS reverse proxy, set a random `TELEGRAM_WEBHOOK_SECRET`, and register
the URL using Telegram `setWebhook` with the same `secret_token`. Telegram
polling and webhooks cannot operate simultaneously.

## MAX

The supported topology has exactly two logical MAX frontends per organisation:

- `client`: the public client bot;
- `staff`: one shared bot for admin, operator and master work.

On `bot_started`/`/start`, the staff bot clears its previous mode and renders
only the roles currently granted to that actor. Its worker consumes all staff
role notifications but cannot claim the client queue. The sample Compose
override runs both MAX bots in polling mode, so it needs no public callback
endpoint and cannot mix their updates.

The adapter uses `https://platform-api2.max.ru` and sends the bot token in the
`Authorization` header. If a deployment chooses webhooks, publish a distinct
public URL for each physical bot and route each URL to a frontend instance with
the matching `client` or `staff` consumer key. Each instance exposes its own
`/webhooks/max` handler on HTTPS port 443. Create each bot subscription with the
corresponding URL and secret:

```bash
curl -X POST https://platform-api2.max.ru/subscriptions \
  -H 'Authorization: YOUR_MAX_BOT_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "https://MAX_CLIENT_OR_STAFF_DOMAIN/webhooks/max",
    "update_types": ["message_created", "message_callback", "bot_started"],
    "secret": "YOUR_MAX_WEBHOOK_SECRET"
  }'
```

MAX sends that secret as `X-Max-Bot-Api-Secret`; the API compares it before
persisting the event. Current MAX requirements allow only HTTPS on port 443
with a trusted certificate and recommend webhooks for production. A webhook
subscription and long polling cannot be active together.

## Reverse proxy and exposure

The Compose file publishes the API directly for development. In production,
place it behind a maintained TLS reverse proxy, expose only required routes,
rate-limit the admin API, and restrict it by network/VPN where possible. Set
trusted proxy handling explicitly if a future deployment needs client IPs; the
bundled Uvicorn launcher intentionally disables implicit proxy-header trust.

## Backup and recovery

The database is the system of record. A production runbook must include:

- encrypted PostgreSQL backups outside the host;
- a retention schedule appropriate for request, GPS and evidence metadata;
- a restoration test into an isolated database;
- separate backup of deployment configuration without mixing plaintext
  secrets into ordinary archives;
- verification that bot tokens and webhook secrets can be rotated.

Compose creates the `dispatch-postgres` volume but does not pretend that a
volume alone is a backup. Automated encrypted backup/restore tooling is still a
roadmap item.

## Queue recovery

Inbox, outbox and outbound rows have `pending`, `processing`, `delivered` or
`dead` states. A worker killed after claiming work does not lose it: stale
`processing` rows become eligible again. Repeated failures use bounded backoff
and eventually move to `dead`. A production UI/metric for dead-letter review is
not implemented yet; inspect and requeue only with an audited operator
procedure.
