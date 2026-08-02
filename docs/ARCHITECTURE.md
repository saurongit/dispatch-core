# Architecture

[Русский](ARCHITECTURE.ru.md) | **English**

## Stable centre, replaceable edges

```text
 REQUEST GENERATORS                   MESSENGER ROLE FRONTENDS
 site | phone intake | API | bot       Telegram | MAX
          |                                      |
          v                                      v
 +------------------- durable ingress / FastAPI -------------------+
 | authentication | provider deduplication | callback capabilities |
 +-------------------------------+---------------------------------+
                                 v
 +------------------------- application layer ---------------------+
 | commands | unit of work | identity | read models | idempotency  |
 +-------------------------------+---------------------------------+
                                 v
 +--------------------------- domain core --------------------------+
 | WorkOrder | pool policies | evidence | TrackingSession | events |
 +-------------------------------+---------------------------------+
                                 v
 +--------------------------- PostgreSQL ---------------------------+
 | aggregates | inbox/cursor | outbox | callbacks | outbound queue |
 +-------------------------------+---------------------------------+
                                 v
                      Telegram / MAX delivery
```

No provider adapter writes domain tables. A Telegram callback, MAX button or
HTTP request reaches the same application service and domain invariants.
Client, operator, master and admin user interfaces live in messenger
frontends; an operator web board is deliberately outside the target product.

The behavioural baseline is `dez_core_dr`. Industry packs generalise its
terminology, fields and evidence requirements, but do not replace its proven
client/operator/master/admin workflows.

## Work allocation policies

`curated` preserves the human dispatch pattern:

1. The request is published to the pool.
2. Executors press “ready”; each response is stored as `interested`.
3. No response assigns the order.
4. The coordinator sees candidates and deliberately selects one.
5. Other responses become `rejected`; the selected response becomes
   `selected`.

`first_claim` is an explicit alternative. The first transaction that locks the
order and commits wins. Direct assignment works without publishing a pool. A
coordinator can be attached, but is not a mandatory actor in the aggregate.

PostgreSQL also has a partial unique index that prevents one executor from
holding two active orders in the same organisation. That invariant survives
multiple API/worker processes and cannot be bypassed by a race in Python.

## Transaction and delivery boundaries

```text
provider POST/poll
      |
      +-- transaction: raw inbox event + polling cursor
      v
inbound worker
      |
      +-- transaction: aggregate mutation + domain outbox
      +-- durable deduplicated reply
      v
outbox projector
      |
      +-- transaction: capability tokens + outbound messages + projected mark
      v
sender -- network call --> provider
      |
      +-- delivered, retry with backoff, or dead letter
```

Delivery is at least once. Provider event IDs, aggregate versions, idempotency
keys, capability tokens and outbound deduplication keys make repetition safe at
the boundaries where it matters. Callback handlers recognise a previously
committed target state, so a crash between command commit and inbox completion
does not repeat the business effect.

Claims use `FOR UPDATE SKIP LOCKED`. A row abandoned in `processing` becomes
claimable after the stale interval. Each failed queue item has bounded
exponential backoff and a dead-letter state after the configured attempt limit.

## Aggregates

### WorkOrder

Owns organisation, request type/source, pack-defined details, optional
requester/coordinator, pool state, assignee, evidence policy, completion report
and optimistic version. Every transition checks its allowed predecessor and
actor.

### TrackingSession

Tracking is separate from the order because a route can contain many points.
Points store capture time, ingestion time, provider source and optional
accuracy. PostgreSQL appends only new points; recording a new location does not
delete or rewrite earlier history. One active tracking session per order is
enforced by a partial unique index.

Each active session owns two unrelated 256-bit bearer capabilities. The client
capability reads only a safe latest-point projection; the master capability may
append browser GPS points but cannot read the client view. Tokens live in URL
fragments, travel to the API only in headers and are cleared atomically when an
order completes or is cancelled. Telegram/MAX native location remains the
primary path; `/track/share` is the provider-neutral browser fallback.

Address collection is a separate capability flow. After the client supplies a
phone number, the intake FSM offers messenger-native location, manual text and
a one-time `/address#…` map. The browser sends the token only in a header. A
single PostgreSQL transaction locks the matching intake session, stores the
object coordinates, advances the FSM to services and removes the token. The
service point is copied into order details only after confirmation and is then
available to the safe tracking projection; no IP geolocation is used.

### Capabilities, actors and bindings

Messenger button payloads contain opaque expiring tokens, not trusted role or
order commands. The database resolves a token within its organisation and role
scope. A production actor represents one person and may have several explicit
memberships in `actor_roles`, plus controlled Telegram and MAX bindings. A
channel or bot frontend selects the effective role for the current interaction;
it does not grant that role.

This supports an optional nano preset where one owner is admin, operator and
master. Removing one membership preserves the actor, their other roles,
bindings and history. A shared MAX staff bot asks a multi-role actor to choose
the current mode; separate Telegram role bots derive it from the frontend.

Inbound identity includes `organization_id + provider + consumer_key +
external_event_id`. Client and staff bots can therefore receive the same
provider update ID without deduplicating or claiming each other's event. The
same routing boundary applies to polling cursors and outbound queues. A MAX
staff sender is the deliberate exception on egress: it claims `staff`, `admin`,
`operator` and `master` messages through one physical token, never `client`.

An unknown operator/master account first enters a durable pre-authentication
session. `/start` opens a 15-minute window with five failed attempts. The
administrator-issued four-digit code is consumed only if its active membership
matches the role selected by the frontend. A mismatch is rejected before an
external identity or membership is mutated. Successful binding clears both the
one-time code and the pre-authentication session; the same flow is parsed by
Telegram and MAX transports.

Operator workflow state is scoped by organisation, actor, effective role and
provider. This prevents a nano-owner's admin configuration flow, Telegram
operator flow and MAX operator flow from overwriting each other. Master
creation carries a durable request key protected by a PostgreSQL advisory lock
and unique index, so an inbound retry cannot create a second actor.

## Scale model

The deployable is a modular monolith. This keeps installation, transactions,
backup and debugging affordable while retaining extraction boundaries:

- all operational rows carry `organization_id`;
- command handlers depend on unit-of-work ports;
- long GPS history is isolated and append-only;
- notifications leave the transaction through an outbox;
- multiple consumers coordinate through database locks;
- transports depend on a provider-neutral contract;
- industry packs are data, not source forks.

If measured load requires it, tracking ingestion, provider delivery and a
public edge can be separated without changing the WorkOrder model. They are not
split pre-emptively.

## Deployment profiles

```text
Current server: API + worker + PostgreSQL on a customer VPS/server
Future desktop: same core + local durable database on an office computer
Future hybrid:  office core/data + small public durable edge
```

Telegram polling requires no public inbound port. MAX documentation recommends
webhooks for production, so a public HTTPS endpoint is the intended server
profile for MAX. Profiles are packaging choices, not product forks.

## Security invariants

- bot tokens, webhook secrets and proxy credentials never enter Git;
- webhook bodies are size-limited and authenticated with provider secrets;
- admin API keys use constant-time comparison and must be at least 32
  characters;
- application containers run as a fixed non-root user, read-only, without Linux
  capabilities;
- cross-organisation reads and token resolution are scoped in SQL;
- staff enrollment is time-bound, attempt-limited and role/frontend scoped;
- provider updates are durably isolated by organisation and logical frontend;
- network calls never hold a database transaction open;
- no location is accepted without an active tracking session;
- client read and master write tracking capabilities are distinct, rate-limited
  and revoked on every terminal transition;
- regulated-industry suitability is not claimed without a separate compliance
  and threat review.
