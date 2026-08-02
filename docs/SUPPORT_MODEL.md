# Commercial support without artificial lock-in

Dispatch Core must earn recurring support revenue through operational value,
not through a hidden kill switch, backdoor, undocumented dependency or the
deliberate degradation of a customer's production system. The customer keeps
access to their data, documented recovery tools and a runnable core.

## The support product

The paid managed layer can provide capabilities that are expensive to operate
well across many installations:

- external heartbeat and queue-lag monitoring with routed alerts;
- scheduled encrypted off-host backups and independently recorded restore
  drills;
- controlled upgrades, migration rehearsals, signed release channels and
  rollback coordination;
- messenger/provider incident diagnostics, proxy/connectivity profiles and
  token-rotation assistance;
- capacity reviews, audit evidence, configuration reviews and a defined SLA;
- customer-specific IndustryPack configuration and integration certification.

The new `/v1/operations/queues` endpoint and worker heartbeat are the local,
vendor-neutral foundation for that service. A managed control plane should
consume least-privilege telemetry; it must not receive bot tokens, callback
capabilities, report contents or precise GPS unless a customer explicitly opts
in for a diagnosed incident.

## Subscription boundary

A subscription may license the hosted monitoring dashboard, automated backup
escrow, managed release feed, alert routing and guaranteed response time. On
expiry those managed services and SLA stop after a documented grace period.
Core dispatch, local administration, data export, backups already held by the
customer and disaster recovery continue to work.

This boundary makes unsupported operation possible but meaningfully harder for
a customer that lacks PostgreSQL, messaging and incident-response expertise.
That is legitimate operational complexity, not manufactured sabotage. It also
produces a stronger sales proposition: the customer pays for measured uptime,
verified recovery and accountable expertise.

## Commercial packaging

- **Care:** updates, monthly health review and business-hours consultation.
- **Managed:** external monitoring, backup verification, upgrade execution and
  incident response.
- **Critical:** tighter SLA, restore drills, capacity planning, security review
  and provider/connectivity escalation.

Contracts should state data ownership, telemetry scope, response windows,
backup RPO/RTO, exit assistance and what happens on subscription expiry. Any
future license enforcement belongs only at the premium-service boundary and
must never block safety-critical dispatch or access to customer data.
