# Connectivity and resilient Telegram egress

[Русский](CONNECTIVITY.ru.md) | **English**

Restricted or unstable Telegram connectivity is handled at the transport
boundary. The honest product claim is **configurable alternative egress**, not
“Telegram always works”: no proxy, tunnel or provider can guarantee availability
against every network policy, outage or credential failure.

## Runtime model

```text
Telegram adapter
      |
      +--> direct HTTPS
      |
      +--> configured HTTP/SOCKS proxy (for example a local WireProxy endpoint)

MAX adapter uses its own direct/proxy setting.
Domain and application code never see routes or tunnel credentials.
```

Both production adapters currently accept one optional proxy URL through a
secret setting. The repository also contains a deterministic route-selection
model with priority and health metadata. Automated probes, WireProxy process
management, signed remote delivery and live route switching are not wired into
the runtime yet and remain a future connectivity-agent module.

## Safe configuration delivery target

1. The installation creates a local identity; its private key stays local.
2. A control endpoint returns versioned, expiring and signed metadata over
   HTTPS.
3. The agent verifies installation identity, signature, digest, expiry and
   monotonic version before accepting it.
4. Sensitive route material is encrypted to the installation or fetched using
   a short-lived reference; it is never embedded in the public repository.
5. A candidate route is health-checked before activation and the last
   known-good version remains available for rollback.
6. Logs expose route state but redact tokens, endpoints and private keys.

The updater should use an audited framework/implementation rather than custom
cryptography. Signed version metadata, expiration and rollback/freeze
protection are mandatory properties.

## Failover rules

- prefer the healthy route with the lowest numeric priority;
- optionally prefer healthy direct access;
- add cooldown and jitter to probes;
- never move an in-flight request between routes;
- retry only operations whose duplicate effect is controlled;
- expose `degraded` when a fallback works and `unavailable` when none does.

Telegram polling is the simplest zero-public-port deployment and remains fully
supported. Telegram webhooks are mutually exclusive with polling. MAX also
disallows simultaneous polling and webhook delivery; its current documentation
recommends HTTPS webhooks for production and polling for development/testing.

Provider references:

- Telegram Bot API: <https://core.telegram.org/bots/api>
- MAX API: <https://dev.max.ru/docs-api>
- MAX webhook subscription: <https://dev.max.ru/docs-api/methods/POST/subscriptions>
