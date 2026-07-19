# Security policy

## Supported version

This repository is an early development release. Only the latest commit on the
default branch receives security fixes; no stable compatibility or response
SLA is promised yet.

## Reporting

Do not open a public issue containing a vulnerability, credential, personal
data or exploitable deployment detail. Contact the repository owner privately
through the security-reporting channel configured on the hosting platform.
Include the affected version, impact, reproduction and any suggested fix.

## Deployment responsibilities

- Replace all example passwords and secrets.
- Keep `.env`, `secrets/`, database dumps and proxy configuration out of Git.
- Terminate production traffic with trusted TLS and restrict the admin API.
- Run supported PostgreSQL/Python/container versions and apply updates.
- Encrypt backups and test restoration.
- Define retention and access rules before collecting GPS or customer data.

The project has not undergone a third-party penetration test. It must not be
presented as certified for emergency, medical, financial or other regulated
workflows without a separate review.
