# Security policy

Please **do not** open public issues for security problems. Report them privately via GitHub Security Advisories
("Report a vulnerability" on the repository's Security tab). We aim to acknowledge reports within 3 working days.

## Deployment notes
- Set `AGENTHUB_API_KEY` to require a bearer token on ingest. The read API and UI have no auth yet (see the
  roadmap), so put them behind your SSO proxy.
- Offloaded prompts and completions may contain sensitive data. Restrict access to the `payload_blob` table and
  set retention to suit your policies.
- `agenthub demo` binds to `127.0.0.1` by default and stores everything in memory.
