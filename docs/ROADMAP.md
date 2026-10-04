# Roadmap

## v0.2: production hardening
- [ ] Streaming Sentinel: evaluate rules in the collector and publish findings to Kafka / webhooks
- [ ] Enforcement hook: `POST /api/authority/check` for gateways to ask "should this tool call proceed?" with the same rules
- [ ] Keep the whole session subtree when any run in it fails (`on_error: keep_subtree`)
- [x] Auth: OIDC/Entra token validation with app roles (done)
- [ ] Per-namespace access control for reads
- [x] Tamper-evident audit record: hash chain + immutable anchors (done)
- [x] Human approvals from GitHub/GitLab webhooks (done)
- [ ] Postgres-only run store as the in-tenant default
- [ ] Versioned npm package for the web component; formal stamp variant; per-namespace prompt retention
- [ ] PII redaction hook for offloaded payloads
- [ ] Helm chart

## v0.3: scale and depth
- [ ] Apache AGE mirror for openCypher path queries (authority chains across sessions, delegation graphs)
- [ ] Column-level lineage in its own table
- [ ] Iceberg export of `raw_events` and an OpenLineage-compatible replay CLI
- [ ] OpenTelemetry trace linking (`trace_id` on runs) to jump to Langfuse / Jaeger
- [ ] Ingest Unity Catalog system tables and Marquez exports for migration

## Integrations
- [ ] OpenAI Agents SDK, CrewAI, AutoGen, MCP server/tool calls
- [ ] TypeScript SDK
- [ ] An MCP server so agents can ask the hub "what touched this dataset?"

## Ideas under discussion
- Agent trust scores from history (denials, failures, findings)
- Agent identity / passport verification (signed agent identities across organisations)
