# Roadmap

## v0.2: production hardening
- [ ] Streaming Sentinel: evaluate rules in the collector and publish findings to Kafka / webhooks
- [ ] Enforcement hook: `POST /api/authority/check` for gateways to ask "should this tool call proceed?" with the same rules
- [ ] Keep the whole session subtree when any run in it fails (`on_error: keep_subtree`)
- [ ] Auth: OIDC for the UI, per-namespace API keys, row-level access by namespace
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
