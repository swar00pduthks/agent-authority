# ADR 0005: Sample run detail, never structure

**Status:** accepted

## Context
Agent workloads emit many low-value events (LLM reasoning steps, read-only tool calls). Dropping them saves a lot,
but a lineage graph with holes is worse than no graph.

## Decision
The collector records nodes and edges for every event *before* the capture decision. Sampling and aggregation only
reduce per-run detail (run rows, raw events). Sampling is deterministic on `runId`. Failures, denials, side
effects and data access are always kept.

## Consequences
* Blast radius and catalogue stay complete under any sampling rate.
* Sampled-out LLM calls cannot be replayed; their latency and cost should go to tracing (OpenTelemetry/Langfuse),
  linked by trace id.
