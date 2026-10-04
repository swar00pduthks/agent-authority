# ADR 0003: Split storage by volume profile

**Status:** accepted

## Context
Marquez stores structure, run history and raw events in one Postgres database. Run history and raw events grow
with run count; structure grows with change. Postgres handles the first two poorly at agent scale.

## Decision
* **Run history + raw events + blobs → ClickHouse.** Lifecycle merge in `AggregatingMergeTree`, TTLs, ZSTD,
  tiering to object storage.
* **Graph → Postgres.** Small, current state with version history, shaped as a property graph so it can be
  mirrored into Apache AGE (openCypher) when deep path queries become core.
* **In-memory store** implements the same contract for demos and tests. Parity tests keep the two honest.

## Alternatives considered
* *Everything in a graph database*: supernodes (a popular dataset with millions of run edges) and write volume
  rule it out as the system of record.
* *Lakehouse (Delta/Iceberg)*: excellent for the raw log and analytics; a supported deployment variant, but adds
  seconds-to-minutes latency and is heavier to run for a default install.

## Consequences
Two databases to operate in production. `agenthub demo` needs none.
