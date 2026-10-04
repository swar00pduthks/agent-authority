# Architecture

AgentHub keeps **OpenLineage on the wire** and changes everything behind it. The design follows from one
observation: a lineage system stores three things with very different volume profiles, and Marquez puts all
three in one Postgres database.

| Layer | Nature | Volume | Store |
|---|---|---|---|
| Lineage graph (jobs, datasets, agents, tools, people, edges) | Slowly changing structure | Small | Postgres (property graph, AGE-ready) |
| Run history | Append-only time series | Huge | ClickHouse `AggregatingMergeTree`, TTL'd |
| Raw events | Immutable audit record | Huge, cold | ClickHouse (ZSTD, tierable to object storage) → Iceberg export |

```
Emitters ──HTTP──▶ API ─┐
         ──Kafka─▶ Collector ─▶ Batch ─┬─▶ ClickHouse: facet_blob, payload_blob, runs, run_io, raw_events,
                                       │               tool_aggregates, ingest_stats
                                       └─▶ Postgres:   ah_nodes, ah_node_versions, ah_edges
API (Marquez-compatible + /api/hub) ◀── both stores ──▶ Web UI, Sentinel
```

## 1. Dehydration

Every facet is classified as **static** (rarely changes between runs) or **dynamic** (run-specific).

| Static, deduplicated by hash | Dynamic, kept on the run row |
|---|---|
| job `sql`, `sourceCode`, `sourceCodeLocation`, `jobType`, `documentation`, `ownership` | `nominalTime`, `parent`, `errorMessage` |
| run `spark.logicalPlan`, `spark_properties`, `spark_version`, `processing_engine`, `spark_unknown` | the `agent` facet |
| all dataset `facets` (schema, dataSource, symlinks, tags ...) except `lifecycleStateChange`, `version` | dataset `inputFacets` / `outputFacets` (e.g. `outputStatistics`) |

Static facets are canonicalised (sorted keys, compact JSON), hashed with SHA-256 (128-bit prefix) and replaced
with `{"$ref": "<hash>"}`. The `_producer` / `_schemaURL` envelope, identical across millions of facets, is
interned the same way as `{"$meta": "<hash>"}`, as is the event-level `producer` / `schemaURL`.

`rehydrate()` reverses this exactly; `GET /api/hub/runs/{id}/replay` rebuilds the original events for auditors.
The policy is configurable (`FacetPolicy`), and facets already dehydrated upstream pass through untouched.

## 2. Run merging in storage

The collector never holds run state. Each event becomes a *partial* run row:

| Column | Merge |
|---|---|
| `start_time` | `min` (set on START only) |
| `end_time`, `last_event_time` | `max` |
| `state` | `max` over an ordinal: START 1 < RUNNING 2 < COMPLETE 3 < ABORT 4 < FAIL 5 |
| `facet_refs` | union |
| `event_count` | sum |
| agent columns, `dyn_facets` | last non-null |

ClickHouse's `AggregatingMergeTree` applies these merges in the background. Reads always `GROUP BY` the sorting key,
so answers are correct before and after merges. The merge is commutative, so out-of-order and duplicate delivery is
safe. The in-memory store implements the same merge in Python (`RunRecord.merge`), and parity tests check that
both stores agree.

## 3. Capture policy (agents)

Agent runs are far chattier than batch jobs. For each event the policy decides `keep`, `aggregate` or `drop`:

1. FAIL / ABORT → keep
2. Policy decision `deny` → keep
3. Tool with side effects or outputs → keep; tool with inputs → keep (data access is always audited)
4. Read-only tool with no dataset I/O → aggregate into `(parent run, tool)` counts
5. Kind listed in `sample` (default `llm: 0.05`) → keep if `sha256(runId)` falls in the bucket, so START and
   COMPLETE get the same decision
6. Otherwise keep

**Structure is recorded before the decision.** Nodes and edges for dropped and aggregated events still land, so
the graph and blast radius are always complete; only per-run detail is reduced.

Prompts, completions and messages in the agent facet are moved to `payload_blob` as `{"$blob": h, "bytes": n}`,
which keeps events small and gives one place for redaction and retention.

## 4. Graph by change

| Thing | Written when |
|---|---|
| Node | First seen, or its attributes change (LRU-suppressed) |
| Node version | Its static facet hash changes (dataset schema, job facets), not on every run |
| Edge | Pre-aggregated per batch: one upsert per distinct `(src, dst, type)` with `min(first_seen)`, `max(last_seen)`, `sum(run_count)` |

Edge `run_count` counts each run once (on its terminal event). Edge types are `INPUT` (dataset→job),
`OUTPUT` (job→dataset), `INVOKES` (parent job→child job, from `ParentRunFacet`) and `ON_BEHALF_OF`
(person→session/agent).

Traversals use recursive CTEs. The tables are a property graph (`id`, `kind`, `attrs`; typed edges), so mirroring
into Apache AGE for openCypher path queries is a sync job, not a redesign (see [ADR 0003](adr/0003-split-stores-by-volume.md)).

## 5. Delivery guarantees

* **HTTP ingest**: synchronous; `201` means the batch is written.
* **Kafka**: offsets are committed after `flush()` succeeds, which is at-least-once. Replays are safe because blob,
  node and version writes are idempotent and run rows merge. Edge `run_count` can over-count on a replayed batch.
* On a failed flush the collector clears its write-suppression caches, so redelivered events rewrite every blob
  they need.
* Malformed events go to `openlineage.events.dlq` with the error in a header.
* Kafka messages should be keyed by `runId` so a run's events stay ordered within a partition. The OpenLineage
  Kafka transport does this by default.

## 6. Governance on top

* **Chain of authority** (`service.authority_chain`) walks parent runs to the session root and evaluates each hop:
  identity (effective principal, inherited down the chain), ownership (agents), policy decision, side-effect
  approval, and prior denial of the same tool in the session. Verdict: pass / warn / fail.
* **Blast radius** (`service.blast_radius`) is a downstream traversal over all edge types, flagging sensitive
  datasets (`pii`, `confidential`, `restricted`, `mnpi`, `gdpr` tags).
* **Sentinel** rules are deterministic Python over runs and the graph. Findings are grouped by
  `(rule, agent, target)` with an occurrence count, and their ids are stable per problem.

## 7. Scaling notes

* The collector is horizontally scalable: run one per few Kafka partitions. The caches only suppress writes, so
  losing them costs idempotent rewrites, never correctness.
* ClickHouse inserts are batched (`COLLECTOR_BATCH_MAX`, `COLLECTOR_BATCH_MS`); avoid tiny batches.
* `runs` and `run_io` have a TTL (`AGENTHUB_RUN_TTL_DAYS`, default 30). `raw_events` has none by default; attach a
  storage policy to move old parts to object storage, or export to Iceberg (roadmap).
* Column-level lineage is the biggest graph-volume risk. It will get its own table rather than going into
  `ah_edges`.

## 8. Platform variants

The design is platform-neutral. The same components map onto a lakehouse:

| AgentHub OSS | Databricks | Open lakehouse |
|---|---|---|
| Kafka | Event Hubs | Kafka |
| Collector | Lakeflow / Structured Streaming | Flink |
| ClickHouse runs + raw | Delta silver/bronze | Iceberg + Trino |
| Postgres graph | Lakebase (synced tables) | Postgres + Apache AGE |
| Sentinel / API | Same | Same |
