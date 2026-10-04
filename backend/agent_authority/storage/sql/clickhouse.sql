-- Agent Authority run store (ClickHouse). Applied idempotently on startup.
-- Volume profile: huge, append-only, time-bounded. Lifecycle events merge in storage.

CREATE TABLE IF NOT EXISTS {db}.facet_blob (
    hash        FixedString(32),
    facet_name  LowCardinality(String),
    body        String CODEC(ZSTD(9)),
    first_seen  DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree
ORDER BY hash;

CREATE TABLE IF NOT EXISTS {db}.payload_blob (
    hash        FixedString(32),
    body        String CODEC(ZSTD(9)),
    first_seen  DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
ORDER BY hash;

-- One logical row per run: START/RUNNING/COMPLETE partial rows collapse on merge.
-- Always read with GROUP BY (namespace, job_name, run_id).
CREATE TABLE IF NOT EXISTS {db}.runs (
    namespace        LowCardinality(String),
    job_name         String,
    run_id           String,
    job_kind         SimpleAggregateFunction(anyLast, LowCardinality(String)),
    root_run_id      SimpleAggregateFunction(anyLast, Nullable(String)),
    parent_run_id    SimpleAggregateFunction(anyLast, Nullable(String)),
    run_path         SimpleAggregateFunction(anyLast, String),
    start_time       SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    end_time         SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))),
    last_event_time  SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    state            SimpleAggregateFunction(max, UInt8),
    event_count      SimpleAggregateFunction(sum, UInt64),
    facet_refs       SimpleAggregateFunction(groupUniqArrayArray, Array(String)),
    dyn_facets       SimpleAggregateFunction(anyLast, String) CODEC(ZSTD(3)),
    agent_id         SimpleAggregateFunction(anyLast, Nullable(String)),
    owner            SimpleAggregateFunction(anyLast, Nullable(String)),
    on_behalf_of     SimpleAggregateFunction(anyLast, Nullable(String)),
    tool_name        SimpleAggregateFunction(anyLast, Nullable(String)),
    side_effecting   SimpleAggregateFunction(anyLast, Nullable(UInt8)),
    policy_decision  SimpleAggregateFunction(anyLast, Nullable(String)),
    policy_rule      SimpleAggregateFunction(anyLast, Nullable(String)),
    model            SimpleAggregateFunction(anyLast, Nullable(String)),
    error_message    SimpleAggregateFunction(anyLast, Nullable(String)),
    event_date       Date,
    INDEX idx_run_id run_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_root root_run_id TYPE bloom_filter GRANULARITY 4
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (namespace, job_name, run_id)
TTL event_date + INTERVAL {run_ttl_days} DAY;

CREATE TABLE IF NOT EXISTS {db}.run_io (
    dataset_id   String,
    run_id       String,
    direction    LowCardinality(String),
    schema_hash  Nullable(String),
    row_count    Nullable(UInt64),
    event_time   DateTime64(3, 'UTC'),
    INDEX idx_run run_id TYPE bloom_filter GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (dataset_id, event_time)
TTL toDate(event_time) + INTERVAL {run_ttl_days} DAY;

-- Dehydrated, lossless audit log. Move to object storage with a storage policy
-- (TTL ... TO VOLUME 'cold') or export to Iceberg; see docs/ARCHITECTURE.md.
CREATE TABLE IF NOT EXISTS {db}.raw_events (
    run_id      String,
    event_time  DateTime64(3, 'UTC'),
    event_type  LowCardinality(String),
    namespace   LowCardinality(String),
    job_name    String,
    bytes_in    UInt32,
    payload     String CODEC(ZSTD(6))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (run_id, event_time);

CREATE TABLE IF NOT EXISTS {db}.tool_aggregates (
    parent_run_id  String,
    tool_name      String,
    calls          SimpleAggregateFunction(sum, UInt64),
    failures       SimpleAggregateFunction(sum, UInt64),
    total_ms       SimpleAggregateFunction(sum, Float64),
    last_seen      SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree
ORDER BY (parent_run_id, tool_name);

CREATE TABLE IF NOT EXISTS {db}.ingest_stats (
    minute              DateTime('UTC'),
    events_in           UInt64,
    bytes_in            UInt64,
    bytes_stored        UInt64,
    facet_refs          UInt64,
    facet_new           UInt64,
    runs_kept_events    UInt64,
    sampled_out         UInt64,
    aggregated          UInt64,
    payloads_offloaded  UInt64
) ENGINE = SummingMergeTree
ORDER BY minute;
