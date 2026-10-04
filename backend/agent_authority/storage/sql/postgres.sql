-- Agent Authority graph store (Postgres). Small, current structure + version history.
-- Shaped as a property graph so it can be mirrored into Apache AGE unchanged.

CREATE TABLE IF NOT EXISTS aa_nodes (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    namespace  TEXT NOT NULL,
    name       TEXT NOT NULL,
    attrs      JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS aa_nodes_ns_kind ON aa_nodes (namespace, kind);

CREATE TABLE IF NOT EXISTS aa_node_versions (
    node_id    TEXT NOT NULL REFERENCES aa_nodes(id) ON DELETE CASCADE,
    hash       TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (node_id, hash)
);

CREATE TABLE IF NOT EXISTS aa_edges (
    src        TEXT NOT NULL,
    dst        TEXT NOT NULL,
    type       TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen  TIMESTAMPTZ NOT NULL,
    run_count  BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (src, dst, type)
);
CREATE INDEX IF NOT EXISTS aa_edges_dst ON aa_edges (dst);
