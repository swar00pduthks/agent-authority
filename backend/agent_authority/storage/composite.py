"""Production store: ClickHouse for runs/raw/blobs, Postgres for the graph."""

from __future__ import annotations

import os
from typing import Any

from agent_authority.storage.base import Batch, Store, Subgraph
from agent_authority.storage.clickhouse import ClickHouseRunStore, ConnectClient
from agent_authority.storage.postgres import PostgresGraphStore


class CompositeStore(Store):
    def __init__(self, runs: ClickHouseRunStore, graph: PostgresGraphStore) -> None:
        self.runs = runs
        self.g = graph

    def ensure_schema(self) -> None:
        self.runs.ensure_schema()
        self.g.ensure_schema()

    def write_batch(self, batch: Batch) -> None:
        # Graph first: if ClickHouse fails the batch is retried and graph upserts are idempotent
        # except edge run counts, which tolerate rare double counting on retry.
        self.g.write(batch)
        self.runs.write(batch)

    # blobs
    def get_facet(self, h):
        return self.runs.get_facet(h)

    def get_payload(self, h):
        return self.runs.get_payload(h)

    # graph
    def list_nodes(self, kind=None, namespace=None, q=None, limit=500):
        return self.g.list_nodes(kind, namespace, q, limit)

    def get_node(self, node_id):
        return self.g.get_node(node_id)

    def node_versions(self, node_id):
        return self.g.node_versions(node_id)

    def subgraph(self, node_id, depth=3, direction="both") -> Subgraph:
        return self.g.subgraph(node_id, depth, direction)

    def graph(self, namespace=None, limit=2000) -> Subgraph:
        return self.g.graph(namespace, limit)

    # runs
    def list_runs(self, namespace=None, job_name=None, kind=None, agent_id=None, roots_only=False, limit=100):
        return self.runs.list_runs(namespace, job_name, kind, agent_id, roots_only, limit)

    def get_run(self, run_id):
        return self.runs.get_run(run_id)

    def run_tree(self, root_run_id):
        return self.runs.run_tree(root_run_id)

    def run_io(self, run_id=None, dataset_id=None, limit=200):
        return self.runs.run_io(run_id, dataset_id, limit)

    def raw_events(self, run_id):
        return self.runs.raw_events(run_id)

    def chain_events(self, chain_key):
        return self.runs.chain_events(chain_key)

    def chain_head(self, chain_key):
        return self.runs.chain_head(chain_key)

    def get_facet_named(self, h):
        return self.runs.get_facet_named(h)

    def tool_aggregates(self, parent_run_id=None):
        return self.runs.tool_aggregates(parent_run_id)

    def agent_catalog(self) -> list[dict[str, Any]]:
        return self.runs.agent_catalog()

    def stats(self) -> dict[str, Any]:
        return {**self.runs.stats(), **self.g.counts()}


def build_prod_store_from_env() -> CompositeStore:
    ch = ConnectClient(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    store = CompositeStore(
        ClickHouseRunStore(ch, os.getenv("CLICKHOUSE_DB", "authority"), int(os.getenv("AUTHORITY_RUN_TTL_DAYS", "30"))),
        PostgresGraphStore(os.getenv("POSTGRES_DSN", "postgresql://authority:authority@localhost:5432/authority")),
    )
    store.ensure_schema()
    return store
