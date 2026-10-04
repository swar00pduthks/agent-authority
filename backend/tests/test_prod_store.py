"""Parity tests: the production store (ClickHouse + Postgres) must answer like the in-memory store.

ClickHouse runs embedded via chdb unless AUTHORITY_TEST_CH_HOST points at a server. Postgres needs a DSN:

    AUTHORITY_TEST_PG_DSN=postgresql://authority@localhost:5432/authority pytest tests/test_prod_store.py
"""

from __future__ import annotations

import os
import random
import uuid

import pytest

from agent_authority.collector.pipeline import Collector
from agent_authority.demo import DemoTraffic
from agent_authority.sentinel import Sentinel
from agent_authority.service import authority_chain, blast_radius, replay
from agent_authority.storage.memory import MemoryStore

DSN = os.getenv("AUTHORITY_TEST_PG_DSN")
chdb = pytest.importorskip("chdb")
pytestmark = pytest.mark.skipif(not DSN, reason="set AUTHORITY_TEST_PG_DSN to run Postgres-backed tests")


@pytest.fixture(scope="module")
def stores():
    from agent_authority.storage.clickhouse import ChdbClient, ClickHouseRunStore, ConnectClient
    from agent_authority.storage.composite import CompositeStore
    from agent_authority.storage.postgres import PostgresGraphStore

    db = f"aa_test_{uuid.uuid4().hex[:8]}"
    graph = PostgresGraphStore(DSN)
    with graph.pool.connection() as conn:
        conn.execute("DROP TABLE IF EXISTS aa_edges, aa_node_versions, aa_nodes CASCADE")
    ch_host = os.getenv("AUTHORITY_TEST_CH_HOST")  # real server (CI) or embedded chdb (local)
    client = ConnectClient(host=ch_host, username=os.getenv("AUTHORITY_TEST_CH_USER", "default"),
                           password=os.getenv("AUTHORITY_TEST_CH_PASSWORD", "")) if ch_host else ChdbClient()
    prod = CompositeStore(ClickHouseRunStore(client, db), graph)
    prod.ensure_schema()
    mem = MemoryStore()

    random.seed(7)
    events: list[dict] = []
    DemoTraffic(events.extend).seed(25)
    for store in (mem, prod):
        c = Collector(store)
        for i in range(0, len(events), 200):  # several batches, like Kafka polls
            c.process_many(events[i:i + 200])
            c.flush()
    yield mem, prod
    graph.pool.close()


def _ms(t):
    return t.replace(microsecond=t.microsecond // 1000 * 1000) if t else None


def test_counts_match(stores):
    mem, prod = stores
    m, p = mem.stats(), prod.stats()
    for k in ("events_in", "bytes_in", "bytes_stored", "runs", "unique_facets", "sampled_out", "aggregated", "nodes",
              "edges", "agent_runs"):
        assert m[k] == p[k], (k, m[k], p[k])


def test_runs_merge_identically(stores):
    mem, prod = stores
    for r in list(mem.runs.values())[:80]:
        pr = prod.get_run(r.run_id)
        assert pr is not None
        assert (pr.state, pr.event_count, pr.job_kind, pr.root_run_id, pr.parent_run_id) == \
               (r.state, r.event_count, r.job_kind, r.root_run_id, r.parent_run_id)
        assert pr.facet_refs == r.facet_refs
        assert _ms(pr.start_time) == _ms(r.start_time) and _ms(pr.end_time) == _ms(r.end_time)  # DateTime64(3)


def test_graph_and_governance_match(stores):
    mem, prod = stores
    assert {(e.src, e.dst, e.type, e.run_count) for e in mem.graph().edges} == \
           {(e.src, e.dst, e.type, e.run_count) for e in prod.graph().edges}
    node = "dataset:abfss://lake@northwind:raw.trades"
    assert {a["id"] for a in blast_radius(mem, node)["affected"]} == {a["id"] for a in blast_radius(prod, node)["affected"]}
    tool = next(r for r in mem.runs.values() if r.job_kind == "tool")
    assert authority_chain(mem, tool.run_id) == authority_chain(prod, tool.run_id)
    def findings(store):
        return {(f["id"], f["evidence"]["occurrences"]) for f in Sentinel(store).scan()}
    assert findings(mem) == findings(prod)


def test_replay_and_catalog_match(stores):
    mem, prod = stores
    rid = next(r.run_id for r in mem.runs.values() if r.job_kind == "job")
    assert [e["event"] for e in replay(mem, rid)] == [e["event"] for e in replay(prod, rid)]
    def norm(cat):
        return sorted((a["agent_id"], a["runs"], a["failures"], a["denials"], tuple(a["tools"])) for a in cat)
    assert norm(mem.agent_catalog()) == norm(prod.agent_catalog())
