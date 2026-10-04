from __future__ import annotations

import json

from agent_authority.collector.facets import dehydrate, facet_hash, rehydrate
from agent_authority.collector.policy import AgentFacetView, CapturePolicy
from agent_authority.model import EdgeType, RunState
from agent_authority.service import replay

from .conftest import new_id, spark_event


def test_dehydrate_rehydrate_is_lossless():
    ev = spark_event(new_id(), "COMPLETE")
    dh = dehydrate(ev)
    blobs = {h: body for h, (_, body) in dh.refs.items()}
    # static facets replaced by refs
    assert set(dh.event["job"]["facets"]["sql"]) == {"$ref"}
    assert set(dh.event["run"]["facets"]["spark.logicalPlan"]) == {"$ref"}
    # dynamic facets untouched
    assert "nominalStartTime" in dh.event["run"]["facets"]["nominalTime"]
    assert "rowCount" in dh.event["outputs"][0]["outputFacets"]["outputStatistics"]
    assert rehydrate(dh.event, blobs.get) == ev


def test_facet_hash_is_order_independent():
    assert facet_hash("schema", {"a": 1, "b": [1, 2]}) == facet_hash("schema", {"b": [1, 2], "a": 1})
    assert facet_hash("schema", {"a": 1}) != facet_hash("sql", {"a": 1})


def test_run_lifecycle_merges_into_one_row(collector, store):
    rid = new_id()
    collector.process(spark_event(rid, "START", minute=0))
    collector.process(spark_event(rid, "RUNNING", minute=1))
    collector.process(spark_event(rid, "COMPLETE", minute=5))
    collector.flush()
    assert len(store.runs) == 1
    run = store.get_run(rid)
    assert run.state == RunState.COMPLETE
    assert run.start_time.minute == 0 and run.end_time.minute == 5
    assert run.event_count == 3


def test_out_of_order_delivery_is_safe(collector, store):
    rid = new_id()
    collector.process(spark_event(rid, "COMPLETE", minute=5))
    collector.flush()
    collector.process(spark_event(rid, "START", minute=0))
    collector.flush()
    run = store.get_run(rid)
    assert run.state == RunState.COMPLETE and run.start_time.minute == 0 and run.end_time.minute == 5


def test_static_facets_stored_once_and_ratio_grows(collector, store):
    for i in range(50):
        rid = new_id()
        collector.process(spark_event(rid, "START", minute=i))
        collector.process(spark_event(rid, "COMPLETE", minute=i, rows=i))
    collector.flush()
    s = store.stats()
    assert s["events_in"] == 100
    assert s["unique_facets"] <= 8  # schema, dataSource, sql, jobType, plan, engine (+ per-dataset dupes)
    assert s["dedup_ratio"] > 5, s


def test_graph_edges_aggregate_and_versions_on_change(collector, store):
    for i in range(10):
        collector.process(spark_event(new_id(), "COMPLETE", minute=i))
    collector.flush()
    edges = {(e.src, e.dst, e.type): e for e in store.edges.values()}
    key = ("dataset:abfss://lake:raw.trades", "job:spark-prod:daily_positions", EdgeType.INPUT)
    assert edges[key].run_count == 10
    assert len(store.node_versions("dataset:abfss://lake:raw.trades")) == 1


def test_replay_returns_original_event(collector, store):
    rid = new_id()
    ev = spark_event(rid, "COMPLETE")
    collector.process(ev)
    collector.flush()
    [r] = replay(store, rid)
    assert r["event"] == ev
    assert r["bytes_stored"] < r["bytes_in"]


def test_sampling_is_deterministic_and_keeps_failures():
    p = CapturePolicy(sample={"llm": 0.5})
    llm = AgentFacetView(kind="llm")
    rid = new_id()
    d1 = p.decide(rid, RunState.START, llm, False, False)
    d2 = p.decide(rid, RunState.COMPLETE, llm, False, False)
    assert d1 == d2
    assert p.decide(rid, RunState.FAIL, llm, False, False)[0] == "keep"
    tool = AgentFacetView(kind="tool", tool_name="calc", side_effecting=False)
    assert p.decide(rid, RunState.COMPLETE, tool, False, False)[0] == "aggregate"
    assert p.decide(rid, RunState.COMPLETE, tool, True, False)[0] == "keep"


def test_payloads_are_offloaded(hub, store):
    with hub.session("s", on_behalf_of="user:alice"):
        with hub.agent("a", owner="team"):
            pass
    big = {"messages": ["hello " * 500]}
    from agent_authority.collector.policy import CapturePolicy as CP  # noqa: F401
    with hub.session("s2", on_behalf_of="user:bob"):
        with hub.agent("a2", owner="team", prompt=big):
            pass
    agent_runs = [r for r in store.runs.values() if r.job_name == "a2"]
    raw = store.raw_events(agent_runs[0].run_id)[0]
    facet = json.loads(raw.payload)["run"]["facets"]["agent"]
    assert "$blob" in facet["prompt"]
    assert store.get_payload(facet["prompt"]["$blob"])
