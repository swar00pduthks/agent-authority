from __future__ import annotations

import dataclasses
import json

from agent_authority.collector.pipeline import Collector
from agent_authority.integrity import FileAnchorSink, MemoryAnchorSink, verify_chain
from agent_authority.sdk import Authority, CallbackTransport, Dataset
from agent_authority.storage.memory import MemoryStore


def build(anchors):
    store = MemoryStore()
    c = Collector(store, anchors=anchors)

    def emit(ev):
        c.process(ev)
        c.flush()

    aa = Authority(transport=CallbackTransport(emit), namespace="appbuilder/demo")
    with aa.session("build: demo", on_behalf_of="user:alice") as s:
        with aa.agent("coder", owner="platform", prompt={"messages": ["build me a dashboard"]}):
            with aa.tool("open_pull_request", side_effecting=True, outputs=[Dataset("git://gh/acme", "demo")]) as t:
                t.allow("human-review-required")
    return store, s.run_id


def test_untouched_record_is_intact():
    anchors = MemoryAnchorSink()
    store, sid = build(anchors)
    rep = verify_chain(store, sid, anchors)
    assert rep.status == "intact", rep.problems
    assert rep.events == 6 and rep.head_seq == 6 and rep.anchored_seq == 6


def test_edited_event_is_detected():
    anchors = MemoryAnchorSink()
    store, sid = build(anchors)
    ev = store.chains[sid][2]
    store.chains[sid][2] = dataclasses.replace(ev, payload=ev.payload.replace('"START"', '"COMPLETE"'))
    rep = verify_chain(store, sid, anchors)
    assert rep.status == "broken"
    assert any("does not match its hash" in p for p in rep.problems)


def test_rewritten_chain_is_caught_by_the_anchor():
    """An attacker who recomputes every hash still can't match the externally anchored head."""
    from agent_authority.integrity import GENESIS, link_hash

    anchors = MemoryAnchorSink()
    store, sid = build(anchors)
    prev, rebuilt = GENESIS, []
    for ev in store.chains[sid]:
        payload = ev.payload.replace("user:alice", "user:mallory")
        h = link_hash(prev, ev.seq, payload)
        rebuilt.append(dataclasses.replace(ev, payload=payload, prev_hash=prev, hash=h))
        prev = h
    store.chains[sid] = rebuilt
    rep = verify_chain(store, sid, anchors)
    assert rep.status == "broken" and any("rewritten" in p for p in rep.problems)


def test_truncation_and_blob_tampering_are_detected():
    anchors = MemoryAnchorSink()
    store, sid = build(anchors)
    store.chains[sid] = store.chains[sid][:-1]
    assert any("truncated" in p for p in verify_chain(store, sid, anchors).problems)

    store, sid = build(anchors := MemoryAnchorSink())
    h = next(h for h, n in store.facet_names.items() if n == "jobType")
    store.facets[h] = json.dumps({"tampered": True})
    assert any("altered" in p for p in verify_chain(store, sid, anchors).problems)

    store, sid = build(anchors := MemoryAnchorSink())
    ph = next(iter(store.payloads))
    store.payloads[ph] = json.dumps({"messages": ["something else"]})
    assert any("payload" in p for p in verify_chain(store, sid, anchors).problems)


def test_without_anchor_the_record_is_reported_unanchored(tmp_path):
    store, sid = build(None)
    assert verify_chain(store, sid, None).status == "unanchored"
    sink = FileAnchorSink(tmp_path)
    store, sid = build(sink)
    assert verify_chain(store, sid, FileAnchorSink(tmp_path)).status == "intact"  # reread from disk
    assert list(tmp_path.glob("anchors-*.jsonl"))


def test_integrity_endpoint():
    from fastapi.testclient import TestClient

    from agent_authority.api.main import create_app
    from agent_authority.demo import DemoTraffic

    app = create_app(MemoryStore())
    DemoTraffic(app.state.ingest).seed(10)
    c = TestClient(app)
    sid = c.get("/api/authority/sessions").json()[0]["run_id"]
    rep = c.get(f"/api/authority/sessions/{sid}/integrity").json()
    assert rep["status"] == "intact" and rep["events"] >= 1
