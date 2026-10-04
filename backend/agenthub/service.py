"""Governance queries built on any ``Store``: authority chains, blast radius, replay."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from agenthub.collector.facets import rehydrate
from agenthub.model import NodeKind, RunRecord, RunState
from agenthub.storage.base import Store

SENSITIVE_TAGS = {"pii", "confidential", "restricted", "mnpi", "gdpr"}
DENY = {"deny", "denied", "block", "blocked"}
ALLOW = {"allow", "allowed", "approved", "approve"}


def is_deny(decision: str | None) -> bool:
    return (decision or "").lower() in DENY


def ancestors(store: Store, run: RunRecord, limit: int = 64) -> list[RunRecord]:
    """Root-first list of runs from the session root down to (and including) ``run``."""
    chain = [run]
    seen = {run.run_id}
    cur = run
    while cur.parent_run_id and len(chain) < limit:
        p = store.get_run(cur.parent_run_id)
        if p is None or p.run_id in seen:
            break
        chain.append(p)
        seen.add(p.run_id)
        cur = p
    return list(reversed(chain))


def effective_principal(chain: list[RunRecord]) -> str | None:
    principal = None
    for r in chain:
        principal = r.on_behalf_of or principal
    return principal


def authority_chain(store: Store, run_id: str) -> dict[str, Any] | None:
    run = store.get_run(run_id)
    if run is None:
        return None
    chain = ancestors(store, run)
    hops = []
    principal = None
    agent_depth = 0
    for r in chain:
        principal = r.on_behalf_of or principal
        if r.job_kind == NodeKind.AGENT:
            agent_depth += 1
        checks = []
        checks.append({
            "check": "identity",
            "status": "pass" if principal else "fail",
            "detail": f"acting for {principal}" if principal else "no human principal in chain",
        })
        if r.job_kind == NodeKind.AGENT:
            checks.append({
                "check": "ownership",
                "status": "pass" if r.owner else "warn",
                "detail": f"owned by {r.owner}" if r.owner else "agent has no registered owner",
            })
        if r.policy_decision:
            checks.append({
                "check": "policy",
                "status": "fail" if is_deny(r.policy_decision) else "pass",
                "detail": f"{r.policy_decision}" + (f" ({r.policy_rule})" if r.policy_rule else ""),
            })
        if r.job_kind == NodeKind.TOOL and r.side_effecting:
            approved = (r.policy_decision or "").lower() in ALLOW
            checks.append({
                "check": "side-effect approval",
                "status": "pass" if approved else ("fail" if is_deny(r.policy_decision) else "warn"),
                "detail": "side-effecting call explicitly approved" if approved
                else "side-effecting call without explicit approval",
            })
        if r.state == RunState.FAIL:
            checks.append({"check": "outcome", "status": "warn", "detail": r.error_message or "run failed"})
        status = "fail" if any(c["status"] == "fail" for c in checks) else (
            "warn" if any(c["status"] == "warn" for c in checks) else "pass")
        hops.append({
            "run_id": r.run_id, "job": r.job_name, "namespace": r.namespace, "kind": r.job_kind,
            "agent_id": r.agent_id, "principal": principal, "tool": r.tool_name, "model": r.model,
            "state": RunState(r.state).name, "status": status, "checks": checks,
        })
    touched = [
        {"dataset_id": x.dataset_id, "direction": x.direction, "row_count": x.row_count}
        for x in store.run_io(run_id=run_id)
    ]
    for t in touched:
        node = store.get_node(t["dataset_id"])
        t["tags"] = (node.attrs.get("tags") if node else None) or []
    verdict = "fail" if any(h["status"] == "fail" for h in hops) else (
        "warn" if any(h["status"] == "warn" for h in hops) else "pass")
    return {"run_id": run_id, "verdict": verdict, "agent_depth": agent_depth, "hops": hops, "datasets": touched}


def blast_radius(store: Store, node_id: str, depth: int = 6) -> dict[str, Any] | None:
    root = store.get_node(node_id)
    if root is None:
        return None
    sg = store.subgraph(node_id, depth=depth, direction="down")
    affected = [n for n in sg.nodes if n.id != node_id]
    by_kind = Counter(n.kind for n in affected)
    sensitive = [
        n.id for n in affected
        if n.kind == NodeKind.DATASET and SENSITIVE_TAGS & {t.lower() for t in n.attrs.get("tags", [])}
    ]
    # hop distance for ranking
    dist = {node_id: 0}
    adj: dict[str, list[str]] = {}
    for e in sg.edges:
        adj.setdefault(e.src, []).append(e.dst)
    frontier = [node_id]
    while frontier:
        nxt = []
        for n in frontier:
            for m in adj.get(n, []):
                if m not in dist:
                    dist[m] = dist[n] + 1
                    nxt.append(m)
        frontier = nxt
    return {
        "node": _node(root),
        "affected": sorted(
            ({**_node(n), "hops": dist.get(n.id)} for n in affected), key=lambda x: (x["hops"] or 99, x["id"])
        ),
        "by_kind": dict(by_kind),
        "sensitive_datasets": sensitive,
        "edges": [_edge(e) for e in sg.edges],
    }


def replay(store: Store, run_id: str, include_payloads: bool = False) -> list[dict[str, Any]]:
    """Rehydrate the exact OpenLineage events stored for a run (auditor view)."""
    out = []
    for ev in store.raw_events(run_id):
        full = rehydrate(json.loads(ev.payload), store.get_facet)
        if include_payloads:
            af = ((full.get("run") or {}).get("facets") or {}).get("agent") or {}
            for k, v in list(af.items()):
                if isinstance(v, dict) and "$blob" in v:
                    body = store.get_payload(v["$blob"])
                    af[k] = json.loads(body) if body else v
        out.append({
            "event_type": ev.event_type,
            "event_time": ev.event_time.isoformat(),
            "bytes_in": ev.bytes_in,
            "bytes_stored": len(ev.payload.encode()),
            "event": full,
        })
    return out


def _node(n) -> dict[str, Any]:
    return {"id": n.id, "kind": n.kind, "namespace": n.namespace, "name": n.name, "attrs": n.attrs}


def _edge(e) -> dict[str, Any]:
    return {
        "src": e.src, "dst": e.dst, "type": e.type, "run_count": e.run_count,
        "first_seen": _iso(e.first_seen), "last_seen": _iso(e.last_seen),
    }


def _iso(v: Any) -> Any:
    return v.isoformat() if hasattr(v, "isoformat") else v


def node_dict(n) -> dict[str, Any]:
    return _node(n)


def edge_dict(e) -> dict[str, Any]:
    return _edge(e)
