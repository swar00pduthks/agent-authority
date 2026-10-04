"""Governance queries built on any ``Store``: authority chains, blast radius, replay."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from agent_authority.collector.facets import rehydrate
from agent_authority.model import NodeKind, RunRecord, RunState
from agent_authority.storage.base import Store

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
    session = store.run_tree(chain[0].run_id) if run.job_kind == NodeKind.TOOL else []
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
                # A denial means the guardrail worked and the action did not happen: worth reviewing, but it is not
                # unaccountable. "fail" is reserved for actions nobody can answer for.
                "status": "warn" if is_deny(r.policy_decision) else "pass",
                "detail": ("blocked by policy" if is_deny(r.policy_decision) else f"{r.policy_decision}")
                + (f" ({r.policy_rule})" if r.policy_rule else ""),
            })
        if r.job_kind == NodeKind.TOOL and r.side_effecting:
            approved = (r.policy_decision or "").lower() in ALLOW
            checks.append({
                "check": "side-effect approval",
                "status": "pass" if approved else "warn",
                "detail": "side-effecting call explicitly approved" if approved
                else "side-effecting call without explicit approval",
            })
        if r.job_kind == NodeKind.TOOL and r.tool_name and not is_deny(r.policy_decision):
            t0 = r.start_time or r.last_event_time
            prior = [x for x in session if x.tool_name == r.tool_name and x.run_id != r.run_id
                     and is_deny(x.policy_decision) and (x.start_time or x.last_event_time) <= t0]
            if prior:
                checks.append({
                    "check": "prior denial",
                    "status": "warn",
                    "detail": f"{r.tool_name} was denied {len(prior)}x earlier in this session"
                              + (f" ({prior[0].policy_rule})" if prior[0].policy_rule else ""),
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
    touched_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for x in store.run_io(run_id=run_id):  # START and COMPLETE both report inputs: keep one row per dataset+direction
        cur = touched_by_key.setdefault((x.dataset_id, x.direction),
                                        {"dataset_id": x.dataset_id, "direction": x.direction, "row_count": None})
        cur["row_count"] = cur["row_count"] if x.row_count is None else x.row_count
    touched = list(touched_by_key.values())
    for t in touched:
        node = store.get_node(t["dataset_id"])
        t["tags"] = (node.attrs.get("tags") if node else None) or []
    if run.job_kind == NodeKind.TOOL and run.side_effecting:
        _close_with_approvals(store, run, session, touched, hops)
    verdict = "fail" if any(h["status"] == "fail" for h in hops) else (
        "warn" if any(h["status"] == "warn" for h in hops) else "pass")
    return {"run_id": run_id, "verdict": verdict, "agent_depth": agent_depth, "hops": hops, "datasets": touched}


APPROVED = {"approved", "merged"}
REJECTED = {"rejected", "denied"}


def _close_with_approvals(store: Store, run: RunRecord, session: list[RunRecord], touched: list[dict[str, Any]],
                          hops: list[dict[str, Any]]) -> None:
    """End the chain at the humans who approved (or blocked) what a side-effecting tool changed."""
    outs = {t["dataset_id"] for t in touched if t["direction"] == "out"}
    needs_review = "review" in (run.policy_rule or "").lower()
    found = []
    for a in sorted((x for x in session if x.job_kind == NodeKind.APPROVAL),
                    key=lambda x: x.end_time or x.last_event_time):
        a_outs = {x.dataset_id for x in store.run_io(run_id=a.run_id) if x.direction == "out"}
        if (a_outs & outs) or needs_review:  # same repo, or any approval in a session that required review
            found.append(a)
    if not found:
        if needs_review:
            hops[-1]["checks"].append({"check": "human approval", "status": "warn",
                                       "detail": "review required, but no approval has been recorded yet"})
            if hops[-1]["status"] == "pass":
                hops[-1]["status"] = "warn"
        return
    final = (found[-1].policy_decision or "").lower()
    for a in found:
        d = (a.policy_decision or "").lower()
        ref = (a.dyn_facets.get("agent") or {}).get("approval", {})
        where = " ".join(x for x in (ref.get("provider"), ref.get("reference")) if x)
        if d in APPROVED:
            status, detail = "pass", f"{d} by {a.on_behalf_of}" + (f" ({where})" if where else "")
        elif final in APPROVED:
            status, detail = "pass", f"{d.replace('_', ' ')} by {a.on_behalf_of}, resolved by a later approval"
        else:
            status = "fail" if d in REJECTED else "warn"
            detail = f"{d.replace('_', ' ')} by {a.on_behalf_of}" + (f" ({where})" if where else "")
        hops.append({
            "run_id": a.run_id, "job": a.job_name, "namespace": a.namespace, "kind": NodeKind.APPROVAL,
            "agent_id": None, "principal": a.on_behalf_of, "tool": None, "model": None,
            "state": RunState(a.state).name, "status": status,
            "checks": [{"check": "human approval", "status": status, "detail": detail}],
        })


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
