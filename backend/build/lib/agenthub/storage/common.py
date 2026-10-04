"""Store-agnostic helpers shared by the in-memory and production stores."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from agenthub.model import NodeKind, RunRecord, RunState, ToolAggregate

DENY = ("deny", "denied", "block", "blocked")


def build_agent_catalog(runs: dict[str, RunRecord], tool_aggs: Iterable[ToolAggregate]) -> list[dict[str, Any]]:
    """Derive the agent catalog from captured runs - no registry needed."""
    children: dict[str, list[str]] = defaultdict(list)
    for r in runs.values():
        if r.parent_run_id:
            children[r.parent_run_id].append(r.run_id)

    agents: dict[str, dict[str, Any]] = {}
    for r in runs.values():
        if r.job_kind != NodeKind.AGENT:
            continue
        aid = r.agent_id or f"{r.namespace}/{r.job_name}"
        a = agents.setdefault(aid, {
            "agent_id": aid, "name": r.job_name, "namespace": r.namespace, "owner": r.owner,
            "runs": 0, "failures": 0, "denials": 0, "tools": set(), "models": set(),
            "principals": set(), "last_seen": None,
        })
        a["runs"] += 1
        a["failures"] += int(r.state == RunState.FAIL)
        a["owner"] = a["owner"] or r.owner
        if r.model:
            a["models"].add(r.model)
        principal, p = r.on_behalf_of, r
        while principal is None and p is not None and p.parent_run_id:  # inherit from the delegation chain
            p = runs.get(p.parent_run_id)
            principal = p.on_behalf_of if p else None
        if principal:
            a["principals"].add(principal)
        t = r.last_event_time
        if t and (a["last_seen"] is None or t > a["last_seen"]):
            a["last_seen"] = t
        stack = list(children.get(r.run_id, []))
        while stack:
            c = runs.get(stack.pop())
            if c is None or c.job_kind == NodeKind.AGENT:
                continue  # sub-agents are catalogued under their own entry
            if c.tool_name:
                a["tools"].add(c.tool_name)
            if c.model:
                a["models"].add(c.model)
            if (c.policy_decision or "").lower() in DENY:
                a["denials"] += 1
            stack.extend(children.get(c.run_id, []))

    for agg in tool_aggs:
        p = runs.get(agg.parent_run_id) if agg.parent_run_id else None
        while p is not None and p.job_kind != NodeKind.AGENT and p.parent_run_id:
            p = runs.get(p.parent_run_id)
        if p is not None and p.job_kind == NodeKind.AGENT:
            aid = p.agent_id or f"{p.namespace}/{p.job_name}"
            if aid in agents:
                agents[aid]["tools"].add(agg.tool_name)

    out = []
    for a in agents.values():
        for k in ("tools", "models", "principals"):
            a[k] = sorted(a[k])
        out.append(a)
    return sorted(out, key=lambda a: a["runs"], reverse=True)
