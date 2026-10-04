"""Sentinel - the "agent police".

Policy-as-code detectors that run over captured lineage and flag risky agent
behaviour. Detection is deterministic code (never an LLM), so it cannot be
prompt-injected. Each rule is a small function; add your own with
``@rule(...)``.

This MVP evaluates on demand over a recent window. The same rules are designed
to run as a streaming detector next to the collector (see docs/ROADMAP.md).
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from typing import Any

from agenthub.model import EdgeType, NodeKind, RunRecord
from agenthub.service import ALLOW, SENSITIVE_TAGS, ancestors, effective_principal, is_deny
from agenthub.storage.base import Store

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}


@dataclass
class Finding:
    rule: str
    severity: str
    title: str
    detail: str
    run_id: str | None = None
    root_run_id: str | None = None
    agent_id: str | None = None
    node_id: str | None = None
    time: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return hashlib.sha1(f"{self.rule}|{self.run_id}|{self.node_id}".encode()).hexdigest()[:12]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, **asdict(self)}


@dataclass
class Context:
    store: Store
    runs: list[RunRecord]
    params: dict[str, Any]
    _chains: dict[str, list[RunRecord]] = field(default_factory=dict)

    def chain(self, r: RunRecord) -> list[RunRecord]:
        if r.run_id not in self._chains:
            self._chains[r.run_id] = ancestors(self.store, r)
        return self._chains[r.run_id]


RuleFn = Callable[[Context], Iterable[Finding]]


@dataclass
class Rule:
    id: str
    severity: str
    description: str
    fn: RuleFn


RULES: dict[str, Rule] = {}


def rule(rule_id: str, severity: str, description: str) -> Callable[[RuleFn], RuleFn]:
    def deco(fn: RuleFn) -> RuleFn:
        RULES[rule_id] = Rule(rule_id, severity, description, fn)
        return fn
    return deco


def _t(r: RunRecord) -> str | None:
    t = r.last_event_time or r.start_time
    return t.isoformat() if t else None


def _nearest_agent(chain: list[RunRecord]) -> RunRecord | None:
    for r in reversed(chain):
        if r.job_kind == NodeKind.AGENT:
            return r
    return None


# --------------------------------------------------------------------- rules
@rule("unattributed-side-effect", "high",
      "A side-effecting tool call ran with no human principal anywhere in its delegation chain.")
def unattributed_side_effect(ctx: Context) -> Iterable[Finding]:
    for r in ctx.runs:
        if r.job_kind != NodeKind.TOOL or not r.side_effecting:
            continue
        chain = ctx.chain(r)
        if effective_principal(chain) is None:
            agent = _nearest_agent(chain)
            yield Finding(
                "unattributed-side-effect", "high",
                f"Unattributed side effect: {r.tool_name or r.job_name}",
                "Tool changed state but no onBehalfOf principal exists in the chain. "
                "Nobody can be held accountable for this action.",
                r.run_id, r.root_run_id, agent.agent_id if agent else r.agent_id, None, _t(r),
                {"chain": [c.job_name for c in chain]},
            )


@rule("deny-then-retry", "high",
      "An agent retried a tool after policy denied it - classic sign of a hijacked or looping agent.")
def deny_then_retry(ctx: Context) -> Iterable[Finding]:
    groups: dict[tuple[str | None, str], list[RunRecord]] = defaultdict(list)
    for r in ctx.runs:
        if r.job_kind == NodeKind.TOOL and r.tool_name:
            groups[(r.root_run_id, r.tool_name)].append(r)
    for (root, tool), rs in groups.items():
        rs.sort(key=lambda r: r.start_time or r.last_event_time)
        denied = [r for r in rs if is_deny(r.policy_decision)]
        if not denied:
            continue
        after = [r for r in rs if (r.start_time or r.last_event_time) > (denied[0].start_time or denied[0].last_event_time)]
        if after:
            agent = _nearest_agent(ctx.chain(after[-1]))
            escalated = any((x.policy_decision or "").lower() in ALLOW for x in after)
            yield Finding(
                "deny-then-retry", "critical" if escalated else "high",
                f"Retried after deny: {tool}" + (" (later allowed)" if escalated else ""),
                f"Tool '{tool}' was denied by policy and attempted {len(after)} more time(s) in the same session"
                + (", and a later attempt was allowed - possible policy bypass." if escalated else "."),
                after[-1].run_id, root, agent.agent_id if agent else None, None, _t(after[-1]),
                {"denied_run": denied[0].run_id, "attempts_after_deny": len(after)},
            )


@rule("deep-delegation", "medium", "Agent-to-agent delegation is deeper than the configured limit.")
def deep_delegation(ctx: Context) -> Iterable[Finding]:
    limit = int(ctx.params.get("max_delegation_depth", 3))
    for r in ctx.runs:
        if r.job_kind != NodeKind.AGENT:
            continue
        depth = sum(1 for c in ctx.chain(r) if c.job_kind == NodeKind.AGENT)
        if depth > limit:
            yield Finding(
                "deep-delegation", "medium", f"Delegation depth {depth} > {limit}: {r.job_name}",
                "Long agent-to-agent chains dilute accountability and amplify prompt-injection risk.",
                r.run_id, r.root_run_id, r.agent_id, None, _t(r),
                {"chain": [c.job_name for c in ctx.chain(r) if c.job_kind == NodeKind.AGENT]},
            )


@rule("sensitive-data-access", "medium",
      "An agent touched a dataset tagged sensitive (pii, confidential, mnpi ...) without an explicit allow.")
def sensitive_data_access(ctx: Context) -> Iterable[Finding]:
    run_ids = {r.run_id: r for r in ctx.runs if r.job_kind in (NodeKind.TOOL, NodeKind.AGENT, NodeKind.STEP)}
    if not run_ids:
        return
    for io in ctx.store.run_io(limit=20_000):
        r = run_ids.get(io.run_id)
        if not r:
            continue
        node = ctx.store.get_node(io.dataset_id)
        tags = {t.lower() for t in (node.attrs.get("tags", []) if node else [])}
        hit = tags & SENSITIVE_TAGS
        if hit and (r.policy_decision or "").lower() not in ALLOW:
            agent = _nearest_agent(ctx.chain(r))
            yield Finding(
                "sensitive-data-access", "medium",
                f"{'Wrote' if io.direction == 'out' else 'Read'} {', '.join(sorted(hit))} data: {node.name}",
                f"{r.tool_name or r.job_name} accessed a sensitive dataset without an explicit policy allow.",
                r.run_id, r.root_run_id, agent.agent_id if agent else None, io.dataset_id, _t(r),
                {"tags": sorted(tags), "direction": io.direction},
            )


@rule("agent-without-owner", "low", "An agent ran with no registered owner.")
def agent_without_owner(ctx: Context) -> Iterable[Finding]:
    seen = set()
    for r in ctx.runs:
        key = r.agent_id or r.job_name
        if r.job_kind == NodeKind.AGENT and not r.owner and key not in seen:
            seen.add(key)
            yield Finding(
                "agent-without-owner", "low", f"Ownerless agent: {r.job_name}",
                "Every agent should have an accountable owning team.",
                r.run_id, r.root_run_id, r.agent_id, None, _t(r),
            )


@rule("new-tool", "info", "An agent invoked a tool for the first time.")
def new_tool(ctx: Context) -> Iterable[Finding]:
    g = ctx.store.graph(limit=20_000)
    kinds = {n.id: n for n in g.nodes}
    for e in g.edges:
        src, dst = kinds.get(e.src), kinds.get(e.dst)
        if e.type == EdgeType.INVOKES and dst and dst.kind == NodeKind.TOOL and e.run_count == 1:
            yield Finding(
                "new-tool", "info", f"First use of tool {dst.name}",
                f"{src.name if src else e.src} invoked {dst.name} for the first time.",
                None, None, None, dst.id,
                e.first_seen.isoformat() if hasattr(e.first_seen, "isoformat") else e.first_seen,
            )


# --------------------------------------------------------------------- engine
class Sentinel:
    def __init__(self, store: Store, params: dict[str, Any] | None = None,
                 disabled: Iterable[str] = (), window: int = 5000) -> None:
        self.store = store
        self.params = params or {}
        self.disabled = set(disabled)
        self.window = window

    def rules(self) -> list[dict[str, Any]]:
        return [
            {"id": r.id, "severity": r.severity, "description": r.description, "enabled": r.id not in self.disabled}
            for r in RULES.values()
        ]

    def scan(self, severity: str | None = None) -> list[dict[str, Any]]:
        runs = self.store.list_runs(limit=self.window)
        ctx = Context(self.store, runs, self.params)
        findings: dict[str, Finding] = {}
        for r in RULES.values():
            if r.id in self.disabled:
                continue
            for f in r.fn(ctx):
                findings.setdefault(f.id, f)
        out = sorted(findings.values(), key=lambda f: (SEVERITY_ORDER.get(f.severity, 9), f.time or ""))
        if severity:
            cutoff = SEVERITY_ORDER[severity]
            out = [f for f in out if SEVERITY_ORDER.get(f.severity, 9) <= cutoff]
        return [f.to_dict() for f in out]
