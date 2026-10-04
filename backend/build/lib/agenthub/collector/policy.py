"""Capture policy for agent workloads.

Agent runs are far chattier than batch jobs. The policy decides, per event,
whether the run row is kept, aggregated, or sampled out. **Structure (nodes and
edges) is always recorded** regardless of the decision - only per-run detail is
reduced - so the lineage graph stays complete.

Sampling is deterministic on ``runId`` so START and COMPLETE of the same run get
the same decision.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from agenthub.model import NodeKind, RunState

KEEP = "keep"
AGGREGATE = "aggregate"
DROP = "drop"


@dataclass
class AgentFacetView:
    """Normalised view of the agent run facet (see docs/agent-facet.md)."""

    kind: str = NodeKind.JOB
    agent_id: str | None = None
    owner: str | None = None
    on_behalf_of: str | None = None
    tool_name: str | None = None
    side_effecting: bool | None = None
    policy_decision: str | None = None
    policy_rule: str | None = None
    model: str | None = None
    latency_ms: float | None = None

    @classmethod
    def from_facet(cls, facet: dict[str, Any] | None) -> AgentFacetView:
        if not facet:
            return cls()
        tool = facet.get("tool") or {}
        pol = facet.get("policy") or {}
        return cls(
            kind=facet.get("kind") or NodeKind.AGENT,
            agent_id=facet.get("agentId"),
            owner=facet.get("owner"),
            on_behalf_of=facet.get("onBehalfOf"),
            tool_name=tool.get("name"),
            side_effecting=tool.get("sideEffecting"),
            policy_decision=pol.get("decision"),
            policy_rule=pol.get("rule"),
            model=facet.get("model"),
            latency_ms=facet.get("latencyMs"),
        )


@dataclass
class CapturePolicy:
    agent_facet_name: str = "agent"
    sample: dict[str, float] = field(default_factory=lambda: {NodeKind.LLM: 0.05})
    aggregate_read_only_tools: bool = True
    offload_fields: list[str] = field(default_factory=lambda: ["prompt", "completion", "messages"])

    @classmethod
    def load(cls, path: str | Path | None) -> CapturePolicy:
        if not path:
            return cls()
        data = yaml.safe_load(Path(path).read_text()) or {}
        cap = data.get("agent_capture", {})
        return cls(
            agent_facet_name=data.get("agent_facet_name", "agent"),
            sample={k: float(v) for k, v in (cap.get("sample") or {NodeKind.LLM: 0.05}).items()},
            aggregate_read_only_tools=bool(cap.get("aggregate_read_only_tools", True)),
            offload_fields=list((data.get("payloads") or {}).get("offload_fields", cls().offload_fields)),
        )

    @staticmethod
    def _bucket(run_id: str) -> float:
        return int(hashlib.sha256(run_id.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF

    def decide(
        self,
        run_id: str,
        state: RunState,
        agent: AgentFacetView,
        has_inputs: bool,
        has_outputs: bool,
    ) -> tuple[str, str]:
        """Return (decision, reason)."""
        if state in (RunState.FAIL, RunState.ABORT):
            return KEEP, "run.failed"
        if agent.policy_decision and agent.policy_decision.lower() in ("deny", "denied", "block"):
            return KEEP, "policy.denied"
        if agent.kind == NodeKind.TOOL:
            if agent.side_effecting or has_outputs:
                return KEEP, "tool.side_effecting"
            if has_inputs:
                return KEEP, "data.read"
            if self.aggregate_read_only_tools:
                return AGGREGATE, "tool.read_only"
        rate = self.sample.get(agent.kind)
        if rate is not None and rate < 1.0:
            return (KEEP, "sampled.in") if self._bucket(run_id) < rate else (DROP, "sampled.out")
        return KEEP, "default"
