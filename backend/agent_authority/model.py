"""Internal records produced by the collector.

OpenLineage is the wire format. Internally an event is split into:

* ``FacetBlob``   - static facets, stored once, addressed by content hash
* ``RunRecord``   - one merged row per run (START/RUNNING/COMPLETE collapse)
* ``RunIO``       - run <-> dataset reads/writes
* ``Node``/``EdgeObs``/``NodeVersion`` - the property graph
* ``RawEvent``    - the dehydrated, lossless audit record
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Any


class RunState(IntEnum):
    """Ordinal so that ``max()`` is a valid merge: a terminal state always wins."""

    OTHER = 0
    START = 1
    RUNNING = 2
    COMPLETE = 3
    ABORT = 4
    FAIL = 5

    @classmethod
    def parse(cls, value: str | None) -> RunState:
        return cls.__members__.get((value or "OTHER").upper(), cls.OTHER)

    @property
    def terminal(self) -> bool:
        return self >= RunState.COMPLETE


class NodeKind:
    DATASET = "dataset"
    JOB = "job"
    SESSION = "session"
    AGENT = "agent"
    STEP = "step"
    TOOL = "tool"
    LLM = "llm"
    IDENTITY = "identity"
    APPROVAL = "approval"


class EdgeType:
    INPUT = "INPUT"  # dataset -> job   (job read the dataset)
    OUTPUT = "OUTPUT"  # job -> dataset   (job wrote the dataset)
    INVOKES = "INVOKES"  # parent job -> child job (from ParentRunFacet)
    ON_BEHALF_OF = "ON_BEHALF_OF"  # identity -> agent/session job


def job_node_id(namespace: str, name: str) -> str:
    return f"job:{namespace}:{name}"


def dataset_node_id(namespace: str, name: str) -> str:
    return f"dataset:{namespace}:{name}"


def identity_node_id(principal: str) -> str:
    return f"identity:{principal}"


@dataclass
class FacetBlob:
    hash: str
    facet_name: str
    body: str  # canonical JSON
    first_seen: datetime


@dataclass
class RunRecord:
    run_id: str
    namespace: str
    job_name: str
    job_kind: str = NodeKind.JOB
    root_run_id: str | None = None
    parent_run_id: str | None = None
    run_path: str = ""
    start_time: datetime | None = None
    end_time: datetime | None = None
    state: int = RunState.OTHER
    facet_refs: set[str] = field(default_factory=set)
    dyn_facets: dict[str, Any] = field(default_factory=dict)
    last_event_time: datetime | None = None
    event_count: int = 0
    # promoted agent columns (from the agent run facet)
    agent_id: str | None = None
    owner: str | None = None
    on_behalf_of: str | None = None
    tool_name: str | None = None
    side_effecting: bool | None = None
    policy_decision: str | None = None
    policy_rule: str | None = None
    model: str | None = None
    error_message: str | None = None

    def merge(self, other: RunRecord) -> None:
        """Commutative/associative merge so out-of-order delivery is safe."""
        if other.start_time and (self.start_time is None or other.start_time < self.start_time):
            self.start_time = other.start_time
        if other.end_time and (self.end_time is None or other.end_time > self.end_time):
            self.end_time = other.end_time
        self.state = max(self.state, other.state)
        self.facet_refs |= other.facet_refs
        newer = other.last_event_time and (
            self.last_event_time is None or other.last_event_time >= self.last_event_time
        )
        if newer:
            self.dyn_facets.update(other.dyn_facets)
            self.last_event_time = other.last_event_time
        else:
            for k, v in other.dyn_facets.items():
                self.dyn_facets.setdefault(k, v)
        self.event_count += other.event_count
        for attr in (
            "root_run_id",
            "parent_run_id",
            "agent_id",
            "owner",
            "on_behalf_of",
            "tool_name",
            "side_effecting",
            "policy_decision",
            "policy_rule",
            "model",
            "error_message",
        ):
            if getattr(other, attr) is not None and (newer or getattr(self, attr) is None):
                setattr(self, attr, getattr(other, attr))
        if other.run_path and (not self.run_path or len(other.run_path) > len(self.run_path)):
            self.run_path = other.run_path
        if other.job_kind != NodeKind.JOB:
            self.job_kind = other.job_kind


@dataclass
class RunIO:
    run_id: str
    dataset_id: str
    direction: str  # "in" | "out"
    schema_hash: str | None
    row_count: int | None
    event_time: datetime


@dataclass
class Node:
    id: str
    kind: str
    namespace: str
    name: str
    attrs: dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeVersion:
    node_id: str
    hash: str
    valid_from: datetime


@dataclass
class EdgeObs:
    """Aggregated edge observation; many runs collapse into one row per batch."""

    src: str
    dst: str
    type: str
    first_seen: datetime
    last_seen: datetime
    run_count: int = 1

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.src, self.dst, self.type)

    def absorb(self, other: EdgeObs) -> None:
        self.first_seen = min(self.first_seen, other.first_seen)
        self.last_seen = max(self.last_seen, other.last_seen)
        self.run_count += other.run_count


@dataclass
class RawEvent:
    event_time: datetime
    event_type: str
    run_id: str
    namespace: str
    job_name: str
    payload: str  # dehydrated OpenLineage JSON
    bytes_in: int
    # tamper-evidence chain (see integrity.py)
    chain_key: str = ""
    seq: int = 0
    prev_hash: str = ""
    hash: str = ""


@dataclass
class ToolAggregate:
    """Read-only tool calls that the sampling policy aggregates instead of storing."""

    parent_run_id: str | None
    tool_name: str
    calls: int
    failures: int
    total_ms: float
    last_seen: datetime


@dataclass
class IngestStats:
    events_in: int = 0
    bytes_in: int = 0
    bytes_stored: int = 0
    facet_refs: int = 0
    facet_new: int = 0
    runs_kept_events: int = 0
    sampled_out: int = 0
    aggregated: int = 0
    payloads_offloaded: int = 0

    def add(self, other: IngestStats) -> None:
        for k, v in other.__dict__.items():
            setattr(self, k, getattr(self, k) + v)

    @property
    def dedup_ratio(self) -> float:
        return round(self.bytes_in / self.bytes_stored, 2) if self.bytes_stored else 0.0
