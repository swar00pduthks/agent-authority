"""Storage contracts.

The collector writes a ``Batch``; the API reads through ``Store``. Production
splits the store by volume profile (see docs/ARCHITECTURE.md):

* run store   -> ClickHouse   (huge, time-series, TTL'd)
* graph store -> Postgres     (small, current structure + version history)
* raw store   -> object store (dehydrated, immutable audit log)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from agent_authority.model import (
    EdgeObs,
    FacetBlob,
    IngestStats,
    Node,
    NodeVersion,
    RawEvent,
    RunIO,
    RunRecord,
    ToolAggregate,
)


@dataclass
class Batch:
    facets: list[FacetBlob] = field(default_factory=list)
    payloads: dict[str, str] = field(default_factory=dict)
    runs: list[RunRecord] = field(default_factory=list)
    run_io: list[RunIO] = field(default_factory=list)
    nodes: list[Node] = field(default_factory=list)
    node_versions: list[NodeVersion] = field(default_factory=list)
    edges: dict[tuple[str, str, str], EdgeObs] = field(default_factory=dict)
    raw: list[RawEvent] = field(default_factory=list)
    tool_aggs: dict[tuple[str | None, str], ToolAggregate] = field(default_factory=dict)
    stats: IngestStats = field(default_factory=IngestStats)

    def add_edge(self, e: EdgeObs) -> None:
        cur = self.edges.get(e.key)
        if cur:
            cur.absorb(e)
        else:
            self.edges[e.key] = e

    def __len__(self) -> int:
        return len(self.runs) + len(self.raw) + len(self.edges) + len(self.facets)


@dataclass
class GraphEdge:
    src: str
    dst: str
    type: str
    first_seen: Any
    last_seen: Any
    run_count: int


@dataclass
class Subgraph:
    nodes: list[Node]
    edges: list[GraphEdge]


class Store(ABC):
    # ---- write ---------------------------------------------------------
    @abstractmethod
    def write_batch(self, batch: Batch) -> None: ...

    # ---- facets / payloads ---------------------------------------------
    @abstractmethod
    def get_facet(self, h: str) -> str | None: ...

    @abstractmethod
    def get_payload(self, h: str) -> str | None: ...

    # ---- graph -----------------------------------------------------------
    @abstractmethod
    def list_nodes(
        self, kind: str | None = None, namespace: str | None = None, q: str | None = None, limit: int = 500
    ) -> list[Node]: ...

    @abstractmethod
    def get_node(self, node_id: str) -> Node | None: ...

    @abstractmethod
    def node_versions(self, node_id: str) -> list[NodeVersion]: ...

    @abstractmethod
    def subgraph(self, node_id: str, depth: int = 3, direction: str = "both") -> Subgraph:
        """BFS from node; direction is 'up' (towards sources), 'down' (towards sinks) or 'both'."""

    @abstractmethod
    def graph(self, namespace: str | None = None, limit: int = 2000) -> Subgraph: ...

    # ---- runs ------------------------------------------------------------
    @abstractmethod
    def list_runs(
        self,
        namespace: str | None = None,
        job_name: str | None = None,
        kind: str | None = None,
        agent_id: str | None = None,
        roots_only: bool = False,
        limit: int = 100,
    ) -> list[RunRecord]: ...

    @abstractmethod
    def get_run(self, run_id: str) -> RunRecord | None: ...

    @abstractmethod
    def run_tree(self, root_run_id: str) -> list[RunRecord]: ...

    @abstractmethod
    def run_io(self, run_id: str | None = None, dataset_id: str | None = None, limit: int = 200) -> list[RunIO]: ...

    @abstractmethod
    def raw_events(self, run_id: str) -> list[RawEvent]: ...

    @abstractmethod
    def chain_events(self, chain_key: str) -> list[RawEvent]:
        """All stored events of one tamper-evidence chain, in sequence order."""

    @abstractmethod
    def chain_head(self, chain_key: str) -> tuple[int, str] | None:
        """(seq, hash) of the chain's newest stored event, or None."""

    @abstractmethod
    def get_facet_named(self, h: str) -> tuple[str, str | None]:
        """(facet name, canonical body) for a facet hash; body None when missing."""

    @abstractmethod
    def tool_aggregates(self, parent_run_id: str | None = None) -> list[ToolAggregate]: ...

    @abstractmethod
    def agent_catalog(self) -> list[dict[str, Any]]:
        """One row per agent: agent_id, name, namespace, owner, runs, failures, denials, tools, models, last_seen."""

    # ---- stats -----------------------------------------------------------
    @abstractmethod
    def stats(self) -> dict[str, Any]: ...
