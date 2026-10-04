"""In-memory store: zero-dependency mode for `authority demo`, tests and local dev."""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from typing import Any

from agent_authority.model import (
    EdgeObs,
    IngestStats,
    Node,
    NodeKind,
    NodeVersion,
    RawEvent,
    RunIO,
    RunRecord,
    ToolAggregate,
)
from agent_authority.storage.base import Batch, GraphEdge, Store, Subgraph
from agent_authority.storage.common import build_agent_catalog


class MemoryStore(Store):
    def __init__(self, max_raw: int = 200_000) -> None:
        self._lock = threading.RLock()
        self.facets: dict[str, str] = {}
        self.facet_names: dict[str, str] = {}
        self.payloads: dict[str, str] = {}
        self.runs: dict[str, RunRecord] = {}
        self.children: dict[str, list[str]] = defaultdict(list)
        self.io: deque[RunIO] = deque(maxlen=max_raw)
        self.nodes: dict[str, Node] = {}
        self.versions: dict[str, list[NodeVersion]] = defaultdict(list)
        self.edges: dict[tuple[str, str, str], EdgeObs] = {}
        self.out_adj: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        self.in_adj: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        self.raw: dict[str, list[RawEvent]] = defaultdict(list)
        self.tool_aggs: dict[tuple[str | None, str], ToolAggregate] = {}
        self.ingest = IngestStats()

    # ------------------------------------------------------------- write
    def write_batch(self, batch: Batch) -> None:
        with self._lock:
            for f in batch.facets:
                self.facets.setdefault(f.hash, f.body)
                self.facet_names.setdefault(f.hash, f.facet_name)
            self.payloads.update(batch.payloads)
            for n in batch.nodes:
                cur = self.nodes.get(n.id)
                if cur:
                    cur.attrs.update(n.attrs)
                    if n.kind != NodeKind.JOB:
                        cur.kind = n.kind
                else:
                    self.nodes[n.id] = Node(n.id, n.kind, n.namespace, n.name, dict(n.attrs))
            for v in batch.node_versions:
                if all(x.hash != v.hash for x in self.versions[v.node_id]):
                    self.versions[v.node_id].append(v)
            for key, e in batch.edges.items():
                cur = self.edges.get(key)
                if cur:
                    cur.absorb(e)
                else:
                    self.edges[key] = EdgeObs(e.src, e.dst, e.type, e.first_seen, e.last_seen, e.run_count)
                    self.out_adj[e.src].add(key)
                    self.in_adj[e.dst].add(key)
            for r in batch.runs:
                cur = self.runs.get(r.run_id)
                if cur:
                    cur.merge(r)
                else:
                    self.runs[r.run_id] = r
                    if r.parent_run_id:
                        self.children[r.parent_run_id].append(r.run_id)
            self.io.extend(batch.run_io)
            for ev in batch.raw:
                self.raw[ev.run_id].append(ev)
            for key, a in batch.tool_aggs.items():
                cur = self.tool_aggs.get(key)
                if cur:
                    cur.calls += a.calls
                    cur.failures += a.failures
                    cur.total_ms += a.total_ms
                    cur.last_seen = max(cur.last_seen, a.last_seen)
                else:
                    self.tool_aggs[key] = a
            self.ingest.add(batch.stats)

    # ------------------------------------------------------------- reads
    def get_facet(self, h: str) -> str | None:
        return self.facets.get(h)

    def get_payload(self, h: str) -> str | None:
        return self.payloads.get(h)

    def list_nodes(self, kind=None, namespace=None, q=None, limit=500) -> list[Node]:
        out = []
        for n in self.nodes.values():
            if kind and n.kind != kind:
                continue
            if namespace and n.namespace != namespace:
                continue
            if q and q.lower() not in n.id.lower():
                continue
            out.append(n)
            if len(out) >= limit:
                break
        return out

    def get_node(self, node_id: str) -> Node | None:
        return self.nodes.get(node_id)

    def node_versions(self, node_id: str) -> list[NodeVersion]:
        return sorted(self.versions.get(node_id, []), key=lambda v: v.valid_from)

    def _edge(self, key: tuple[str, str, str]) -> GraphEdge:
        e = self.edges[key]
        return GraphEdge(e.src, e.dst, e.type, e.first_seen, e.last_seen, e.run_count)

    def subgraph(self, node_id: str, depth: int = 3, direction: str = "both") -> Subgraph:
        with self._lock:
            if node_id not in self.nodes:
                return Subgraph([], [])
            seen = {node_id}
            edge_keys: set[tuple[str, str, str]] = set()
            frontier = [node_id]
            for _ in range(depth):
                nxt = []
                for nid in frontier:
                    if direction in ("down", "both"):
                        for k in self.out_adj.get(nid, ()):
                            edge_keys.add(k)
                            if k[1] not in seen:
                                seen.add(k[1])
                                nxt.append(k[1])
                    if direction in ("up", "both"):
                        for k in self.in_adj.get(nid, ()):
                            edge_keys.add(k)
                            if k[0] not in seen:
                                seen.add(k[0])
                                nxt.append(k[0])
                frontier = nxt
                if not frontier:
                    break
            return Subgraph([self.nodes[i] for i in seen if i in self.nodes], [self._edge(k) for k in edge_keys])

    def graph(self, namespace=None, limit=2000) -> Subgraph:
        with self._lock:
            nodes = [n for n in self.nodes.values() if not namespace or n.namespace == namespace][:limit]
            ids = {n.id for n in nodes}
            edges = [self._edge(k) for k in self.edges if k[0] in ids and k[1] in ids]
            return Subgraph(nodes, edges)

    def list_runs(self, namespace=None, job_name=None, kind=None, agent_id=None, roots_only=False, limit=100):
        with self._lock:
            rs = [
                r for r in self.runs.values()
                if (not namespace or r.namespace == namespace)
                and (not job_name or r.job_name == job_name)
                and (not kind or r.job_kind == kind)
                and (not agent_id or r.agent_id == agent_id)
                and (not roots_only or not r.parent_run_id)
            ]
        rs.sort(key=lambda r: r.last_event_time or r.start_time, reverse=True)
        return rs[:limit]

    def get_run(self, run_id: str) -> RunRecord | None:
        return self.runs.get(run_id)

    def run_tree(self, root_run_id: str) -> list[RunRecord]:
        with self._lock:
            return [r for r in self.runs.values() if r.root_run_id == root_run_id or r.run_id == root_run_id]

    def run_io(self, run_id=None, dataset_id=None, limit=200) -> list[RunIO]:
        with self._lock:
            out = [
                x for x in reversed(self.io)
                if (not run_id or x.run_id == run_id) and (not dataset_id or x.dataset_id == dataset_id)
            ]
        return out[:limit]

    def raw_events(self, run_id: str) -> list[RawEvent]:
        return list(self.raw.get(run_id, []))

    def tool_aggregates(self, parent_run_id=None) -> list[ToolAggregate]:
        return [a for k, a in self.tool_aggs.items() if parent_run_id is None or k[0] == parent_run_id]

    def agent_catalog(self) -> list[dict[str, Any]]:
        with self._lock:
            return build_agent_catalog(self.runs, self.tool_aggs.values())

    def stats(self) -> dict[str, Any]:
        with self._lock:
            s = self.ingest
            facet_bytes = sum(len(v.encode()) for v in self.facets.values())
            return {
                **s.__dict__,
                "dedup_ratio": s.dedup_ratio,
                "unique_facets": len(self.facets),
                "facet_bytes": facet_bytes,
                "runs": len(self.runs),
                "nodes": len(self.nodes),
                "edges": len(self.edges),
                "agent_runs": sum(1 for r in self.runs.values() if r.job_kind == NodeKind.AGENT),
                "datasets": sum(1 for n in self.nodes.values() if n.kind == NodeKind.DATASET),
                "jobs": sum(1 for n in self.nodes.values() if n.kind != NodeKind.DATASET),
            }
