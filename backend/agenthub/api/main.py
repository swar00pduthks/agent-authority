"""HTTP API.

* ``POST /api/v1/lineage`` - OpenLineage ingest, drop-in compatible with Marquez.
* ``/api/v1/...``         - Marquez-compatible read subset (namespaces, jobs, datasets, lineage, runs).
* ``/api/hub/...``        - AgentHub governance API used by the web UI.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from agenthub import __version__
from agenthub.collector.pipeline import Collector, InvalidEvent
from agenthub.collector.policy import CapturePolicy
from agenthub.model import NodeKind, RunRecord, RunState
from agenthub.sentinel import Sentinel
from agenthub.service import authority_chain, blast_radius, edge_dict, node_dict, replay
from agenthub.storage.base import Store, Subgraph

log = logging.getLogger("agenthub.api")
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


def run_dict(r: RunRecord) -> dict[str, Any]:
    dur = None
    if r.start_time and r.end_time:
        dur = int((r.end_time - r.start_time).total_seconds() * 1000)
    return {
        "run_id": r.run_id, "namespace": r.namespace, "job": r.job_name, "kind": r.job_kind,
        "root_run_id": r.root_run_id, "parent_run_id": r.parent_run_id, "run_path": r.run_path,
        "state": RunState(r.state).name,
        "start_time": r.start_time.isoformat() if r.start_time else None,
        "end_time": r.end_time.isoformat() if r.end_time else None,
        "last_event_time": r.last_event_time.isoformat() if r.last_event_time else None,
        "duration_ms": dur, "event_count": r.event_count, "facet_refs": sorted(r.facet_refs),
        "agent_id": r.agent_id, "owner": r.owner, "on_behalf_of": r.on_behalf_of, "tool": r.tool_name,
        "side_effecting": r.side_effecting, "policy_decision": r.policy_decision, "policy_rule": r.policy_rule,
        "model": r.model, "error": r.error_message,
    }


def _tree(runs: list[RunRecord], root: str) -> dict[str, Any] | None:
    by_id = {r.run_id: {**run_dict(r), "children": []} for r in runs}
    top = None
    for r in sorted(runs, key=lambda r: r.start_time or r.last_event_time):
        node = by_id[r.run_id]
        if r.run_id == root:
            top = node
        elif r.parent_run_id in by_id:
            by_id[r.parent_run_id]["children"].append(node)
    return top


def create_app(store: Store, collector: Collector | None = None, sentinel: Sentinel | None = None,
               api_key: str | None = None) -> FastAPI:
    collector = collector or Collector(store, capture_policy=CapturePolicy.load(os.getenv("AGENTHUB_POLICY")))
    sentinel = sentinel or Sentinel(store)
    lock = threading.Lock()
    app = FastAPI(title="AgentHub", version=__version__,
                  description="OpenLineage-native lineage and governance for data pipelines and AI agents")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    app.state.store, app.state.collector, app.state.sentinel = store, collector, sentinel

    def ingest_events(events: list[dict[str, Any]]) -> dict[str, int]:
        with lock:
            decisions = collector.process_many(events)
            collector.flush()
        return decisions

    app.state.ingest = ingest_events

    def check_key(authorization: str | None = Header(default=None)) -> None:
        if api_key and authorization != f"Bearer {api_key}":
            raise HTTPException(401, "invalid API key")

    # ------------------------------------------------------------- ingest
    @app.post("/api/v1/lineage", status_code=201, dependencies=[Depends(check_key)], tags=["openlineage"])
    def ingest(payload: dict[str, Any] | list[dict[str, Any]] = Body(...)) -> dict[str, Any]:
        events = payload if isinstance(payload, list) else [payload]
        try:
            decisions = ingest_events(events)
        except InvalidEvent as exc:
            raise HTTPException(422, str(exc)) from exc
        return {"accepted": len(events), "decisions": decisions}

    # --------------------------------------------------- marquez-compatible
    @app.get("/api/v1/namespaces", tags=["marquez-compatible"])
    def namespaces() -> dict[str, Any]:
        names = sorted({n.namespace for n in store.list_nodes(limit=100_000) if n.kind != NodeKind.IDENTITY})
        return {"namespaces": [{"name": n} for n in names]}

    @app.get("/api/v1/namespaces/{ns}/jobs", tags=["marquez-compatible"])
    def jobs(ns: str, limit: int = 100) -> dict[str, Any]:
        out = []
        for n in store.list_nodes(namespace=ns, limit=100_000):
            if n.kind in (NodeKind.DATASET, NodeKind.IDENTITY):
                continue
            latest = store.list_runs(namespace=ns, job_name=n.name, limit=1)
            out.append({
                "id": {"namespace": ns, "name": n.name}, "name": n.name, "namespace": ns, "type": n.kind.upper(),
                "latestRun": run_dict(latest[0]) if latest else None,
            })
        return {"totalCount": len(out), "jobs": out[:limit]}

    @app.get("/api/v1/namespaces/{ns}/datasets", tags=["marquez-compatible"])
    def datasets(ns: str, limit: int = 100) -> dict[str, Any]:
        ds = [n for n in store.list_nodes(kind=NodeKind.DATASET, namespace=ns, limit=100_000)]
        return {"totalCount": len(ds), "datasets": [
            {"id": {"namespace": ns, "name": n.name}, "name": n.name, "namespace": ns,
             "tags": n.attrs.get("tags", [])} for n in ds[:limit]
        ]}

    @app.get("/api/v1/namespaces/{ns}/jobs/{job}/runs", tags=["marquez-compatible"])
    def job_runs(ns: str, job: str, limit: int = 100) -> dict[str, Any]:
        return {"runs": [run_dict(r) for r in store.list_runs(namespace=ns, job_name=job, limit=limit)]}

    @app.get("/api/v1/lineage", tags=["marquez-compatible"])
    def lineage(nodeId: str, depth: int = 20) -> dict[str, Any]:  # noqa: N803 (Marquez parameter name)
        sg = store.subgraph(nodeId, depth=depth, direction="both")
        if not sg.nodes:
            raise HTTPException(404, f"node {nodeId} not found")
        ins: dict[str, list[dict[str, str]]] = {}
        outs: dict[str, list[dict[str, str]]] = {}
        for e in sg.edges:
            outs.setdefault(e.src, []).append({"origin": e.src, "destination": e.dst})
            ins.setdefault(e.dst, []).append({"origin": e.src, "destination": e.dst})
        return {"graph": [{
            "id": n.id, "type": "DATASET" if n.kind == NodeKind.DATASET else "JOB",
            "data": {"namespace": n.namespace, "name": n.name, "kind": n.kind, **n.attrs},
            "inEdges": ins.get(n.id, []), "outEdges": outs.get(n.id, []),
        } for n in sg.nodes]}

    # ---------------------------------------------------------- hub api
    @app.get("/api/hub/health", tags=["hub"])
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "store": type(store).__name__}

    @app.get("/api/hub/stats", tags=["hub"])
    def stats() -> dict[str, Any]:
        s = store.stats()
        s["cache_hit_rate"] = round(collector.facet_cache.hit_rate, 4)
        return s

    @app.get("/api/hub/agents", tags=["hub"])
    def agents() -> list[dict[str, Any]]:
        out = store.agent_catalog()
        for a in out:
            if a.get("last_seen") is not None and hasattr(a["last_seen"], "isoformat"):
                a["last_seen"] = a["last_seen"].isoformat()
        return out

    @app.get("/api/hub/runs", tags=["hub"])
    def runs(namespace: str | None = None, job: str | None = None, kind: str | None = None,
             agent_id: str | None = None, roots_only: bool = False, limit: int = Query(100, le=5000)):
        return [run_dict(r) for r in store.list_runs(namespace, job, kind, agent_id, roots_only, limit)]

    @app.get("/api/hub/sessions", tags=["hub"])
    def sessions(limit: int = Query(50, le=1000)) -> list[dict[str, Any]]:
        out = []
        for r in store.list_runs(kind=NodeKind.SESSION, limit=limit):
            tree = store.run_tree(r.run_id)
            d = run_dict(r)
            d["size"] = len(tree)
            d["agents"] = sorted({t.job_name for t in tree if t.job_kind == NodeKind.AGENT})
            d["tools"] = sum(1 for t in tree if t.job_kind == NodeKind.TOOL)
            d["failures"] = sum(1 for t in tree if t.state == RunState.FAIL)
            d["denials"] = sum(1 for t in tree if (t.policy_decision or "").lower() in ("deny", "denied", "block"))
            out.append(d)
        return out

    @app.get("/api/hub/sessions/{root_run_id}/tree", tags=["hub"])
    def session_tree(root_run_id: str) -> dict[str, Any]:
        tree = _tree(store.run_tree(root_run_id), root_run_id)
        if tree is None:
            raise HTTPException(404, "session not found")
        aggs = [{"parent_run_id": a.parent_run_id, "tool": a.tool_name, "calls": a.calls, "failures": a.failures,
                 "avg_ms": round(a.total_ms / a.calls, 1) if a.calls else None}
                for a in store.tool_aggregates()
                if a.parent_run_id and (r := store.get_run(a.parent_run_id)) and r.root_run_id == root_run_id]
        return {"tree": tree, "aggregated_tools": aggs}

    @app.get("/api/hub/runs/{run_id}", tags=["hub"])
    def get_run(run_id: str) -> dict[str, Any]:
        r = store.get_run(run_id)
        if r is None:
            raise HTTPException(404, "run not found")
        d = run_dict(r)
        d["dynamic_facets"] = r.dyn_facets
        d["io"] = [x.__dict__ | {"event_time": x.event_time.isoformat()} for x in store.run_io(run_id=run_id)]
        return d

    @app.get("/api/hub/runs/{run_id}/authority", tags=["hub"])
    def get_authority(run_id: str) -> dict[str, Any]:
        out = authority_chain(store, run_id)
        if out is None:
            raise HTTPException(404, "run not found")
        return out

    @app.get("/api/hub/runs/{run_id}/replay", tags=["hub"])
    def get_replay(run_id: str, payloads: bool = False) -> list[dict[str, Any]]:
        return replay(store, run_id, include_payloads=payloads)

    @app.get("/api/hub/facets/{h}", tags=["hub"])
    def get_facet(h: str) -> dict[str, Any]:
        body = store.get_facet(h)
        if body is None:
            raise HTTPException(404, "facet not found")
        return {"hash": h, "body": body}

    @app.get("/api/hub/graph", tags=["hub"])
    def graph(namespace: str | None = None, focus: str | None = None, depth: int = 3,
              direction: str = "both", limit: int = 2000) -> dict[str, Any]:
        if focus:
            sg = store.subgraph(focus, depth, direction)
        else:
            sg = store.graph(None, limit)
            if namespace:  # the namespace's nodes plus their direct neighbours, so the view stays connected
                core = {n.id for n in sg.nodes if n.namespace == namespace}
                keep = core | {e.dst for e in sg.edges if e.src in core} | {e.src for e in sg.edges if e.dst in core}
                sg = Subgraph([n for n in sg.nodes if n.id in keep],
                              [e for e in sg.edges if e.src in keep and e.dst in keep])
        return {"nodes": [node_dict(n) for n in sg.nodes], "edges": [edge_dict(e) for e in sg.edges]}

    @app.get("/api/hub/nodes", tags=["hub"])
    def nodes(kind: str | None = None, namespace: str | None = None, q: str | None = None, limit: int = 200):
        return [node_dict(n) for n in store.list_nodes(kind, namespace, q, limit)]

    @app.get("/api/hub/node", tags=["hub"])
    def node(id: str) -> dict[str, Any]:  # noqa: A002
        n = store.get_node(id)
        if n is None:
            raise HTTPException(404, "node not found")
        d = node_dict(n)
        d["versions"] = [{"hash": v.hash, "valid_from": v.valid_from.isoformat()} for v in store.node_versions(id)]
        if n.kind == NodeKind.DATASET:
            d["recent_io"] = [x.__dict__ | {"event_time": x.event_time.isoformat()}
                              for x in store.run_io(dataset_id=id, limit=50)]
        else:
            d["recent_runs"] = [run_dict(r) for r in store.list_runs(namespace=n.namespace, job_name=n.name, limit=25)]
        return d

    @app.get("/api/hub/blast-radius", tags=["hub"])
    def get_blast_radius(node: str, depth: int = 6) -> dict[str, Any]:
        out = blast_radius(store, node, depth)
        if out is None:
            raise HTTPException(404, "node not found")
        return out

    @app.get("/api/hub/findings", tags=["sentinel"])
    def findings(severity: str | None = None) -> list[dict[str, Any]]:
        return sentinel.scan(severity)

    @app.get("/api/hub/rules", tags=["sentinel"])
    def rules() -> list[dict[str, Any]]:
        return sentinel.rules()

    # ------------------------------------------------------------- web ui
    if STATIC_DIR.exists():
        app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str) -> FileResponse:
            f = STATIC_DIR / path
            return FileResponse(f if path and f.is_file() else STATIC_DIR / "index.html")

    return app


def build_store_from_env() -> Store:
    kind = os.getenv("AGENTHUB_STORE", "memory")
    if kind == "memory":
        from agenthub.storage.memory import MemoryStore
        return MemoryStore()
    if kind == "prod":
        from agenthub.storage.composite import build_prod_store_from_env
        return build_prod_store_from_env()
    raise ValueError(f"unknown AGENTHUB_STORE={kind}")


def run() -> None:
    import uvicorn

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app = create_app(build_store_from_env(), api_key=os.getenv("AGENTHUB_API_KEY"))
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
