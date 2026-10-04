"""HTTP API.

* ``POST /api/v1/lineage`` - OpenLineage ingest, drop-in compatible with Marquez.
* ``/api/v1/...``         - Marquez-compatible read subset (namespaces, jobs, datasets, lineage, runs).
* ``/api/authority/...``        - Agent Authority governance API used by the web UI.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from agent_authority import __version__
from agent_authority.approvals import (
    ApprovalDecision,
    WebhookError,
    approval_events,
    parse_github,
    parse_gitlab,
    verify_github,
    verify_gitlab,
)
from agent_authority.auth import AuthConfig, Authenticator, AuthError
from agent_authority.collector.pipeline import Collector, InvalidEvent
from agent_authority.collector.policy import CapturePolicy
from agent_authority.integrity import AnchorSink, MemoryAnchorSink, anchor_sink_from_env, verify_chain
from agent_authority.model import NodeKind, RunRecord, RunState
from agent_authority.sentinel import Sentinel
from agent_authority.service import authority_chain, blast_radius, edge_dict, node_dict, replay
from agent_authority.storage.base import Store, Subgraph

log = logging.getLogger("agent_authority.api")
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
               api_key: str | None = None, auth: AuthConfig | None = None,
               anchors: AnchorSink | None = None) -> FastAPI:
    anchors = anchors if anchors is not None else (anchor_sink_from_env() or MemoryAnchorSink())
    collector = collector or Collector(store, capture_policy=CapturePolicy.load(os.getenv("AUTHORITY_POLICY")),
                                       anchors=anchors)
    sentinel = sentinel or Sentinel(store)
    lock = threading.Lock()
    app = FastAPI(title="Agent Authority", version=__version__,
                  description="OpenLineage-native lineage and governance for data pipelines and AI agents")
    auth = auth or AuthConfig()
    if api_key:
        auth.api_key = api_key
    authenticator = Authenticator(auth)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        path = request.url.path
        if request.method != "OPTIONS" and path.startswith("/api/"):
            try:
                request.state.principal = authenticator.authenticate(
                    path, request.method, request.headers.get("authorization"))
            except AuthError as exc:
                hdrs = {"WWW-Authenticate": 'Bearer error="invalid_token"'} if exc.status == 401 else None
                return JSONResponse({"detail": exc.message}, status_code=exc.status, headers=hdrs)
        return await call_next(request)

    # CORS is added last so it wraps the auth middleware and 401s still carry CORS headers.
    origins = [o.strip() for o in os.getenv("AUTHORITY_CORS_ORIGINS", "*").split(",") if o.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["*"], allow_headers=["*"])
    app.state.store, app.state.collector, app.state.sentinel = store, collector, sentinel

    def ingest_events(events: list[dict[str, Any]]) -> dict[str, int]:
        with lock:
            decisions = collector.process_many(events)
            collector.flush()
        return decisions

    app.state.ingest = ingest_events

    # ------------------------------------------------------------- ingest
    @app.post("/api/v1/lineage", status_code=201, tags=["openlineage"])
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
    @app.get("/api/authority/health", tags=["authority"])
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "store": type(store).__name__,
                "auth": "oidc" if auth.oidc_enabled else "open"}

    @app.get("/api/authority/me", tags=["authority"])
    def me(request: Request) -> dict[str, Any]:
        p = request.state.principal
        return {"subject": p.subject, "name": p.name, "roles": p.roles, "via": p.via}

    @app.get("/api/authority/stats", tags=["authority"])
    def stats() -> dict[str, Any]:
        s = store.stats()
        s["cache_hit_rate"] = round(collector.facet_cache.hit_rate, 4)
        return s

    @app.get("/api/authority/agents", tags=["authority"])
    def agents(namespace: str | None = None) -> list[dict[str, Any]]:
        out = [a for a in store.agent_catalog() if not namespace or a["namespace"] == namespace]
        for a in out:
            if a.get("last_seen") is not None and hasattr(a["last_seen"], "isoformat"):
                a["last_seen"] = a["last_seen"].isoformat()
        return out

    @app.get("/api/authority/runs", tags=["authority"])
    def runs(namespace: str | None = None, job: str | None = None, kind: str | None = None,
             agent_id: str | None = None, roots_only: bool = False, limit: int = Query(100, le=5000)):
        return [run_dict(r) for r in store.list_runs(namespace, job, kind, agent_id, roots_only, limit)]

    @app.get("/api/authority/sessions", tags=["authority"])
    def sessions(namespace: str | None = None, limit: int = Query(50, le=1000)) -> list[dict[str, Any]]:
        out = []
        for r in store.list_runs(namespace=namespace, kind=NodeKind.SESSION, limit=limit):
            tree = store.run_tree(r.run_id)
            d = run_dict(r)
            d["size"] = len(tree)
            d["agents"] = sorted({t.job_name for t in tree if t.job_kind == NodeKind.AGENT})
            d["tools"] = sum(1 for t in tree if t.job_kind == NodeKind.TOOL)
            d["failures"] = sum(1 for t in tree if t.state == RunState.FAIL)
            d["denials"] = sum(1 for t in tree if (t.policy_decision or "").lower() in ("deny", "denied", "block"))
            out.append(d)
        return out

    @app.get("/api/authority/sessions/{root_run_id}/tree", tags=["authority"])
    def session_tree(root_run_id: str) -> dict[str, Any]:
        tree = _tree(store.run_tree(root_run_id), root_run_id)
        if tree is None:
            raise HTTPException(404, "session not found")
        aggs = [{"parent_run_id": a.parent_run_id, "tool": a.tool_name, "calls": a.calls, "failures": a.failures,
                 "avg_ms": round(a.total_ms / a.calls, 1) if a.calls else None}
                for a in store.tool_aggregates()
                if a.parent_run_id and (r := store.get_run(a.parent_run_id)) and r.root_run_id == root_run_id]
        return {"tree": tree, "aggregated_tools": aggs}

    # ------------------------------------------------------- human approvals
    def record_approval(d: ApprovalDecision) -> dict[str, Any]:
        session = store.get_run(d.session_id)
        if session is None:
            raise HTTPException(404, f"session {d.session_id} not found")
        ingest_events(approval_events(session, d))
        return {"recorded": True, "session_id": d.session_id, "approver": d.approver, "decision": d.decision}

    @app.post("/api/authority/approvals", status_code=201, tags=["approvals"])
    def post_approval(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        """Record a human approval directly (for hosts that already receive the merge webhook)."""
        try:
            repo = body.get("repository")
            d = ApprovalDecision(body["session_id"], body["approver"], body.get("decision", "approved"),
                                 body.get("provider", "api"), body.get("url"), body.get("reference"),
                                 (repo["namespace"], repo["name"]) if repo else None)
        except (KeyError, TypeError) as exc:
            raise HTTPException(422, f"missing field: {exc}") from exc
        return record_approval(d)

    def webhook_secret(name: str) -> str | None:
        secret = os.getenv(name)
        if not secret and auth.oidc_enabled:
            raise HTTPException(403, f"set {name} to accept webhooks on a secured deployment")
        return secret

    @app.post("/api/authority/webhooks/github", tags=["approvals"])
    async def github_webhook(request: Request) -> dict[str, Any]:
        body = await request.body()
        try:
            verify_github(webhook_secret("AUTHORITY_GITHUB_WEBHOOK_SECRET"), body,
                          request.headers.get("x-hub-signature-256"))
        except WebhookError as exc:
            raise HTTPException(exc.status, exc.message) from exc
        d = parse_github(request.headers.get("x-github-event", ""), json.loads(body or b"{}"))
        return record_approval(d) if d else {"recorded": False, "reason": "not an approval or merge of a linked PR"}

    @app.post("/api/authority/webhooks/gitlab", tags=["approvals"])
    async def gitlab_webhook(request: Request) -> dict[str, Any]:
        try:
            verify_gitlab(webhook_secret("AUTHORITY_GITLAB_WEBHOOK_TOKEN"), request.headers.get("x-gitlab-token"))
        except WebhookError as exc:
            raise HTTPException(exc.status, exc.message) from exc
        d = parse_gitlab(await request.json())
        return record_approval(d) if d else {"recorded": False, "reason": "not an approval or merge of a linked MR"}

    @app.get("/api/authority/sessions/{root_run_id}/integrity", tags=["authority"])
    def session_integrity(root_run_id: str) -> dict[str, Any]:
        """Verify the session's hash chain, its blobs, and that every anchored head is still present."""
        return verify_chain(store, root_run_id, collector.anchors).to_dict()

    @app.get("/api/authority/runs/{run_id}", tags=["authority"])
    def get_run(run_id: str) -> dict[str, Any]:
        r = store.get_run(run_id)
        if r is None:
            raise HTTPException(404, "run not found")
        d = run_dict(r)
        d["dynamic_facets"] = r.dyn_facets
        d["io"] = [x.__dict__ | {"event_time": x.event_time.isoformat()} for x in store.run_io(run_id=run_id)]
        return d

    @app.get("/api/authority/runs/{run_id}/authority", tags=["authority"])
    def get_authority(run_id: str) -> dict[str, Any]:
        out = authority_chain(store, run_id)
        if out is None:
            raise HTTPException(404, "run not found")
        return out

    @app.get("/api/authority/runs/{run_id}/replay", tags=["authority"])
    def get_replay(run_id: str, payloads: bool = False) -> list[dict[str, Any]]:
        return replay(store, run_id, include_payloads=payloads)

    @app.get("/api/authority/facets/{h}", tags=["authority"])
    def get_facet(h: str) -> dict[str, Any]:
        body = store.get_facet(h)
        if body is None:
            raise HTTPException(404, "facet not found")
        return {"hash": h, "body": body}

    @app.get("/api/authority/graph", tags=["authority"])
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

    @app.get("/api/authority/nodes", tags=["authority"])
    def nodes(kind: str | None = None, namespace: str | None = None, q: str | None = None, limit: int = 200):
        return [node_dict(n) for n in store.list_nodes(kind, namespace, q, limit)]

    @app.get("/api/authority/node", tags=["authority"])
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

    @app.get("/api/authority/blast-radius", tags=["authority"])
    def get_blast_radius(node: str, depth: int = 6) -> dict[str, Any]:
        out = blast_radius(store, node, depth)
        if out is None:
            raise HTTPException(404, "node not found")
        return out

    @app.get("/api/authority/findings", tags=["sentinel"])
    def findings(severity: str | None = None, namespace: str | None = None,
                 session: str | None = None) -> list[dict[str, Any]]:
        if session:
            return sentinel.scan(severity, runs=store.run_tree(session))
        if namespace:
            runs = [r for s in store.list_runs(namespace=namespace, kind=NodeKind.SESSION, limit=500)
                    for r in store.run_tree(s.run_id)]
            return sentinel.scan(severity, runs=runs)
        return sentinel.scan(severity)

    @app.get("/api/authority/rules", tags=["sentinel"])
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
    kind = os.getenv("AUTHORITY_STORE", "memory")
    if kind == "memory":
        from agent_authority.storage.memory import MemoryStore
        return MemoryStore()
    if kind == "prod":
        from agent_authority.storage.composite import build_prod_store_from_env
        return build_prod_store_from_env()
    raise ValueError(f"unknown AUTHORITY_STORE={kind}")


def run() -> None:
    import uvicorn

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app = create_app(build_store_from_env(), auth=AuthConfig.from_env())
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
