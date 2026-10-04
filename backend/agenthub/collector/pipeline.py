"""The collector: OpenLineage event in -> dehydrated, merged, graph-shaped records out.

Stateless apart from bounded write-suppression caches, so it scales
horizontally behind Kafka partitions. All run-lifecycle merging is done by the
store (ClickHouse AggregatingMergeTree / in-memory merge), never here.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any

from agenthub.collector.facets import FacetPolicy, canonical_json, dehydrate
from agenthub.collector.lru import LRUSet
from agenthub.collector.policy import AGGREGATE, DROP, AgentFacetView, CapturePolicy
from agenthub.model import (
    EdgeObs,
    EdgeType,
    FacetBlob,
    IngestStats,
    Node,
    NodeKind,
    NodeVersion,
    RawEvent,
    RunIO,
    RunRecord,
    RunState,
    ToolAggregate,
    dataset_node_id,
    identity_node_id,
    job_node_id,
)
from agenthub.storage.base import Batch, Store

log = logging.getLogger(__name__)

BLOB_KEY = "$blob"


class InvalidEvent(ValueError):
    pass


def parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC)
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


class _PathCache:
    """run_id -> (root_run_id, run_path) for recently seen runs, to build materialised paths."""

    def __init__(self, capacity: int = 100_000) -> None:
        self._d: OrderedDict[str, tuple[str, str]] = OrderedDict()
        self.capacity = capacity

    def get(self, run_id: str) -> tuple[str, str] | None:
        v = self._d.get(run_id)
        if v:
            self._d.move_to_end(run_id)
        return v

    def put(self, run_id: str, root: str, path: str) -> None:
        self._d[run_id] = (root, path)
        self._d.move_to_end(run_id)
        if len(self._d) > self.capacity:
            self._d.popitem(last=False)


class Collector:
    def __init__(
        self,
        store: Store,
        facet_policy: FacetPolicy | None = None,
        capture_policy: CapturePolicy | None = None,
        cache_size: int = 200_000,
    ) -> None:
        self.store = store
        self.facet_policy = facet_policy or FacetPolicy()
        self.capture = capture_policy or CapturePolicy()
        self.facet_cache = LRUSet(cache_size)
        self.struct_cache = LRUSet(cache_size)
        self.paths = _PathCache(cache_size)
        self.batch = Batch()

    # ------------------------------------------------------------------ api
    def process(self, event: dict[str, Any]) -> str:
        """Process one OpenLineage RunEvent into the current batch. Returns the capture decision."""
        try:
            return self._process(event)
        except (KeyError, TypeError, AttributeError) as exc:
            raise InvalidEvent(f"malformed OpenLineage event: {exc}") from exc

    def process_many(self, events: list[dict[str, Any]]) -> dict[str, int]:
        out: dict[str, int] = {}
        for ev in events:
            d = self.process(ev)
            out[d] = out.get(d, 0) + 1
        return out

    def flush(self) -> Batch:
        batch, self.batch = self.batch, Batch()
        if len(batch) or batch.stats.events_in:
            self.store.write_batch(batch)
        return batch

    # ------------------------------------------------------------ internals
    def _process(self, event: dict[str, Any]) -> str:
        raw_bytes = len(canonical_json(event).encode())
        st = IngestStats(events_in=1, bytes_in=raw_bytes)
        b = self.batch

        run_id = event["run"]["runId"]
        job = event["job"]
        ns, job_name = job["namespace"], job["name"]
        etime = parse_time(event.get("eventTime"))
        state = RunState.parse(event.get("eventType"))
        run_facets = (event.get("run") or {}).get("facets") or {}
        agent = AgentFacetView.from_facet(run_facets.get(self.capture.agent_facet_name))
        kind = agent.kind
        inputs, outputs = event.get("inputs") or [], event.get("outputs") or []

        # 1. offload large payloads from the agent facet
        work = copy.deepcopy(event)
        afacet = ((work.get("run") or {}).get("facets") or {}).get(self.capture.agent_facet_name)
        new_payload_bytes = 0
        if isinstance(afacet, dict):
            for fld in self.capture.offload_fields:
                if fld in afacet and not (isinstance(afacet[fld], dict) and BLOB_KEY in afacet[fld]):
                    body = canonical_json(afacet[fld])
                    h = hashlib.sha256(body.encode()).hexdigest()
                    if not self.facet_cache.seen(("payload", h)):
                        b.payloads[h] = body
                        new_payload_bytes += len(body.encode())
                    afacet[fld] = {BLOB_KEY: h, "bytes": len(body.encode())}
                    st.payloads_offloaded += 1

        # 2. dehydrate static facets
        dh = dehydrate(work, self.facet_policy)
        new_facet_bytes = 0
        for h, (name, body) in dh.refs.items():
            st.facet_refs += 1
            if not self.facet_cache.seen(("facet", h)):
                b.facets.append(FacetBlob(h, name, body, etime))
                st.facet_new += 1
                new_facet_bytes += len(body.encode())

        # 3. structure: always recorded, whatever the capture decision
        parent = run_facets.get("parent") or {}
        parent_run_id = (parent.get("run") or {}).get("runId")
        parent_job = parent.get("job") or {}
        root_run_id = ((parent.get("root") or {}).get("run") or {}).get("runId")
        n = 1 if (state.terminal or state == RunState.OTHER) else 0  # count each run once
        self._structure(b, ns, job_name, kind, agent, dh, inputs, outputs, parent_job, etime, n)

        # 4. capture decision
        decision, reason = self.capture.decide(run_id, state, agent, bool(inputs), bool(outputs))
        if decision == DROP:
            st.sampled_out += 1
            b.stats.add(st)
            return DROP
        if decision == AGGREGATE:
            if state.terminal:
                key = (parent_run_id, agent.tool_name or job_name)
                agg = b.tool_aggs.get(key)
                if agg is None:
                    agg = b.tool_aggs[key] = ToolAggregate(parent_run_id, key[1], 0, 0, 0.0, etime)
                agg.calls += 1
                agg.failures += int(state == RunState.FAIL)
                agg.total_ms += agent.latency_ms or 0.0
                agg.last_seen = max(agg.last_seen, etime)
            st.aggregated += 1
            b.stats.add(st)
            return AGGREGATE

        # 5. run row (partial; merged by the store)
        if parent_run_id:
            cached = self.paths.get(parent_run_id)
            if cached:
                root, ppath = cached
            else:  # parent not seen (yet): fall back to the ParentRunFacet root, if any
                root = root_run_id or parent_run_id
                ppath = parent_run_id if root == parent_run_id else f"{root}/{parent_run_id}"
            path = f"{ppath}/{run_id}"
        else:
            root, path = run_id, run_id
        self.paths.put(run_id, root, path)

        dyn = {k: v for k, v in (dh.event["run"].get("facets") or {}).items()
               if not (isinstance(v, dict) and set(v) == {"$ref"}) and k != "parent"}
        err = (run_facets.get("errorMessage") or {}).get("message")
        rec = RunRecord(
            run_id=run_id,
            namespace=ns,
            job_name=job_name,
            job_kind=kind,
            root_run_id=root,
            parent_run_id=parent_run_id,
            run_path=path,
            start_time=etime if state == RunState.START else None,
            end_time=etime if state.terminal else None,
            state=int(state),
            facet_refs=dh.run_refs | dh.job_refs,
            dyn_facets=dyn,
            last_event_time=etime,
            event_count=1,
            agent_id=agent.agent_id,
            owner=agent.owner,
            on_behalf_of=agent.on_behalf_of,
            tool_name=agent.tool_name,
            side_effecting=agent.side_effecting,
            policy_decision=agent.policy_decision,
            policy_rule=agent.policy_rule,
            model=agent.model,
            error_message=err,
        )
        if rec.start_time is None and state == RunState.OTHER:
            rec.start_time = etime
        b.runs.append(rec)

        for direction, items in (("in", inputs), ("out", outputs)):
            for ds in items:
                stats_facet = (ds.get("outputFacets") or {}).get("outputStatistics") or {}
                b.run_io.append(RunIO(
                    run_id=run_id,
                    dataset_id=dataset_node_id(ds["namespace"], ds["name"]),
                    direction=direction,
                    schema_hash=dh.dataset_schema_hash.get((ds["namespace"], ds["name"])),
                    row_count=stats_facet.get("rowCount"),
                    event_time=etime,
                ))

        payload = json.dumps(dh.event, separators=(",", ":"), ensure_ascii=False)
        b.raw.append(RawEvent(etime, state.name, run_id, ns, job_name, payload, raw_bytes))
        st.runs_kept_events += 1
        st.bytes_stored += len(payload.encode()) + new_facet_bytes + new_payload_bytes
        b.stats.add(st)
        return reason if reason in ("run.failed", "policy.denied") else "keep"

    def _structure(
        self,
        b: Batch,
        ns: str,
        job_name: str,
        kind: str,
        agent: AgentFacetView,
        dh: Any,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        parent_job: dict[str, Any],
        etime: datetime,
        n: int = 1,
    ) -> None:
        jid = job_node_id(ns, job_name)
        attrs: dict[str, Any] = {}
        for key in ("agent_id", "owner", "model", "tool_name", "side_effecting"):
            val = getattr(agent, key)
            if val is not None:
                attrs[key] = val
        jt = (dh.event["job"].get("facets") or {}).get("jobType")
        if jt and "$ref" in jt:
            attrs["job_type_ref"] = jt["$ref"]
        self._upsert_node(b, Node(jid, kind, ns, job_name, attrs))

        if dh.job_refs:
            jhash = hashlib.sha256("|".join(sorted(dh.job_refs)).encode()).hexdigest()
            if not self.struct_cache.seen(("ver", jid, jhash)):
                b.node_versions.append(NodeVersion(jid, jhash, etime))

        for direction, items in (("in", inputs), ("out", outputs)):
            for ds in items:
                did = dataset_node_id(ds["namespace"], ds["name"])
                tags = sorted({
                    str(t.get("key")) for t in ((ds.get("facets") or {}).get("tags") or {}).get("tags", [])
                    if isinstance(t, dict) and t.get("key")
                })
                self._upsert_node(
                    b, Node(did, NodeKind.DATASET, ds["namespace"], ds["name"], {"tags": tags} if tags else {})
                )
                sh = dh.dataset_schema_hash.get((ds["namespace"], ds["name"]))
                if sh and not self.struct_cache.seen(("ver", did, sh)):
                    b.node_versions.append(NodeVersion(did, sh, etime))
                if direction == "in":
                    b.add_edge(EdgeObs(did, jid, EdgeType.INPUT, etime, etime, n))
                else:
                    b.add_edge(EdgeObs(jid, did, EdgeType.OUTPUT, etime, etime, n))

        if parent_job.get("name"):
            pid = job_node_id(parent_job.get("namespace", ns), parent_job["name"])
            b.add_edge(EdgeObs(pid, jid, EdgeType.INVOKES, etime, etime, n))

        if agent.on_behalf_of and kind in (NodeKind.SESSION, NodeKind.AGENT):
            iid = identity_node_id(agent.on_behalf_of)
            self._upsert_node(b, Node(iid, NodeKind.IDENTITY, "identity", agent.on_behalf_of, {}))
            b.add_edge(EdgeObs(iid, jid, EdgeType.ON_BEHALF_OF, etime, etime, n))

    def _upsert_node(self, b: Batch, node: Node) -> None:
        key = ("node", node.id, node.kind, canonical_json(node.attrs))
        if not self.struct_cache.seen(key):
            b.nodes.append(node)
