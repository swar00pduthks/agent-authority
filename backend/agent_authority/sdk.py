"""Agent SDK: emit OpenLineage events for agent sessions, agents, tools and LLM calls.

    from agent_authority.sdk import Authority, Dataset

    hub = Authority("http://localhost:8000", namespace="trading")
    with hub.session("desk-assistant", on_behalf_of="user:alice"):
        with hub.agent("research", owner="markets-ai", model="claude"):
            with hub.tool("get_prices", inputs=[Dataset("market", "eod_prices")]):
                ...

Every context manager emits START on enter and COMPLETE (or FAIL with the
exception message) on exit. Parent/child linkage uses the standard OpenLineage
``ParentRunFacet`` (including ``root``), so any OpenLineage consumer can read
the events; Agent Authority additionally understands the ``agent`` run facet.
"""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

PRODUCER = "https://github.com/agent-authority/agent-authority/sdk/python"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
AGENT_FACET_SCHEMA = "https://github.com/agent-authority/agent-authority/spec/AgentRunFacet.json"


@dataclass
class Dataset:
    namespace: str
    name: str
    tags: list[str] = field(default_factory=list)
    schema: list[tuple[str, str]] | None = None
    row_count: int | None = None

    def to_ol(self, output: bool = False) -> dict[str, Any]:
        facets: dict[str, Any] = {}
        if self.schema:
            facets["schema"] = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                "fields": [{"name": n, "type": t} for n, t in self.schema],
            }
        if self.tags:
            facets["tags"] = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json",
                "tags": [{"key": t, "value": "true", "source": "SDK"} for t in self.tags],
            }
        d: dict[str, Any] = {"namespace": self.namespace, "name": self.name, "facets": facets}
        if output and self.row_count is not None:
            d["outputFacets"] = {"outputStatistics": {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
                "rowCount": self.row_count,
            }}
        return d


# ------------------------------------------------------------------ transports
class Transport:
    def emit(self, event: dict[str, Any]) -> None:
        raise NotImplementedError


class HttpTransport(Transport):
    """POSTs to any OpenLineage HTTP endpoint (Authority, Marquez, ...)."""

    def __init__(self, url: str, api_key: str | None = None, timeout: float = 5.0) -> None:
        self.url = url.rstrip("/") + "/api/v1/lineage"
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
        self.timeout = timeout

    def emit(self, event: dict[str, Any]) -> None:
        req = urllib.request.Request(self.url, json.dumps(event).encode(), self.headers, method="POST")
        try:
            urllib.request.urlopen(req, timeout=self.timeout).close()
        except Exception as exc:  # lineage must never break the agent
            log.warning("authority: failed to emit event: %s", exc)


class CallbackTransport(Transport):
    def __init__(self, fn: Callable[[dict[str, Any]], None]) -> None:
        self.fn = fn

    def emit(self, event: dict[str, Any]) -> None:
        self.fn(event)


class ListTransport(Transport):
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def emit(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(event)


# ------------------------------------------------------------------ run handle
@dataclass
class _Frame:
    run_id: str
    namespace: str
    job: str
    root_run_id: str
    root_job: tuple[str, str]


Frame = _Frame  # public alias for integrations that manage run ids themselves

_current: contextvars.ContextVar[_Frame | None] = contextvars.ContextVar("authority_frame", default=None)


class RunHandle:
    """Yielded by the context managers; lets the caller attach outputs or a policy decision."""

    def __init__(self, hub: Authority, frame: _Frame, agent_facet: dict[str, Any]) -> None:
        self.hub = hub
        self.frame = frame
        self.agent_facet = agent_facet
        self.outputs: list[Dataset] = []
        self.inputs: list[Dataset] = []

    @property
    def run_id(self) -> str:
        return self.frame.run_id

    def allow(self, rule: str | None = None) -> None:
        self.agent_facet["policy"] = {"decision": "allow", **({"rule": rule} if rule else {})}

    def deny(self, rule: str | None = None) -> None:
        self.agent_facet["policy"] = {"decision": "deny", **({"rule": rule} if rule else {})}

    def wrote(self, *datasets: Dataset) -> None:
        self.outputs.extend(datasets)

    def read(self, *datasets: Dataset) -> None:
        self.inputs.extend(datasets)

    def set(self, **fields: Any) -> None:
        self.agent_facet.update(fields)


class PolicyDenied(PermissionError):
    pass


class Authority:
    def __init__(
        self,
        url: str | None = None,
        namespace: str = "agents",
        transport: Transport | None = None,
        api_key: str | None = None,
    ) -> None:
        if transport is None and url is None:
            raise ValueError("pass url= or transport=")
        self.transport = transport or HttpTransport(url, api_key)  # type: ignore[arg-type]
        self.namespace = namespace

    # -------------------------------------------------------------- public
    def session(self, name: str, on_behalf_of: str | None = None, **extra: Any):
        return self._run("session", name, {"onBehalfOf": on_behalf_of, **extra})

    def agent(self, name: str, agent_id: str | None = None, owner: str | None = None,
              model: str | None = None, on_behalf_of: str | None = None, **extra: Any):
        return self._run("agent", name, {
            "agentId": agent_id or name, "owner": owner, "model": model, "onBehalfOf": on_behalf_of, **extra,
        })

    def step(self, name: str, **extra: Any):
        return self._run("step", name, extra)

    def tool(self, name: str, side_effecting: bool = False, inputs: list[Dataset] | None = None,
             outputs: list[Dataset] | None = None, policy: tuple[str, str | None] | None = None, **extra: Any):
        facet: dict[str, Any] = {"tool": {"name": name, "sideEffecting": side_effecting}, **extra}
        if policy:
            facet["policy"] = {"decision": policy[0], **({"rule": policy[1]} if policy[1] else {})}
        return self._run("tool", name, facet, inputs or [], outputs or [])

    def llm(self, model: str, prompt: Any = None, completion: Any = None, latency_ms: float | None = None,
            name: str | None = None, **extra: Any) -> str:
        """Record a completed LLM call as a single START+COMPLETE pair."""
        with self._run("llm", name or f"llm.{model}", {
            "model": model, "prompt": prompt, "completion": completion, "latencyMs": latency_ms, **extra,
        }) as h:
            return h.run_id

    # ------------------------------------------------------------ internal
    @contextmanager
    def _run(self, kind: str, name: str, facet: dict[str, Any],
             inputs: list[Dataset] | None = None, outputs: list[Dataset] | None = None) -> Iterator[RunHandle]:
        parent = _current.get()
        run_id = str(uuid.uuid4())
        root_id = parent.root_run_id if parent else run_id
        root_job = parent.root_job if parent else (self.namespace, name)
        frame = _Frame(run_id, self.namespace, name, root_id, root_job)
        agent_facet = {k: v for k, v in {"kind": kind, **facet}.items() if v is not None}
        handle = RunHandle(self, frame, agent_facet)
        handle.inputs.extend(inputs or [])
        handle.outputs.extend(outputs or [])
        token = _current.set(frame)
        self._emit("START", frame, parent, agent_facet, handle.inputs, [])
        try:
            yield handle
        except BaseException as exc:
            self._emit("FAIL", frame, parent, agent_facet, handle.inputs, handle.outputs,
                       error=f"{type(exc).__name__}: {exc}")
            raise
        else:
            self._emit("COMPLETE", frame, parent, agent_facet, handle.inputs, handle.outputs)
        finally:
            _current.reset(token)

    def emit(self, event_type: str, frame: _Frame, parent: _Frame | None, agent_facet: dict[str, Any],
             inputs: list[Dataset] | None = None, outputs: list[Dataset] | None = None, error: str | None = None) -> None:
        """Low-level emit for integrations (e.g. framework callbacks) that track run ids themselves."""
        self._emit(event_type, frame, parent, agent_facet, inputs or [], outputs or [], error)

    def _emit(self, event_type: str, frame: _Frame, parent: _Frame | None, agent_facet: dict[str, Any],
              inputs: list[Dataset], outputs: list[Dataset], error: str | None = None) -> None:
        run_facets: dict[str, Any] = {
            "agent": {"_producer": PRODUCER, "_schemaURL": AGENT_FACET_SCHEMA, **agent_facet},
        }
        if parent:
            run_facets["parent"] = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json",
                "run": {"runId": parent.run_id},
                "job": {"namespace": parent.namespace, "name": parent.job},
                "root": {
                    "run": {"runId": frame.root_run_id},
                    "job": {"namespace": frame.root_job[0], "name": frame.root_job[1]},
                },
            }
        if error:
            run_facets["errorMessage"] = {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
                "message": error, "programmingLanguage": "python",
            }
        event = {
            "eventType": event_type,
            "eventTime": datetime.now(UTC).isoformat(),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": frame.run_id, "facets": run_facets},
            "job": {"namespace": frame.namespace, "name": frame.job, "facets": {
                "jobType": {
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
                    "processingType": "STREAMING" if agent_facet["kind"] == "session" else "BATCH",
                    "integration": "AGENTHUB", "jobType": agent_facet["kind"].upper(),
                },
            }},
            "inputs": [d.to_ol() for d in inputs],
            "outputs": [d.to_ol(output=True) for d in outputs],
        }
        self.transport.emit(event)
