"""LangChain / LangGraph integration.

    from agent_authority.integrations.langchain import AuthorityCallbackHandler

    handler = AuthorityCallbackHandler(hub, session_name="research-graph", on_behalf_of="user:alice",
                                      agents={"researcher", "writer"})
    graph.invoke(inputs, config={"callbacks": [handler]})

Mapping (LangChain run tree -> OpenLineage runs with the ``agent`` facet):

* top-level chain/graph           -> ``session``
* chains whose name is in ``agents`` (e.g. LangGraph nodes) -> ``agent``
* other chains                    -> ``step``
* tools                           -> ``tool``  (side effects via ``side_effecting_tools``)
* LLM / chat model calls          -> ``llm``

LangChain's own run ids are reused, so lineage lines up with LangSmith/Langfuse traces.
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID

from agent_authority.sdk import Authority, Frame

try:
    from langchain_core.callbacks import BaseCallbackHandler
except ImportError as exc:  # pragma: no cover
    raise ImportError("pip install langchain-core to use the Agent Authority LangChain integration") from exc


class AuthorityCallbackHandler(BaseCallbackHandler):
    raise_error = False

    def __init__(self, hub: Authority, session_name: str | None = None, on_behalf_of: str | None = None,
                 agents: set[str] | None = None, owner: str | None = None,
                 side_effecting_tools: set[str] | None = None) -> None:
        self.hub = hub
        self.session_name = session_name
        self.on_behalf_of = on_behalf_of
        self.agents = agents or set()
        self.owner = owner
        self.side_effecting = side_effecting_tools or set()
        self.frames: dict[str, tuple[Frame, Frame | None, dict[str, Any], float]] = {}

    # ---------------------------------------------------------------- helpers
    def _start(self, run_id: UUID, parent_run_id: UUID | None, name: str, facet: dict[str, Any]) -> None:
        parent = self.frames.get(str(parent_run_id))[0] if parent_run_id and str(parent_run_id) in self.frames else None
        root = parent.root_run_id if parent else str(run_id)
        root_job = parent.root_job if parent else (self.hub.namespace, name)
        frame = Frame(str(run_id), self.hub.namespace, name, root, root_job)
        facet = {k: v for k, v in facet.items() if v is not None}
        self.frames[str(run_id)] = (frame, parent, facet, time.perf_counter())
        self.hub.emit("START", frame, parent, facet)

    def _end(self, run_id: UUID, error: BaseException | None = None, **extra: Any) -> None:
        entry = self.frames.pop(str(run_id), None)
        if not entry:
            return
        frame, parent, facet, t0 = entry
        facet = {**facet, **{k: v for k, v in extra.items() if v is not None},
                 "latencyMs": round((time.perf_counter() - t0) * 1000, 1)}
        self.hub.emit("FAIL" if error else "COMPLETE", frame, parent, facet,
                      error=f"{type(error).__name__}: {error}" if error else None)

    # ----------------------------------------------------------------- chains
    def on_chain_start(self, serialized: dict[str, Any] | None, inputs: Any, *, run_id: UUID,
                       parent_run_id: UUID | None = None, **kw: Any) -> None:
        name = kw.get("name") or (serialized or {}).get("name") or "chain"
        if parent_run_id is None:
            self._start(run_id, None, self.session_name or name, {"kind": "session", "onBehalfOf": self.on_behalf_of})
        elif name in self.agents:
            self._start(run_id, parent_run_id, name, {"kind": "agent", "agentId": name, "owner": self.owner})
        else:
            self._start(run_id, parent_run_id, name, {"kind": "step"})

    def on_chain_end(self, outputs: Any, *, run_id: UUID, **kw: Any) -> None:
        self._end(run_id)

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kw: Any) -> None:
        self._end(run_id, error)

    # ------------------------------------------------------------------ tools
    def on_tool_start(self, serialized: dict[str, Any] | None, input_str: str, *, run_id: UUID,
                      parent_run_id: UUID | None = None, **kw: Any) -> None:
        name = kw.get("name") or (serialized or {}).get("name") or "tool"
        self._start(run_id, parent_run_id, name, {
            "kind": "tool", "tool": {"name": name, "sideEffecting": name in self.side_effecting},
        })

    def on_tool_end(self, output: Any, *, run_id: UUID, **kw: Any) -> None:
        self._end(run_id)

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kw: Any) -> None:
        self._end(run_id, error)

    # ------------------------------------------------------------------- llms
    def _llm_start(self, serialized: dict[str, Any] | None, prompt: Any, run_id: UUID, parent_run_id: UUID | None,
                   kw: dict[str, Any]) -> None:
        params = kw.get("invocation_params") or {}
        model = params.get("model") or params.get("model_name") or (serialized or {}).get("name") or "llm"
        self._start(run_id, parent_run_id, f"llm.{model}", {"kind": "llm", "model": model, "prompt": prompt})

    def on_llm_start(self, serialized: dict[str, Any] | None, prompts: list[str], *, run_id: UUID,
                     parent_run_id: UUID | None = None, **kw: Any) -> None:
        self._llm_start(serialized, prompts, run_id, parent_run_id, kw)

    def on_chat_model_start(self, serialized: dict[str, Any] | None, messages: list[list[Any]], *, run_id: UUID,
                            parent_run_id: UUID | None = None, **kw: Any) -> None:
        msgs = [[getattr(m, "content", str(m)) for m in batch] for batch in messages]
        self._llm_start(serialized, msgs, run_id, parent_run_id, kw)

    def on_llm_end(self, response: Any, *, run_id: UUID, **kw: Any) -> None:
        try:
            completion = [[g.text for g in gens] for gens in response.generations]
        except AttributeError:
            completion = None
        self._end(run_id, completion=completion)

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kw: Any) -> None:
        self._end(run_id, error)
