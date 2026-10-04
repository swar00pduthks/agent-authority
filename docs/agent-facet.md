# The `agent` run facet

Agent Authority models agent work as ordinary OpenLineage runs. Hierarchy comes from the standard `ParentRunFacet`
(including `root`). What makes a run an *agent* run is one custom run facet, `agent`:

```json
{
  "run": {
    "runId": "0d8f...",
    "facets": {
      "parent": { "run": { "runId": "..." }, "job": { "namespace": "trading", "name": "research" },
                  "root": { "run": { "runId": "..." }, "job": { "namespace": "trading", "name": "desk-assistant" } } },
      "agent": {
        "_producer": "https://github.com/agent-authority/agent-authority/sdk/python",
        "_schemaURL": "https://github.com/agent-authority/agent-authority/spec/AgentRunFacet.json",
        "kind": "tool",
        "agentId": "research",
        "owner": "markets-ai",
        "onBehalfOf": "user:alice",
        "model": "claude-sonnet",
        "tool": { "name": "place_order", "sideEffecting": true },
        "policy": { "decision": "allow", "rule": "within-limits" },
        "latencyMs": 412.5,
        "prompt": { "$blob": "9f2c...", "bytes": 1840 }
      }
    }
  }
}
```

| Field | Type | Meaning |
|---|---|---|
| `kind` | `session` \| `agent` \| `step` \| `tool` \| `llm` | Role of this run in the agent tree |
| `agentId` | string | Stable agent identity (catalog key) |
| `owner` | string | Accountable owning team |
| `onBehalfOf` | string | Human principal, e.g. `user:alice`. Set on the session or agent; inherited down the chain |
| `model` | string | Model used by this agent or LLM call |
| `tool.name` / `tool.sideEffecting` | string / bool | Tool identity and whether it changes state outside the agent |
| `policy.decision` / `policy.rule` | `allow` \| `deny` \| ... / string | Outcome of a guardrail or approval check |
| `latencyMs` | number | Duration as measured by the emitter |
| `prompt`, `completion`, `messages` | any | Payloads. The collector moves them to blob storage as `{"$blob", "bytes"}` |

The JSON Schema is in [`spec/AgentRunFacet.json`](../spec/AgentRunFacet.json). The facet name is configurable
(`agent_facet_name` in the capture policy) so the hub can track the OpenLineage community's AgentRunFacet
proposal as it evolves without breaking existing emitters.

## Mapping tips

* **One session per user request.** Put `onBehalfOf` on the session. Background jobs with no human should still
  open a session, and Sentinel will flag any side effects they cause.
* **Data access is lineage.** Put datasets a tool reads or writes in `inputs` / `outputs` (with `tags` for
  sensitivity). This feeds both the graph and the sensitive-data rule.
* **Record denials as runs.** A blocked tool call is a `tool` run with `policy.decision = deny`. Never drop it.
