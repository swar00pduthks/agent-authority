from __future__ import annotations

import pytest

from agent_authority.model import NodeKind, RunState
from agent_authority.sdk import Dataset, PolicyDenied
from agent_authority.sentinel import Sentinel
from agent_authority.service import authority_chain, blast_radius

CLIENTS = Dataset("crm", "clients", tags=["pii"], schema=[("id", "int"), ("name", "string")])
ORDERS = Dataset("oms", "orders", schema=[("id", "int")])


def test_sdk_builds_run_tree(hub, store):
    with hub.session("desk", on_behalf_of="user:alice") as s:
        with hub.agent("research", owner="markets-ai", model="claude") as a:
            with hub.tool("lookup_client", inputs=[CLIENTS]):
                pass
            hub.llm("claude", prompt="p", completion="c")
    tree = store.run_tree(s.run_id)
    kinds = {r.job_kind for r in tree}
    assert {NodeKind.SESSION, NodeKind.AGENT, NodeKind.TOOL} <= kinds
    agent = store.get_run(a.run_id)
    assert agent.parent_run_id == s.run_id and agent.root_run_id == s.run_id
    assert agent.state == RunState.COMPLETE


def test_failure_is_recorded(hub, store):
    with pytest.raises(RuntimeError):
        with hub.session("desk", on_behalf_of="user:alice"):
            with hub.tool("explode") as t:
                raise RuntimeError("boom")
    run = store.get_run(t.run_id)
    assert run.state == RunState.FAIL and "boom" in run.error_message


def test_authority_chain_flags_missing_principal(hub, store):
    with hub.session("batch-bot"):  # no human principal
        with hub.agent("ops", owner="ops-ai"):
            with hub.tool("place_order", side_effecting=True, outputs=[ORDERS]) as t:
                pass
    chain = authority_chain(store, t.run_id)
    assert chain["verdict"] == "fail"
    assert [h["kind"] for h in chain["hops"]] == ["session", "agent", "tool"]
    assert chain["datasets"][0]["dataset_id"] == "dataset:oms:orders"


def test_authority_chain_passes_when_approved(hub, store):
    with hub.session("desk", on_behalf_of="user:alice"):
        with hub.agent("ops", owner="ops-ai"):
            with hub.tool("place_order", side_effecting=True, outputs=[ORDERS], policy=("allow", "4-eyes")) as t:
                pass
    assert authority_chain(store, t.run_id)["verdict"] == "pass"


def test_blast_radius_reaches_downstream_and_sensitive(hub, store):
    with hub.session("desk", on_behalf_of="user:alice"):
        with hub.agent("enricher", owner="x"):
            with hub.tool("write_clients", side_effecting=True, outputs=[CLIENTS], policy=("allow", None)):
                pass
        with hub.agent("reporter", owner="y"):
            with hub.tool("read_clients", inputs=[CLIENTS]):
                pass
    br = blast_radius(store, "job:agents:enricher")
    ids = {a["id"] for a in br["affected"]}
    assert "dataset:crm:clients" in ids
    assert "job:agents:read_clients" in ids  # downstream reader of the written dataset
    assert br["sensitive_datasets"] == ["dataset:crm:clients"]


def test_sentinel_rules(hub, store):
    # 1. unattributed side effect
    with hub.session("cron-bot"):
        with hub.agent("ops", owner=None):
            with hub.tool("wire_transfer", side_effecting=True):
                pass
    # 2. deny then retry, later allowed
    with hub.session("desk", on_behalf_of="user:alice"):
        with hub.agent("trader", owner="markets"):
            with hub.tool("place_order", side_effecting=True, policy=("deny", "limit-breach")):
                pass
            with hub.tool("place_order", side_effecting=True, policy=("allow", None)):
                pass
    # 3. deep delegation
    with hub.session("desk", on_behalf_of="user:bob"):
        with hub.agent("a1", owner="o"):
            with hub.agent("a2", owner="o"):
                with hub.agent("a3", owner="o"):
                    with hub.agent("a4", owner="o"):
                        pass
    # 4. sensitive read
    with hub.session("desk", on_behalf_of="user:carol"):
        with hub.agent("kyc", owner="o"):
            with hub.tool("read_clients", inputs=[CLIENTS]):
                pass
    rules = {f["rule"]: f for f in Sentinel(store).scan()}
    assert "unattributed-side-effect" in rules
    assert rules["deny-then-retry"]["severity"] == "critical"
    assert "deep-delegation" in rules
    assert "sensitive-data-access" in rules
    assert "agent-without-owner" in rules
    assert "new-tool" in rules
    # severity ordering
    sev = [f["severity"] for f in Sentinel(store).scan()]
    assert sev[0] == "critical"


def test_policy_denied_exception_exists():
    assert issubclass(PolicyDenied, PermissionError)


def test_authority_warns_on_retry_after_denial(hub, store):
    with hub.session("desk", on_behalf_of="user:alice"), hub.agent("trader", owner="m"):
        with hub.tool("place_order", side_effecting=True, policy=("deny", "limit-breach")):
            pass
        with hub.tool("place_order", side_effecting=True, policy=("allow", "override")) as t:
            pass
    chain = authority_chain(store, t.run_id)
    assert chain["verdict"] == "warn"
    assert any(c["check"] == "prior denial" for c in chain["hops"][-1]["checks"])
