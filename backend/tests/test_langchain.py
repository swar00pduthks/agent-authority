from __future__ import annotations

import pytest

pytest.importorskip("langchain_core")

from langchain_core.language_models.fake_chat_models import FakeListChatModel  # noqa: E402
from langchain_core.runnables import RunnableLambda  # noqa: E402
from langchain_core.tools import tool  # noqa: E402

from agent_authority.integrations.langchain import AuthorityCallbackHandler  # noqa: E402
from agent_authority.model import NodeKind  # noqa: E402
from agent_authority.service import authority_chain  # noqa: E402


@tool
def place_order(symbol: str) -> str:
    """Place an order."""
    return f"ordered {symbol}"


def test_langchain_run_tree_maps_to_agent_lineage(hub, store):
    llm = FakeListChatModel(responses=["buy ACME"])

    def researcher(x: dict) -> str:
        return llm.invoke(x["q"]).content

    def trader(text: str) -> str:
        return place_order.invoke({"symbol": text.split()[-1]})

    chain = RunnableLambda(researcher).with_config(run_name="researcher") | \
        RunnableLambda(trader).with_config(run_name="trader")
    handler = AuthorityCallbackHandler(hub, session_name="desk-graph", on_behalf_of="user:alice",
                                      agents={"researcher", "trader"}, owner="markets-ai",
                                      side_effecting_tools={"place_order"})
    assert chain.invoke({"q": "what to buy?"}, config={"callbacks": [handler]}) == "ordered ACME"

    kinds = {r.job_name: r.job_kind for r in store.runs.values()}
    assert kinds["desk-graph"] == NodeKind.SESSION
    assert kinds["researcher"] == NodeKind.AGENT and kinds["trader"] == NodeKind.AGENT
    assert kinds["place_order"] == NodeKind.TOOL
    order = next(r for r in store.runs.values() if r.job_name == "place_order")
    chain_ = authority_chain(store, order.run_id)
    assert chain_["hops"][0]["principal"] == "user:alice"
    assert [h["job"] for h in chain_["hops"]] == ["desk-graph", "trader", "place_order"]
