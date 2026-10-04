"""Minimal agent instrumentation. Run `authority demo` first, then: python examples/quickstart_agent.py"""

from agent_authority.sdk import Authority, Dataset

hub = Authority(__import__("os").getenv("AUTHORITY_URL", "http://localhost:8000"), namespace="quickstart")
CLIENTS = Dataset("crm", "clients", tags=["pii"], schema=[("id", "bigint"), ("name", "string")])
ORDERS = Dataset("oms", "orders")

with hub.session("support-assistant", on_behalf_of="user:you"):
    with hub.agent("helper", owner="your-team", model="claude-sonnet"):
        with hub.tool("lookup_client", inputs=[CLIENTS]) as t:
            t.allow("support-entitlement")
        hub.llm("claude-sonnet", prompt="Summarise the client's open orders", completion="...", latency_ms=820)
        with hub.tool("create_ticket", side_effecting=True, outputs=[ORDERS]) as t:
            t.allow("human-approved")

print("Sent. Open http://localhost:8000/sessions to see the session and its chain of authority.")
