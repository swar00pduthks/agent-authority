"""Synthetic but realistic traffic for `agenthub demo`.

A fictional bank, "Northwind Capital", runs Spark pipelines and a handful of
agent workflows. Some of them misbehave on purpose so Sentinel has something
to find: an unattended bot with side effects, a trader agent that retries after
a limit breach, a deep research swarm, and an ownerless scraper.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from agenthub.sdk import AgentHub, CallbackTransport, Dataset

Emit = Callable[[list[dict[str, Any]]], Any]
P = "https://github.com/OpenLineage/OpenLineage/tree/1.30.0/integration/spark"

# --------------------------------------------------------------- datasets
LAKE = "abfss://lake@northwind"
TRADES = Dataset(LAKE, "raw.trades")
POSITIONS = Dataset(LAKE, "curated.positions")
VAR = Dataset(LAKE, "risk.var_daily")
DASH = Dataset(LAKE, "reporting.risk_dashboard")
RAW_CLIENTS = Dataset(LAKE, "raw.clients", tags=["pii"])
CLIENTS = Dataset(LAKE, "curated.clients", tags=["pii", "gdpr"])
MARKET = Dataset("bloomberg://feeds", "eod_prices")
ORDERS = Dataset("oms://northwind", "orders")
LEDGER = Dataset("gl://northwind", "general_ledger", tags=["restricted"])
CRM_NOTES = Dataset("crm://northwind", "client_notes", tags=["pii"])
EXPORTS = Dataset("s3://shadow-exports", "client_dump.csv")

PIPELINES = [
    ("ingest_trades", [], [TRADES]),
    ("build_positions", [TRADES, MARKET], [POSITIONS]),
    ("compute_var", [POSITIONS, MARKET], [VAR]),
    ("publish_risk_dashboard", [VAR, POSITIONS], [DASH]),
    ("ingest_clients", [], [RAW_CLIENTS]),
    ("curate_clients", [RAW_CLIENTS], [CLIENTS]),
]


def _schema(n: int, prefix: str) -> list[dict[str, str]]:
    return [{"name": f"{prefix}_{i}", "type": random.choice(["string", "decimal(18,4)", "timestamp", "bigint"]),
             "description": f"{prefix} attribute {i} as delivered by the upstream system of record"}
            for i in range(n)]


_SCHEMAS: dict[str, list[dict[str, str]]] = {}


def _ds(d: Dataset, out: bool, rows: int) -> dict[str, Any]:
    fields = _SCHEMAS.setdefault(d.name, _schema(random.randint(25, 60), d.name.split(".")[-1]))
    facets: dict[str, Any] = {
        "schema": {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                   "fields": fields},
        "dataSource": {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json",
                       "name": d.namespace, "uri": d.namespace},
        "symlinks": {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json",
                     "identifiers": [{"namespace": "hive://metastore", "name": d.name, "type": "TABLE"}]},
    }
    if d.tags:
        facets["tags"] = {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json",
                          "tags": [{"key": t, "value": "true", "source": "CATALOG"} for t in d.tags]}
    out_d: dict[str, Any] = {"namespace": d.namespace, "name": d.name, "facets": facets}
    if out:
        out_d["outputFacets"] = {"outputStatistics": {
            "_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json",
            "rowCount": rows, "size": rows * 412}}
    return out_d


def spark_run(job: str, inputs: list[Dataset], outputs: list[Dataset], fail: bool = False) -> list[dict[str, Any]]:
    run_id = str(uuid.uuid4())
    rows = random.randint(50_000, 5_000_000)
    plan = {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LogicalPlan.json",
            "plan": [{"class": "org.apache.spark.sql.catalyst.plans.logical.Project",
                      "projectList": [f"col_{i}#{i}L" for i in range(120)], "child": [{"class": "Join"}]}]}
    job_facets = {
        "jobType": {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
                    "processingType": "BATCH", "integration": "SPARK", "jobType": "SQL_JOB"},
        "sql": {"_producer": P, "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json",
                "query": f"INSERT OVERWRITE {outputs[0].name if outputs else 'x'} SELECT "
                         + ", ".join(f"t.c{i}" for i in range(80)) + " FROM source t JOIN dim d ON t.k = d.k"},
        "sourceCodeLocation": {"_producer": P, "_schemaURL": "s", "type": "git",
                               "url": "https://git.northwind.example/data/pipelines", "path": f"jobs/{job}.py"},
    }
    events = []
    for et in ("START", "RUNNING", "FAIL" if fail else "COMPLETE"):
        run_facets: dict[str, Any] = {
            "spark.logicalPlan": plan,
            "spark_properties": {"_producer": P, "_schemaURL": "s",
                                 "properties": {"spark.master": "yarn", "spark.app.name": job}},
            "processing_engine": {"_producer": P, "_schemaURL": "s", "version": "3.5.1", "name": "spark",
                                  "openlineageAdapterVersion": "1.30.0"},
            "nominalTime": {"_producer": P, "_schemaURL": "s", "nominalStartTime": datetime.now(UTC).isoformat()},
        }
        if et == "FAIL":
            run_facets["errorMessage"] = {"_producer": P, "_schemaURL": "s", "programmingLanguage": "scala",
                                          "message": "org.apache.spark.SparkException: Job aborted due to stage failure"}
        events.append({
            "eventType": et, "eventTime": datetime.now(UTC).isoformat(), "producer": P,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {"runId": run_id, "facets": run_facets},
            "job": {"namespace": "spark-prod", "name": job, "facets": job_facets},
            "inputs": [_ds(d, False, rows) for d in inputs],
            "outputs": [_ds(d, True, rows) for d in outputs] if et != "START" else [],
        })
    return events


# ---------------------------------------------------------------- agents
RMS = ["user:priya.sharma", "user:tom.becker", "user:aisha.khan", "user:li.wei"]
TRADERS = ["user:marco.rossi", "user:sofia.lind"]


def _llm(hub: AgentHub, model: str = "claude-sonnet") -> None:
    hub.llm(model, prompt={"messages": [{"role": "user", "content": "lorem ipsum " * random.randint(20, 80)}]},
            completion="dolor sit amet " * random.randint(10, 60), latency_ms=random.uniform(300, 2500))


def client_service(hub: AgentHub) -> None:
    with hub.session("client-service-copilot", on_behalf_of=random.choice(RMS)):
        with hub.agent("triage", owner="wealth-ai-platform", model="claude-sonnet"):
            _llm(hub)
            with hub.tool("web_search"):
                pass
            with hub.agent("kyc-checker", owner="fincrime-ai", model="claude-haiku"):
                with hub.tool("lookup_client", inputs=[CLIENTS], policy=("allow", "rm-entitlement")):
                    pass
                with hub.tool("read_crm_notes", inputs=[CRM_NOTES]):
                    pass
                _llm(hub, "claude-haiku")
            with hub.agent("drafting", owner="wealth-ai-platform", model="claude-sonnet"):
                for _ in range(random.randint(1, 3)):
                    _llm(hub)
                with hub.tool("calculator"):
                    pass
                with hub.tool("send_email", side_effecting=True, policy=("allow", "human-approved")):
                    pass


def trade_assistant(hub: AgentHub) -> None:
    with hub.session("trade-assistant", on_behalf_of=random.choice(TRADERS)):
        with hub.agent("research", owner="markets-ai", model="claude-opus"):
            with hub.tool("get_var", inputs=[VAR]):
                pass
            with hub.tool("get_prices", inputs=[MARKET]):
                pass
            _llm(hub, "claude-opus")
        with hub.agent("execution", owner="markets-ai", model="claude-sonnet"):
            if random.random() < 0.35:
                with hub.tool("place_order", side_effecting=True, outputs=[ORDERS], policy=("deny", "limit-breach")):
                    pass
                with hub.tool("place_order", side_effecting=True, outputs=[ORDERS],
                              policy=random.choice([("deny", "limit-breach"), ("allow", "desk-head-override")])):
                    pass
            else:
                with hub.tool("place_order", side_effecting=True, outputs=[ORDERS], policy=("allow", "within-limits")):
                    pass


def recon_bot(hub: AgentHub) -> None:
    with hub.session("nightly-recon-bot"):  # no human principal: Sentinel should flag the side effect
        with hub.agent("recon", owner="finance-ops-ai", model="claude-haiku"):
            with hub.tool("read_ledger", inputs=[LEDGER]):
                pass
            _llm(hub, "claude-haiku")
            with hub.tool("post_adjustment", side_effecting=True, outputs=[LEDGER]):
                pass


def research_swarm(hub: AgentHub) -> None:
    with hub.session("research-swarm", on_behalf_of="user:li.wei"):
        with hub.agent("planner", owner="research-ai", model="claude-opus"):
            _llm(hub, "claude-opus")
            with hub.agent("researcher", owner="research-ai", model="claude-sonnet"):
                for _ in range(3):
                    with hub.tool("web_search"):
                        pass
                with hub.agent("summariser", owner="research-ai", model="claude-haiku"):
                    _llm(hub, "claude-haiku")
                    with hub.agent("fact-checker", owner="research-ai", model="claude-haiku"):
                        with hub.tool("get_prices", inputs=[MARKET]):
                            pass
                        _llm(hub, "claude-haiku")


def shadow_scraper(hub: AgentHub) -> None:
    with hub.session("adhoc-notebook", on_behalf_of="user:tom.becker"):
        with hub.agent("shadow-scraper", model="gpt-4o"):  # no owner
            with hub.tool("lookup_client", inputs=[CLIENTS]):
                pass
            with hub.tool("export_csv", side_effecting=True, inputs=[CLIENTS], outputs=[EXPORTS]):
                pass


def flaky_agent(hub: AgentHub) -> None:
    try:
        with hub.session("client-service-copilot", on_behalf_of=random.choice(RMS)):
            with hub.agent("triage", owner="wealth-ai-platform", model="claude-sonnet"):
                with hub.tool("lookup_client", inputs=[CLIENTS]):
                    raise TimeoutError("CRM API timed out after 30s")
    except TimeoutError:
        pass


SCENARIOS: list[tuple[Callable[[AgentHub], None], float]] = [
    (client_service, 0.38), (trade_assistant, 0.25), (research_swarm, 0.12),
    (recon_bot, 0.08), (flaky_agent, 0.1), (shadow_scraper, 0.07),
]


class DemoTraffic:
    def __init__(self, emit: Emit, rate: float = 1.0) -> None:
        self.emit = emit
        self.rate = rate
        self._stop = threading.Event()
        self.hub = AgentHub(transport=CallbackTransport(lambda ev: self.emit([ev])), namespace="agents")

    def tick(self) -> None:
        if random.random() < 0.45:
            job, ins, outs = random.choice(PIPELINES)
            self.emit(spark_run(job, ins, outs, fail=random.random() < 0.06))
        fn = random.choices([s[0] for s in SCENARIOS], [s[1] for s in SCENARIOS])[0]
        fn(self.hub)

    def seed(self, n: int = 60) -> None:
        for job, ins, outs in PIPELINES:  # every pipeline at least once
            self.emit(spark_run(job, ins, outs))
        for fn, _ in SCENARIOS:  # every scenario at least once
            fn(self.hub)
        for _ in range(n):
            self.tick()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.tick()
            time.sleep(max(0.05, random.expovariate(self.rate)))

    def start(self) -> threading.Thread:
        t = threading.Thread(target=self.run_forever, name="agenthub-demo", daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()
