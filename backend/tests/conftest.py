from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from agent_authority.collector.pipeline import Collector
from agent_authority.sdk import Authority, CallbackTransport
from agent_authority.storage.memory import MemoryStore

T0 = datetime(2026, 10, 1, 9, 0, tzinfo=UTC)

SCHEMA = [{"name": f"col_{i}", "type": "string", "description": "x" * 40} for i in range(40)]
PLAN = {"plan": [{"class": "org.apache.spark.sql.catalyst.plans.logical.Project", "child": "y" * 3000}]}


def spark_event(run_id: str, event_type: str, minute: int = 0, job: str = "daily_positions",
                inputs=("raw.trades",), outputs=("curated.positions",), rows: int = 1000) -> dict:
    def ds(name: str, out: bool = False) -> dict:
        d = {"namespace": "abfss://lake", "name": name, "facets": {
            "schema": {"_producer": "p", "_schemaURL": "s", "fields": SCHEMA},
            "dataSource": {"_producer": "p", "_schemaURL": "s", "name": "abfss://lake", "uri": "abfss://lake"},
        }}
        if out:
            d["outputFacets"] = {"outputStatistics": {"_producer": "p", "_schemaURL": "s", "rowCount": rows}}
        return d

    return {
        "eventType": event_type,
        "eventTime": (T0 + timedelta(minutes=minute)).isoformat(),
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.30.0/integration/spark",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": run_id, "facets": {
            "spark.logicalPlan": {"_producer": "p", "_schemaURL": "s", **PLAN},
            "processing_engine": {"_producer": "p", "_schemaURL": "s", "name": "spark", "version": "3.5.1"},
            "nominalTime": {"_producer": "p", "_schemaURL": "s",
                            "nominalStartTime": (T0 + timedelta(minutes=minute)).isoformat()},
        }},
        "job": {"namespace": "spark-prod", "name": job, "facets": {
            "jobType": {"_producer": "p", "_schemaURL": "s", "processingType": "BATCH",
                        "integration": "SPARK", "jobType": "SQL_JOB"},
            "sql": {"_producer": "p", "_schemaURL": "s", "query": "SELECT * FROM trades " * 50},
        }},
        "inputs": [ds(n) for n in inputs],
        "outputs": [ds(n, True) for n in outputs],
    }


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()


@pytest.fixture
def collector(store: MemoryStore) -> Collector:
    return Collector(store)


@pytest.fixture
def hub(collector: Collector) -> Authority:
    def emit(ev: dict) -> None:
        collector.process(ev)
        collector.flush()

    return Authority(transport=CallbackTransport(emit), namespace="agents")


def new_id() -> str:
    return str(uuid.uuid4())
