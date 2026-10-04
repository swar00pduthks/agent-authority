from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from agent_authority.api.main import create_app
from agent_authority.demo import DemoTraffic
from agent_authority.storage.memory import MemoryStore

from .conftest import new_id, spark_event


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = create_app(MemoryStore())
    DemoTraffic(app.state.ingest).seed(30)
    return TestClient(app)


def test_ingest_single_and_batch(client):
    rid = new_id()
    r = client.post("/api/v1/lineage", json=spark_event(rid, "START"))
    assert r.status_code == 201 and r.json()["accepted"] == 1
    r = client.post("/api/v1/lineage", json=[spark_event(rid, "COMPLETE", minute=3)])
    assert r.status_code == 201
    run = client.get(f"/api/authority/runs/{rid}").json()
    assert run["state"] == "COMPLETE" and run["duration_ms"] == 180_000


def test_ingest_rejects_malformed(client):
    assert client.post("/api/v1/lineage", json={"eventType": "START"}).status_code == 422


def test_api_key_enforced():
    c = TestClient(create_app(MemoryStore(), api_key="s3cret"))
    ev = spark_event(new_id(), "START")
    assert c.post("/api/v1/lineage", json=ev).status_code == 401
    assert c.post("/api/v1/lineage", json=ev, headers={"Authorization": "Bearer s3cret"}).status_code == 201


def test_marquez_compatible_reads(client):
    ns = [n["name"] for n in client.get("/api/v1/namespaces").json()["namespaces"]]
    assert "spark-prod" in ns and "agents" in ns
    jobs = client.get("/api/v1/namespaces/spark-prod/jobs").json()["jobs"]
    assert any(j["name"] == "compute_var" for j in jobs)
    g = client.get("/api/v1/lineage", params={"nodeId": "job:spark-prod:compute_var"}).json()["graph"]
    ids = {n["id"] for n in g}
    assert "dataset:abfss://lake@northwind:risk.var_daily" in ids


def test_hub_endpoints(client):
    s = client.get("/api/authority/stats").json()
    assert s["dedup_ratio"] > 1 and s["events_in"] > 100
    agents = client.get("/api/authority/agents").json()
    assert any(a["name"] == "kyc-checker" for a in agents)
    sessions = client.get("/api/authority/sessions").json()
    tree = client.get(f"/api/authority/sessions/{sessions[0]['run_id']}/tree").json()["tree"]
    assert tree["kind"] == "session"
    findings = client.get("/api/authority/findings").json()
    assert {f["rule"] for f in findings} >= {"unattributed-side-effect", "deep-delegation"}
    f = next(f for f in findings if f["run_id"])
    auth = client.get(f"/api/authority/runs/{f['run_id']}/authority").json()
    assert auth["hops"][0]["kind"] == "session"
    br = client.get("/api/authority/blast-radius", params={"node": "dataset:abfss://lake@northwind:raw.trades"}).json()
    assert any(a["name"] == "risk.var_daily" for a in br["affected"])
    replay = client.get(f"/api/authority/runs/{f['run_id']}/replay").json()
    assert replay and replay[0]["event"]["producer"]
    assert client.get("/api/authority/rules").json()
    node = client.get("/api/authority/node", params={"id": "dataset:abfss://lake@northwind:curated.clients"}).json()
    assert node["attrs"]["tags"] == ["gdpr", "pii"]


def test_scoping_for_embedded_views(client):
    ns = next(s["namespace"] for s in client.get("/api/authority/sessions", params={"limit": 500}).json()
              if s["namespace"].startswith("appbuilder/"))
    sessions = client.get("/api/authority/sessions", params={"namespace": ns}).json()
    assert sessions and all(s["namespace"] == ns for s in sessions)
    sid = sessions[0]["run_id"]
    tree = client.get(f"/api/authority/sessions/{sid}/tree").json()["tree"]
    assert tree["job"].startswith("build: ")
    for f in client.get("/api/authority/findings", params={"session": sid}).json():
        assert f["root_run_id"] == sid
    assert all(a["namespace"] == ns for a in client.get("/api/authority/agents", params={"namespace": ns}).json())
