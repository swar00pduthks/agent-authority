from __future__ import annotations

import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from agent_authority.api.main import create_app
from agent_authority.auth import AuthConfig
from agent_authority.storage.memory import MemoryStore

from .conftest import new_id, spark_event

ISS = "https://login.microsoftonline.com/tenant-123/v2.0"
AUD = "api://agent-authority"


@pytest.fixture(scope="module")
def keys():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update(kid="k1", use="sig", alg="RS256")
    return key, {"keys": [jwk]}


def token(key, **over) -> str:
    now = int(time.time())
    claims = {"iss": ISS, "aud": AUD, "sub": "s-1", "oid": "oid-alice", "preferred_username": "alice@northwind.example",
              "roles": ["Authority.Read"], "iat": now, "exp": now + 600, **over}
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "k1"})


@pytest.fixture
def client(keys) -> TestClient:
    _, jwks = keys
    cfg = AuthConfig(issuer=ISS, audience=AUD, jwks=jwks, read_role="Authority.Read", api_key="spark-key")
    return TestClient(create_app(MemoryStore(), auth=cfg))


def h(t: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {t}"}


def test_read_requires_valid_token(client, keys):
    key, _ = keys
    assert client.get("/api/authority/stats").status_code == 401
    assert client.get("/api/authority/stats", headers=h("garbage")).status_code == 401
    assert client.get("/api/authority/stats", headers=h(token(key))).status_code == 200
    me = client.get("/api/authority/me", headers=h(token(key))).json()
    assert me == {"subject": "oid-alice", "name": "alice@northwind.example", "roles": ["Authority.Read"], "via": "oidc"}


def test_expired_wrong_audience_wrong_issuer_and_missing_role(client, keys):
    key, _ = keys
    r = client.get("/api/authority/stats", headers=h(token(key, exp=int(time.time()) - 3600)))
    assert r.status_code == 401 and r.json()["detail"] == "token expired"
    assert "WWW-Authenticate" in r.headers
    assert client.get("/api/authority/stats", headers=h(token(key, aud="api://other"))).status_code == 401
    assert client.get("/api/authority/stats", headers=h(token(key, iss="https://evil.example/"))).status_code == 401
    assert client.get("/api/authority/stats", headers=h(token(key, roles=[]))).status_code == 403


def test_token_signed_by_another_key_is_rejected(client):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    assert client.get("/api/authority/stats", headers=h(token(other))).status_code == 401


def test_ingest_accepts_api_key_or_token_and_health_is_public(client, keys):
    key, _ = keys
    ev = spark_event(new_id(), "START")
    assert client.post("/api/v1/lineage", json=ev).status_code == 401
    assert client.post("/api/v1/lineage", json=ev, headers=h("spark-key")).status_code == 201
    assert client.post("/api/v1/lineage", json=ev, headers=h(token(key))).status_code == 201
    assert client.get("/api/authority/health").json()["auth"] == "oidc"


def test_open_mode_without_issuer():
    c = TestClient(create_app(MemoryStore()))
    assert c.get("/api/authority/stats").status_code == 200
    assert c.get("/api/authority/health").json()["auth"] == "open"
