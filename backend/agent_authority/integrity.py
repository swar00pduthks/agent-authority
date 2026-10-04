"""Tamper evidence for the audit record.

Every stored (dehydrated) event joins a hash chain for its session:

    hash_n = SHA-256( hash_{n-1} | seq_n | payload_n )

so changing, removing or reordering any stored event breaks every later link. Facet and payload blobs are
content-addressed, so a changed blob no longer matches the reference inside the chained payload.

A chain on its own only proves internal consistency: whoever can rewrite the database could rewrite the whole
chain. So after each flush the collector writes the newest head of every touched chain to an **anchor sink**
the database operator cannot rewrite: an append-only file on WORM storage, or an Azure append blob in a
container with an immutability (time-based retention) policy. Verification checks the chain AND that every
anchored head is still present, which also catches truncation.

Chains are written in ingest order, so a session's events must reach one writer in order: key Kafka messages
by the session root run id (the SDK does this), or run a single HTTP ingest replica.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("agent_authority.integrity")

GENESIS = "0" * 64


def link_hash(prev_hash: str, seq: int, payload: str) -> str:
    h = hashlib.sha256()
    h.update(prev_hash.encode())
    h.update(b"|")
    h.update(str(seq).encode())
    h.update(b"|")
    h.update(payload.encode())
    return h.hexdigest()


@dataclass
class Anchor:
    chain_key: str
    seq: int
    hash: str
    at: str

    def to_json(self) -> str:
        return json.dumps(self.__dict__, separators=(",", ":"))


class AnchorSink(ABC):
    """Somewhere the database operator cannot rewrite."""

    @abstractmethod
    def append(self, anchors: list[Anchor]) -> None: ...

    @abstractmethod
    def latest(self, chain_key: str) -> Anchor | None: ...


class MemoryAnchorSink(AnchorSink):
    def __init__(self) -> None:
        self._by_key: dict[str, Anchor] = {}

    def append(self, anchors: list[Anchor]) -> None:
        for a in anchors:
            cur = self._by_key.get(a.chain_key)
            if cur is None or a.seq >= cur.seq:
                self._by_key[a.chain_key] = a

    def latest(self, chain_key: str) -> Anchor | None:
        return self._by_key.get(chain_key)


class FileAnchorSink(AnchorSink):
    """Append-only JSON-lines files, one per UTC day. Put the directory on WORM storage
    (an immutable Azure Files share, an S3 Object Lock mount, or a write-once volume)."""

    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._index: dict[str, Anchor] | None = None

    def _file(self) -> Path:
        return self.dir / f"anchors-{datetime.now(UTC):%Y-%m-%d}.jsonl"

    def append(self, anchors: list[Anchor]) -> None:
        if not anchors:
            return
        with self._lock, open(self._file(), "a", encoding="utf-8") as f:
            f.write("".join(a.to_json() + "\n" for a in anchors))
            f.flush()
            os.fsync(f.fileno())
            if self._index is not None:
                for a in anchors:
                    self._index[a.chain_key] = a

    def latest(self, chain_key: str) -> Anchor | None:
        with self._lock:
            if self._index is None:
                self._index = {}
                for p in sorted(self.dir.glob("anchors-*.jsonl")):
                    for line in p.read_text(encoding="utf-8").splitlines():
                        if line.strip():
                            a = Anchor(**json.loads(line))
                            cur = self._index.get(a.chain_key)
                            if cur is None or a.seq >= cur.seq:
                                self._index[a.chain_key] = a
            return self._index.get(chain_key)


class AzureBlobAnchorSink(AnchorSink):
    """Append blob per UTC day in a container that has a time-based immutability policy, so appended
    anchors can't be modified or deleted until retention ends. Needs `azure-storage-blob`.

    Experimental: written against the azure-storage-blob API, not yet run against a live Azure account.
    """

    def __init__(self, container_client: Any) -> None:
        self.cc = container_client
        self._index: dict[str, Anchor] = {}
        self._loaded = False

    @classmethod
    def from_connection_string(cls, conn: str, container: str) -> AzureBlobAnchorSink:
        from azure.storage.blob import BlobServiceClient

        return cls(BlobServiceClient.from_connection_string(conn).get_container_client(container))

    def append(self, anchors: list[Anchor]) -> None:
        if not anchors:
            return
        blob = self.cc.get_blob_client(f"anchors-{datetime.now(UTC):%Y-%m-%d}.jsonl")
        if not blob.exists():
            blob.create_append_blob()
        blob.append_block("".join(a.to_json() + "\n" for a in anchors).encode())
        for a in anchors:
            self._index[a.chain_key] = a

    def latest(self, chain_key: str) -> Anchor | None:
        if not self._loaded:
            for props in self.cc.list_blobs(name_starts_with="anchors-"):
                data = self.cc.get_blob_client(props.name).download_blob().readall().decode()
                for line in data.splitlines():
                    if line.strip():
                        a = Anchor(**json.loads(line))
                        cur = self._index.get(a.chain_key)
                        if cur is None or a.seq >= cur.seq:
                            self._index[a.chain_key] = a
            self._loaded = True
        return self._index.get(chain_key)


def anchor_sink_from_env() -> AnchorSink | None:
    if conn := os.getenv("AUTHORITY_ANCHOR_AZURE_CONNECTION_STRING"):
        return AzureBlobAnchorSink.from_connection_string(conn, os.getenv("AUTHORITY_ANCHOR_AZURE_CONTAINER", "anchors"))
    if d := os.getenv("AUTHORITY_ANCHOR_DIR"):
        return FileAnchorSink(d)
    return None


# ------------------------------------------------------------------------- verification
@dataclass
class IntegrityReport:
    chain_key: str
    status: str  # intact | broken | unanchored | empty
    events: int = 0
    head_seq: int = 0
    head_hash: str | None = None
    anchored_seq: int | None = None
    anchored_at: str | None = None
    problems: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _refs(obj: Any, out: list[tuple[str, str]]) -> None:
    if isinstance(obj, dict):
        for k in ("$ref", "$meta"):
            if isinstance(obj.get(k), str):
                out.append(("facet", obj[k]))
        if isinstance(obj.get("$blob"), str):
            out.append(("payload", obj["$blob"]))
        for v in obj.values():
            _refs(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _refs(v, out)


def verify_chain(store: Any, chain_key: str, anchors: AnchorSink | None) -> IntegrityReport:
    from agent_authority.collector.facets import HASH_HEX, facet_hash

    events = store.chain_events(chain_key)
    rep = IntegrityReport(chain_key, "empty", events=len(events))
    if not events:
        return rep
    prev, expected_seq = GENESIS, 1
    blob_cache: dict[tuple[str, str], bool] = {}
    for ev in events:
        if ev.seq != expected_seq:
            rep.problems.append(f"sequence gap: expected {expected_seq}, found {ev.seq}")
        if ev.prev_hash != prev:
            rep.problems.append(f"event {ev.seq}: previous-hash link does not match")
        if link_hash(ev.prev_hash, ev.seq, ev.payload) != ev.hash:
            rep.problems.append(f"event {ev.seq} ({ev.event_type} {ev.job_name}): content does not match its hash")
        refs: list[tuple[str, str]] = []
        try:
            _refs(json.loads(ev.payload), refs)
        except json.JSONDecodeError:
            rep.problems.append(f"event {ev.seq}: payload is not valid JSON")
        for kind, h in refs:
            if (kind, h) in blob_cache:
                ok = blob_cache[(kind, h)]
            else:
                if kind == "facet":
                    name, body = store.get_facet_named(h)
                    ok = body is not None and facet_hash(name, json.loads(body)) == h
                else:
                    body = store.get_payload(h)
                    ok = body is not None and hashlib.sha256(body.encode()).hexdigest()[:HASH_HEX] == h
                blob_cache[(kind, h)] = ok
            if not ok:
                rep.problems.append(f"event {ev.seq}: referenced {kind} {h[:12]}… is missing or altered")
        prev, expected_seq = ev.hash, ev.seq + 1
    rep.head_seq, rep.head_hash = events[-1].seq, events[-1].hash
    if anchors is not None and (a := anchors.latest(chain_key)) is not None:
        rep.anchored_seq, rep.anchored_at = a.seq, a.at
        match = next((e for e in events if e.seq == a.seq), None)
        if match is None:
            rep.problems.append(f"anchored event {a.seq} is missing: the record was truncated")
        elif match.hash != a.hash:
            rep.problems.append(f"event {a.seq} differs from its anchored hash: the chain was rewritten")
    rep.problems = list(dict.fromkeys(rep.problems))
    if rep.problems:
        rep.status = "broken"
    elif rep.anchored_seq is None:
        rep.status = "unanchored"
    else:
        rep.status = "intact"
    return rep
