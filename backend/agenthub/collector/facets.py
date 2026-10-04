"""Facet classification, canonical hashing, and lossless dehydration.

Static facets (schema, SQL, source code, Spark plan ...) rarely change between
runs but dominate event size. They are hashed (canonical JSON, SHA-256) and
replaced in the stored event by ``{"$ref": "<hash>"}``. ``rehydrate`` restores
the original event byte-for-byte equivalent (modulo key order).
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

REF_KEY = "$ref"

# Location -> facet names treated as static. Anything not listed is dynamic.
# Dataset ``facets`` are static by default (run-specific values live in
# ``inputFacets``/``outputFacets`` per the OpenLineage spec).
DEFAULT_STATIC: dict[str, set[str] | str] = {
    "job": {
        "sql",
        "sourceCode",
        "sourceCodeLocation",
        "jobType",
        "documentation",
        "ownership",
    },
    "run": {
        "spark.logicalPlan",
        "spark_properties",
        "spark_version",
        "spark_unknown",
        "processing_engine",
        "environment-properties",
    },
    "dataset": "*",
}
DATASET_DYNAMIC = {"lifecycleStateChange", "version"}


def canonical_json(obj: Any) -> str:
    """Deterministic JSON (sorted keys, compact) - an RFC 8785-style subset sufficient for hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def facet_hash(name: str, body: Any) -> str:
    h = hashlib.sha256()
    h.update(name.encode())
    h.update(b"\x00")
    h.update(canonical_json(body).encode())
    return h.hexdigest()


@dataclass
class FacetPolicy:
    static: dict[str, set[str] | str] = field(default_factory=lambda: copy.deepcopy(DEFAULT_STATIC))

    def is_static(self, location: str, name: str) -> bool:
        rule = self.static.get(location)
        if rule == "*":
            return name not in DATASET_DYNAMIC
        return bool(rule) and name in rule  # type: ignore[operator]


@dataclass
class Dehydrated:
    event: dict[str, Any]
    refs: dict[str, tuple[str, str]]  # hash -> (facet name, canonical body)
    run_refs: set[str]
    job_refs: set[str]
    dataset_schema_hash: dict[tuple[str, str], str]  # (ns, name) -> schema facet hash


def _dehydrate_facets(
    facets: dict[str, Any] | None,
    location: str,
    policy: FacetPolicy,
    refs: dict[str, tuple[str, str]],
    on_ref: Callable[[str, str], None] | None = None,
) -> tuple[dict[str, Any] | None, set[str]]:
    if not facets:
        return facets, set()
    out: dict[str, Any] = {}
    local: set[str] = set()
    for name, body in facets.items():
        if isinstance(body, dict) and REF_KEY in body:
            out[name] = body  # already dehydrated upstream
            continue
        if policy.is_static(location, name):
            h = facet_hash(name, body)
            refs[h] = (name, canonical_json(body))
            local.add(h)
            out[name] = {REF_KEY: h}
            if on_ref:
                on_ref(name, h)
        else:
            out[name] = body
    return out, local


def dehydrate(event: dict[str, Any], policy: FacetPolicy | None = None) -> Dehydrated:
    policy = policy or FacetPolicy()
    ev = copy.deepcopy(event)
    refs: dict[str, tuple[str, str]] = {}
    schema_hash: dict[tuple[str, str], str] = {}

    run = ev.get("run") or {}
    run["facets"], run_refs = _dehydrate_facets(run.get("facets"), "run", policy, refs)
    job = ev.get("job") or {}
    job["facets"], job_refs = _dehydrate_facets(job.get("facets"), "job", policy, refs)

    for key in ("inputs", "outputs"):
        for ds in ev.get(key) or []:
            ns, nm = ds.get("namespace", ""), ds.get("name", "")

            def _capture(name: str, h: str, ns: str = ns, nm: str = nm) -> None:
                if name == "schema":
                    schema_hash[(ns, nm)] = h

            ds["facets"], _ = _dehydrate_facets(ds.get("facets"), "dataset", policy, refs, _capture)

    return Dehydrated(ev, refs, run_refs, job_refs, schema_hash)


def rehydrate(event: dict[str, Any], lookup: Callable[[str], str | None]) -> dict[str, Any]:
    """Replace every ``{"$ref": h}`` facet with its stored body."""
    ev = copy.deepcopy(event)

    def _fill(facets: dict[str, Any] | None) -> None:
        if not facets:
            return
        for name, body in list(facets.items()):
            if isinstance(body, dict) and set(body) == {REF_KEY}:
                stored = lookup(body[REF_KEY])
                if stored is None:
                    raise KeyError(f"facet blob {body[REF_KEY]} ({name}) not found")
                facets[name] = json.loads(stored)

    _fill((ev.get("run") or {}).get("facets"))
    _fill((ev.get("job") or {}).get("facets"))
    for key in ("inputs", "outputs"):
        for ds in ev.get(key) or []:
            _fill(ds.get("facets"))
    return ev
