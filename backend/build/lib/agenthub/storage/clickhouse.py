"""ClickHouse run store: runs, run I/O, raw events, facet/payload blobs, aggregates, ingest stats.

Lifecycle merging happens in ``AggregatingMergeTree``; the collector only ever
appends partial rows. Reads always ``GROUP BY`` the sorting key, so results are
correct before and after background merges.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from importlib import resources
from typing import Any, Protocol

from agenthub.model import RawEvent, RunIO, RunRecord, ToolAggregate
from agenthub.storage.base import Batch
from agenthub.storage.common import build_agent_catalog


# --------------------------------------------------------------------- clients
def q(v: Any) -> str:
    """Quote a Python value as a ClickHouse literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int | float):
        return repr(v)
    if isinstance(v, datetime):
        return f"'{v.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
    if isinstance(v, list | tuple | set):
        return "[" + ",".join(q(x) for x in v) + "]"
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def render(sql: str, params: dict[str, Any] | None = None) -> str:
    for k, v in (params or {}).items():
        sql = sql.replace(f"%({k})s", q(v))
    return sql


class CHClient(Protocol):
    def command(self, sql: str) -> None: ...
    def insert(self, table: str, rows: list[list[Any]], columns: list[str]) -> None: ...
    def query(self, sql: str) -> list[dict[str, Any]]: ...


class ConnectClient:
    """clickhouse-connect (HTTP) adapter."""

    def __init__(self, **kwargs: Any) -> None:
        import clickhouse_connect

        self.c = clickhouse_connect.get_client(**kwargs)

    def command(self, sql: str) -> None:
        self.c.command(sql)

    def insert(self, table: str, rows: list[list[Any]], columns: list[str]) -> None:
        if rows:
            self.c.insert(table, rows, column_names=columns)

    def query(self, sql: str) -> list[dict[str, Any]]:
        res = self.c.query(sql)
        return [dict(zip(res.column_names, r, strict=True)) for r in res.result_rows]


class ChdbClient:
    """Embedded ClickHouse (chdb) adapter - used by tests and single-node setups."""

    def __init__(self, path: str | None = None) -> None:
        from chdb import session

        self.s = session.Session(path) if path else session.Session()

    def command(self, sql: str) -> None:
        self.s.query(sql)

    def insert(self, table: str, rows: list[list[Any]], columns: list[str]) -> None:
        if not rows:
            return
        values = ",".join("(" + ",".join(q(v) for v in r) + ")" for r in rows)
        self.s.query(f"INSERT INTO {table} ({','.join(columns)}) VALUES {values}")

    def query(self, sql: str) -> list[dict[str, Any]]:
        out = self.s.query(sql, "JSONEachRow")
        text = out.bytes().decode() if hasattr(out, "bytes") else str(out)
        return [json.loads(line) for line in text.splitlines() if line.strip()]


# ----------------------------------------------------------------------- store
RUN_SELECT = """
SELECT namespace, job_name, run_id,
       anyLast(job_kind) AS kind_, anyLast(root_run_id) AS root_, anyLast(parent_run_id) AS parent_,
       anyLast(run_path) AS path_, min(start_time) AS start_, max(end_time) AS end_,
       max(last_event_time) AS last_, max(state) AS state_, sum(event_count) AS events_,
       groupUniqArrayArray(facet_refs) AS refs_, anyLast(dyn_facets) AS dyn_,
       anyLast(agent_id) AS agent_, anyLast(owner) AS owner_, anyLast(on_behalf_of) AS obo_,
       anyLast(tool_name) AS tool_, anyLast(side_effecting) AS se_, anyLast(policy_decision) AS pol_,
       anyLast(policy_rule) AS rule_, anyLast(model) AS model_, anyLast(error_message) AS err_
FROM {db}.runs
"""
RUN_GROUP = " GROUP BY namespace, job_name, run_id "


def _dt(v: Any) -> datetime | None:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(v).replace(" ", "T")).replace(tzinfo=UTC)


def _run(row: dict[str, Any]) -> RunRecord:
    se = row.get("se_")
    return RunRecord(
        run_id=row["run_id"], namespace=row["namespace"], job_name=row["job_name"], job_kind=row["kind_"],
        root_run_id=row.get("root_"), parent_run_id=row.get("parent_"), run_path=row.get("path_") or "",
        start_time=_dt(row.get("start_")), end_time=_dt(row.get("end_")), state=int(row["state_"]),
        facet_refs=set(row.get("refs_") or []), dyn_facets=json.loads(row.get("dyn_") or "{}"),
        last_event_time=_dt(row.get("last_")), event_count=int(row.get("events_") or 0),
        agent_id=row.get("agent_"), owner=row.get("owner_"), on_behalf_of=row.get("obo_"), tool_name=row.get("tool_"),
        side_effecting=None if se is None else bool(int(se)), policy_decision=row.get("pol_"),
        policy_rule=row.get("rule_"), model=row.get("model_"), error_message=row.get("err_"),
    )


class ClickHouseRunStore:
    def __init__(self, client: CHClient, database: str = "agenthub", run_ttl_days: int = 30) -> None:
        self.c = client
        self.db = database
        self.ttl = run_ttl_days

    # ------------------------------------------------------------- schema
    def ensure_schema(self) -> None:
        self.c.command(f"CREATE DATABASE IF NOT EXISTS {self.db}")
        ddl = resources.files("agenthub.storage").joinpath("sql/clickhouse.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.strip().startswith("--"))
        for stmt in ddl.split(";"):
            if stmt.strip():
                self.c.command(stmt.format(db=self.db, run_ttl_days=self.ttl))

    def _sql(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        return self.c.query(render(sql.replace("{db}", self.db), params))

    # --------------------------------------------------------------- write
    def write(self, b: Batch) -> None:
        t = f"{self.db}."
        self.c.insert(t + "facet_blob", [[f.hash, f.facet_name, f.body, f.first_seen] for f in b.facets],
                      ["hash", "facet_name", "body", "first_seen"])
        self.c.insert(t + "payload_blob", [[h, body] for h, body in b.payloads.items()], ["hash", "body"])
        self.c.insert(t + "runs", [[
            r.namespace, r.job_name, r.run_id, r.job_kind, r.root_run_id, r.parent_run_id, r.run_path,
            r.start_time, r.end_time, r.last_event_time, int(r.state), r.event_count, sorted(r.facet_refs),
            json.dumps(r.dyn_facets, separators=(",", ":"), default=str), r.agent_id, r.owner, r.on_behalf_of,
            r.tool_name, None if r.side_effecting is None else int(r.side_effecting), r.policy_decision,
            r.policy_rule, r.model, r.error_message, (r.last_event_time or datetime.now(UTC)).date(),
        ] for r in b.runs], [
            "namespace", "job_name", "run_id", "job_kind", "root_run_id", "parent_run_id", "run_path",
            "start_time", "end_time", "last_event_time", "state", "event_count", "facet_refs", "dyn_facets",
            "agent_id", "owner", "on_behalf_of", "tool_name", "side_effecting", "policy_decision", "policy_rule",
            "model", "error_message", "event_date",
        ])
        self.c.insert(t + "run_io", [[x.dataset_id, x.run_id, x.direction, x.schema_hash, x.row_count, x.event_time]
                                     for x in b.run_io],
                      ["dataset_id", "run_id", "direction", "schema_hash", "row_count", "event_time"])
        self.c.insert(t + "raw_events", [[e.run_id, e.event_time, e.event_type, e.namespace, e.job_name, e.bytes_in,
                                          e.payload] for e in b.raw],
                      ["run_id", "event_time", "event_type", "namespace", "job_name", "bytes_in", "payload"])
        self.c.insert(t + "tool_aggregates", [[a.parent_run_id or "", a.tool_name, a.calls, a.failures, a.total_ms,
                                               a.last_seen] for a in b.tool_aggs.values()],
                      ["parent_run_id", "tool_name", "calls", "failures", "total_ms", "last_seen"])
        s = b.stats
        if s.events_in:
            minute = datetime.now(UTC).replace(second=0, microsecond=0, tzinfo=None)
            self.c.insert(t + "ingest_stats", [[minute, s.events_in, s.bytes_in, s.bytes_stored, s.facet_refs,
                                                s.facet_new, s.runs_kept_events, s.sampled_out, s.aggregated,
                                                s.payloads_offloaded]],
                          ["minute", "events_in", "bytes_in", "bytes_stored", "facet_refs", "facet_new",
                           "runs_kept_events", "sampled_out", "aggregated", "payloads_offloaded"])

    # ---------------------------------------------------------------- read
    def get_facet(self, h: str) -> str | None:
        rows = self._sql("SELECT body FROM {db}.facet_blob WHERE hash = %(h)s LIMIT 1", h=h)
        return rows[0]["body"] if rows else None

    def get_payload(self, h: str) -> str | None:
        rows = self._sql("SELECT body FROM {db}.payload_blob WHERE hash = %(h)s LIMIT 1", h=h)
        return rows[0]["body"] if rows else None

    def list_runs(self, namespace=None, job_name=None, kind=None, agent_id=None, roots_only=False, limit=100):
        where, having = [], []
        if namespace:
            where.append(f"namespace = {q(namespace)}")
        if job_name:
            where.append(f"job_name = {q(job_name)}")
        if kind:
            having.append(f"kind_ = {q(kind)}")
        if agent_id:
            having.append(f"agent_ = {q(agent_id)}")
        if roots_only:
            having.append("parent_ IS NULL")
        sql = RUN_SELECT + (" WHERE " + " AND ".join(where) if where else "") + RUN_GROUP
        sql += (" HAVING " + " AND ".join(having) if having else "") + f" ORDER BY last_ DESC LIMIT {int(limit)}"
        return [_run(r) for r in self._sql(sql)]

    def get_run(self, run_id: str) -> RunRecord | None:
        rows = self._sql(RUN_SELECT + " WHERE run_id = %(r)s" + RUN_GROUP, r=run_id)
        return _run(rows[0]) if rows else None

    def run_tree(self, root_run_id: str) -> list[RunRecord]:
        rows = self._sql(RUN_SELECT + " WHERE run_id IN (SELECT run_id FROM {db}.runs WHERE root_run_id = %(r)s)"
                         " OR run_id = %(r)s" + RUN_GROUP, r=root_run_id)
        return [_run(r) for r in rows]

    def run_io(self, run_id=None, dataset_id=None, limit=200) -> list[RunIO]:
        where = []
        if run_id:
            where.append(f"run_id = {q(run_id)}")
        if dataset_id:
            where.append(f"dataset_id = {q(dataset_id)}")
        sql = ("SELECT dataset_id, run_id, direction, schema_hash, row_count, event_time FROM {db}.run_io"
               + (" WHERE " + " AND ".join(where) if where else "") + f" ORDER BY event_time DESC LIMIT {int(limit)}")
        return [RunIO(r["run_id"], r["dataset_id"], r["direction"], r["schema_hash"],
                      None if r["row_count"] is None else int(r["row_count"]), _dt(r["event_time"]))
                for r in self._sql(sql)]

    def raw_events(self, run_id: str) -> list[RawEvent]:
        rows = self._sql("SELECT * FROM {db}.raw_events WHERE run_id = %(r)s ORDER BY event_time", r=run_id)
        return [RawEvent(_dt(r["event_time"]), r["event_type"], r["run_id"], r["namespace"], r["job_name"],
                         r["payload"], int(r["bytes_in"])) for r in rows]

    def tool_aggregates(self, parent_run_id=None) -> list[ToolAggregate]:
        where = f" WHERE parent_run_id = {q(parent_run_id)}" if parent_run_id else ""
        rows = self._sql("SELECT parent_run_id, tool_name, sum(calls) AS c, sum(failures) AS f, sum(total_ms) AS ms,"
                         " max(last_seen) AS ls FROM {db}.tool_aggregates" + where + " GROUP BY parent_run_id, tool_name")
        return [ToolAggregate(r["parent_run_id"] or None, r["tool_name"], int(r["c"]), int(r["f"]), float(r["ms"]),
                              _dt(r["ls"])) for r in rows]

    def agent_catalog(self, window: int = 50_000) -> list[dict[str, Any]]:
        runs = {r.run_id: r for r in self.list_runs(limit=window) if r.job_kind != "job"}
        return build_agent_catalog(runs, self.tool_aggregates())

    def stats(self) -> dict[str, Any]:
        cols = ["events_in", "bytes_in", "bytes_stored", "facet_refs", "facet_new", "runs_kept_events",
                "sampled_out", "aggregated", "payloads_offloaded"]
        s = self._sql("SELECT " + ", ".join(f"sum({c}) AS {c}" for c in cols) + " FROM {db}.ingest_stats")[0]
        s = {k: int(v or 0) for k, v in s.items()}
        f = self._sql("SELECT uniqExact(hash) AS n, sum(length(body)) AS b FROM {db}.facet_blob")[0]
        r = self._sql("SELECT uniqExact(run_id) AS runs, uniqExactIf(run_id, job_kind = 'agent') AS agent_runs"
                      " FROM {db}.runs")[0]
        s.update(unique_facets=int(f["n"] or 0), facet_bytes=int(f["b"] or 0), runs=int(r["runs"] or 0),
                 agent_runs=int(r["agent_runs"] or 0))
        s["dedup_ratio"] = round(s["bytes_in"] / s["bytes_stored"], 2) if s["bytes_stored"] else 0.0
        return s
