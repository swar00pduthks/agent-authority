"""Postgres graph store: nodes, node versions and aggregated edges.

Writes scale with *structural change*, not run volume: the collector
pre-aggregates edges per batch and only sends nodes/versions it has not
recently written. Traversals use recursive CTEs (fine to ~5-6 hops on graphs of
this size); the schema maps 1:1 onto an Apache AGE property graph for deeper
path queries.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources
from typing import Any

from agenthub.model import Node, NodeVersion
from agenthub.storage.base import Batch, GraphEdge, Subgraph


class _SingleConnection:
    """Fallback when psycopg_pool is not installed: one connection, serialised by a lock."""

    def __init__(self, dsn: str) -> None:
        import psycopg

        self.conn = psycopg.connect(dsn)
        self.lock = threading.Lock()

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self.lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise


class PostgresGraphStore:
    def __init__(self, dsn: str, pool_size: int = 8) -> None:
        try:
            from psycopg_pool import ConnectionPool

            self.pool: Any = ConnectionPool(dsn, min_size=1, max_size=pool_size, open=True)
        except ImportError:
            self.pool = _SingleConnection(dsn)

    def ensure_schema(self) -> None:
        ddl = resources.files("agenthub.storage").joinpath("sql/postgres.sql").read_text()
        with self.pool.connection() as conn:
            conn.execute(ddl)

    # --------------------------------------------------------------- write
    def write(self, b: Batch) -> None:
        if not (b.nodes or b.node_versions or b.edges):
            return
        with self.pool.connection() as conn, conn.cursor() as cur:
            if b.nodes:
                cur.executemany(
                    """INSERT INTO ah_nodes (id, kind, namespace, name, attrs) VALUES (%s, %s, %s, %s, %s::jsonb)
                       ON CONFLICT (id) DO UPDATE SET
                         kind = CASE WHEN EXCLUDED.kind <> 'job' THEN EXCLUDED.kind ELSE ah_nodes.kind END,
                         attrs = ah_nodes.attrs || EXCLUDED.attrs, updated_at = now()""",
                    [(n.id, n.kind, n.namespace, n.name, json.dumps(n.attrs)) for n in b.nodes],
                )
            if b.node_versions:
                cur.executemany(
                    """INSERT INTO ah_node_versions (node_id, hash, valid_from) VALUES (%s, %s, %s)
                       ON CONFLICT (node_id, hash) DO UPDATE SET valid_from = LEAST(ah_node_versions.valid_from,
                       EXCLUDED.valid_from)""",
                    [(v.node_id, v.hash, v.valid_from) for v in b.node_versions],
                )
            if b.edges:
                cur.executemany(
                    """INSERT INTO ah_edges (src, dst, type, first_seen, last_seen, run_count)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (src, dst, type) DO UPDATE SET
                         first_seen = LEAST(ah_edges.first_seen, EXCLUDED.first_seen),
                         last_seen = GREATEST(ah_edges.last_seen, EXCLUDED.last_seen),
                         run_count = ah_edges.run_count + EXCLUDED.run_count""",
                    [(e.src, e.dst, e.type, e.first_seen, e.last_seen, e.run_count) for e in b.edges.values()],
                )

    # ---------------------------------------------------------------- read
    def _rows(self, sql: str, params: tuple | dict = ()) -> list[tuple]:
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    @staticmethod
    def _node(r: tuple) -> Node:
        attrs = r[4] if isinstance(r[4], dict) else json.loads(r[4] or "{}")
        return Node(r[0], r[1], r[2], r[3], attrs)

    def list_nodes(self, kind=None, namespace=None, q=None, limit=500) -> list[Node]:
        where, params = [], []
        if kind:
            where.append("kind = %s")
            params.append(kind)
        if namespace:
            where.append("namespace = %s")
            params.append(namespace)
        if q:
            where.append("id ILIKE %s")
            params.append(f"%{q}%")
        sql = "SELECT id, kind, namespace, name, attrs FROM ah_nodes"
        sql += (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY id LIMIT %s"
        return [self._node(r) for r in self._rows(sql, (*params, limit))]

    def get_node(self, node_id: str) -> Node | None:
        rows = self._rows("SELECT id, kind, namespace, name, attrs FROM ah_nodes WHERE id = %s", (node_id,))
        return self._node(rows[0]) if rows else None

    def node_versions(self, node_id: str) -> list[NodeVersion]:
        return [NodeVersion(node_id, h, t) for h, t in self._rows(
            "SELECT hash, valid_from FROM ah_node_versions WHERE node_id = %s ORDER BY valid_from", (node_id,))]

    def subgraph(self, node_id: str, depth: int = 3, direction: str = "both") -> Subgraph:
        parts = []
        if direction in ("down", "both"):
            parts.append("""SELECT id FROM (WITH RECURSIVE w(id, d) AS (
                SELECT %(n)s::text, 0 UNION SELECT e.dst, w.d + 1 FROM ah_edges e JOIN w ON e.src = w.id
                WHERE w.d < %(depth)s) SELECT id FROM w) dn""")
        if direction in ("up", "both"):
            parts.append("""SELECT id FROM (WITH RECURSIVE w(id, d) AS (
                SELECT %(n)s::text, 0 UNION SELECT e.src, w.d + 1 FROM ah_edges e JOIN w ON e.dst = w.id
                WHERE w.d < %(depth)s) SELECT id FROM w) up""")
        sql = f"""WITH ids AS ({' UNION '.join(parts)})
            SELECT id, kind, namespace, name, attrs FROM ah_nodes WHERE id IN (SELECT id FROM ids)"""
        nodes = [self._node(r) for r in self._rows(sql, {"n": node_id, "depth": depth})]
        if not nodes:
            return Subgraph([], [])
        ids = [n.id for n in nodes]
        edges = self._edges("WHERE src = ANY(%s) AND dst = ANY(%s)", (ids, ids))
        if direction == "down":
            edges = self._directional(node_id, edges, down=True)
        elif direction == "up":
            edges = self._directional(node_id, edges, down=False)
        return Subgraph(nodes, edges)

    @staticmethod
    def _directional(origin: str, edges: list[GraphEdge], down: bool) -> list[GraphEdge]:
        """Keep only edges on traversal paths in the requested direction (matches the in-memory BFS)."""
        reach = {origin}
        changed = True
        while changed:
            changed = False
            for e in edges:
                a, b = (e.src, e.dst) if down else (e.dst, e.src)
                if a in reach and b not in reach:
                    reach.add(b)
                    changed = True
        return [e for e in edges if (e.src if down else e.dst) in reach]

    def _edges(self, where: str = "", params: tuple = ()) -> list[GraphEdge]:
        return [GraphEdge(*r) for r in self._rows(
            f"SELECT src, dst, type, first_seen, last_seen, run_count FROM ah_edges {where}", params)]

    def graph(self, namespace=None, limit=2000) -> Subgraph:
        nodes = self.list_nodes(namespace=namespace, limit=limit)
        ids = [n.id for n in nodes]
        return Subgraph(nodes, self._edges("WHERE src = ANY(%s) AND dst = ANY(%s)", (ids, ids)))

    def counts(self) -> dict[str, Any]:
        (n, d, e), = self._rows("""SELECT (SELECT count(*) FROM ah_nodes),
                                          (SELECT count(*) FROM ah_nodes WHERE kind = 'dataset'),
                                          (SELECT count(*) FROM ah_edges)""")
        return {"nodes": n, "datasets": d, "jobs": n - d, "edges": e}
