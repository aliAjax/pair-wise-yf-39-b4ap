import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    cursor INTEGER NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    item_index INTEGER NOT NULL,
                    entity_id TEXT,
                    op TEXT,
                    reason TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_conflicts_status
                    ON conflicts(status, id);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def get_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "batch_id": row["batch_id"],
            "actor_id": row["actor_id"],
            "payload_hash": row["payload_hash"],
            "status": row["status"],
            "cursor": int(row["cursor"]),
            "result": json.loads(row["result"]),
            "created_at": row["created_at"],
        }

    def list_batches(self, limit=100):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT batch_id, actor_id, status, cursor, created_at "
                "FROM batches ORDER BY rowid DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            {
                "batch_id": row["batch_id"],
                "actor_id": row["actor_id"],
                "status": row["status"],
                "cursor": int(row["cursor"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def save_batch(self, batch_id, actor_id, payload_hash, status, cursor, result):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO batches(batch_id, actor_id, payload_hash, status, cursor, result, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    actor_id,
                    payload_hash,
                    status,
                    cursor,
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def add_conflict(self, batch_id, item_index, entity_id, op, reason, payload):
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO conflicts(batch_id, item_index, entity_id, op, reason, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    item_index,
                    entity_id,
                    op,
                    reason,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )
            return int(cursor.lastrowid)

    def list_conflicts(self, status=None, limit=100):
        if status:
            sql = (
                "SELECT * FROM conflicts WHERE status = ? ORDER BY id DESC LIMIT ?"
            )
            params = (status, int(limit))
        else:
            sql = "SELECT * FROM conflicts ORDER BY id DESC LIMIT ?"
            params = (int(limit),)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._conflict_from_row(row) for row in rows]

    @staticmethod
    def _conflict_from_row(row):
        return {
            "id": int(row["id"]),
            "batch_id": row["batch_id"],
            "item_index": int(row["item_index"]),
            "entity_id": row["entity_id"],
            "op": row["op"],
            "reason": row["reason"],
            "payload": json.loads(row["payload"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
        }

    def list_changes(self, after_id=0, limit=200):
        """Incremental change feed; audit ids act as the monotonic cursor."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT a.id, a.entity_id, a.actor_id, a.actor_role, a.action, "
                "       a.from_status, a.to_status, a.detail, a.created_at, "
                "       e.kind, e.status AS entity_status, e.version, e.data "
                "FROM audit_log a LEFT JOIN entities e ON e.id = a.entity_id "
                "WHERE a.id > ? ORDER BY a.id LIMIT ?",
                (int(after_id), int(limit)),
            ).fetchall()
        changes = []
        for row in rows:
            changes.append(
                {
                    "id": int(row["id"]),
                    "entity_id": row["entity_id"],
                    "kind": row["kind"],
                    "action": row["action"],
                    "from_status": row["from_status"],
                    "to_status": row["to_status"],
                    "actor_id": row["actor_id"],
                    "actor_role": row["actor_role"],
                    "version": int(row["version"]) if row["version"] is not None else None,
                    "entity_status": row["entity_status"],
                    "data": json.loads(row["data"]) if row["data"] is not None else None,
                    "detail": json.loads(row["detail"]),
                    "created_at": row["created_at"],
                }
            )
        return changes

    def max_audit_id(self):
        with self._connect() as connection:
            row = connection.execute("SELECT COALESCE(MAX(id), 0) AS m FROM audit_log").fetchone()
        return int(row["m"])

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
