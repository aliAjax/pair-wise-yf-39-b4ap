import hashlib
import json
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, DomainError, NotFoundError, ValidationError
from .rules import RuleEngine


def _content_hash(batch_id, operations):
    canonical = json.dumps(
        {"batch_id": batch_id, "operations": operations},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None, batch_id=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        self.repository.append_sync(
            entity_id, kind, "create", entity["version"], actor.user_id, batch_id=batch_id
        )
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None, batch_id=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        self.repository.append_sync(
            entity_id, entity["kind"], action, updated["version"], actor.user_id, batch_id=batch_id
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    def submit_batch(self, actor, payload):
        payload = dict(payload or {})
        batch_id = str(payload.get("batch_id") or "").strip()
        if not batch_id:
            raise ValidationError("batch_id is required")
        operations = payload.get("operations")
        if not isinstance(operations, list) or not operations:
            raise ValidationError("operations must be a non-empty list")
        content_hash = _content_hash(batch_id, operations)
        existing = self.repository.get_batch(batch_id)
        if existing:
            if existing["content_hash"] != content_hash:
                raise ConflictError(
                    "batch already received with different content: " + batch_id
                )
            replay = dict(existing["result"])
            replay["replayed"] = True
            return replay
        results = [
            self._apply_batch_operation(actor, batch_id, index, operation)
            for index, operation in enumerate(operations)
        ]
        applied = sum(1 for item in results if item["status"] == "applied")
        if applied == len(results):
            status = "applied"
        elif applied:
            status = "partial"
        else:
            status = "failed"
        cursor = self.repository.sync_cursor()
        result = {
            "batch_id": batch_id,
            "status": status,
            "cursor": cursor,
            "applied": applied,
            "conflicts": len(results) - applied,
            "results": results,
            "replayed": False,
        }
        self.repository.save_batch(
            batch_id, actor.user_id, content_hash, status, result, cursor
        )
        return result

    def _apply_batch_operation(self, actor, batch_id, index, operation):
        if not isinstance(operation, dict):
            return self._record_batch_conflict(
                batch_id, index, None, None, "operation must be an object", operation
            )
        op = operation.get("op")
        entity_id = operation.get("entity_id")
        try:
            if op == "create":
                entity = self._batch_create(actor, batch_id, operation)
            elif op == "transition":
                entity = self._batch_transition(actor, batch_id, operation)
            else:
                raise ValidationError("unknown batch op: " + str(op))
        except DomainError as exc:
            return self._record_batch_conflict(
                batch_id, index, op, entity_id, str(exc), operation
            )
        return {
            "index": index,
            "op": op,
            "status": "applied",
            "entity_id": entity["id"],
            "version": entity["version"],
        }

    def _batch_create(self, actor, batch_id, operation):
        kind = operation.get("kind")
        if not kind:
            raise ValidationError("kind is required")
        client_op_id = operation.get("client_op_id")
        idempotency_key = "%s:%s" % (batch_id, client_op_id) if client_op_id else None
        return self.create(
            actor,
            kind,
            operation.get("data") or {},
            idempotency_key=idempotency_key,
            batch_id=batch_id,
        )

    def _batch_transition(self, actor, batch_id, operation):
        entity_id = operation.get("entity_id")
        if not entity_id:
            raise ValidationError("entity_id is required")
        action = operation.get("action")
        if not action:
            raise ValidationError("action is required")
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + str(entity_id))
        base_version = operation.get("base_version")
        if base_version is None:
            raise ValidationError("base_version is required")
        try:
            base_version = int(base_version)
        except (TypeError, ValueError):
            raise ValidationError("base_version must be an integer")
        if base_version != entity["version"]:
            raise ConflictError(
                "stale base version for %s: base %s, current %s"
                % (entity_id, base_version, entity["version"])
            )
        return self.transition(
            actor,
            entity_id,
            action,
            operation.get("data") or {},
            expected_version=base_version,
            batch_id=batch_id,
        )

    def _record_batch_conflict(self, batch_id, index, op, entity_id, reason, operation):
        self.repository.append_conflict(
            batch_id=batch_id,
            op_index=index,
            op=op or "unknown",
            entity_id=entity_id,
            reason=reason,
            detail={"operation": operation},
        )
        return {
            "index": index,
            "op": op,
            "status": "conflict",
            "entity_id": entity_id,
            "reason": reason,
        }

    def list_batches(self):
        items = []
        for row in self.repository.list_batches():
            results = row["result"].get("results", [])
            items.append(
                {
                    "batch_id": row["batch_id"],
                    "actor_id": row["actor_id"],
                    "status": row["status"],
                    "cursor": row["cursor"],
                    "applied": sum(1 for item in results if item.get("status") == "applied"),
                    "conflicts": sum(1 for item in results if item.get("status") == "conflict"),
                    "total": len(results),
                    "created_at": row["created_at"],
                }
            )
        return items

    def list_conflicts(self, status=None):
        return self.repository.list_conflicts(status=status)

    def sync_changes(self, since=0):
        try:
            since = int(since or 0)
        except (TypeError, ValueError):
            raise ValidationError("since must be an integer")
        if since < 0:
            raise ValidationError("since must be >= 0")
        return {
            "cursor": self.repository.sync_cursor(),
            "items": self.repository.list_sync_changes(since),
        }
