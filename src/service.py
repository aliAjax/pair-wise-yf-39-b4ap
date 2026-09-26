import hashlib
import json
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, DomainError, NotFoundError, ValidationError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
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
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
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
        return updated

    def patch_entity(self, actor, entity_id, data, base_version=None):
        """Apply an offline edit without changing the entity status."""
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        self.rules.validate_patch(actor, entity["kind"])
        expected = int(base_version) if base_version is not None else entity["version"]
        merged = dict(entity["data"])
        merged.update(dict(data or {}))
        updated = self.repository.update_entity(entity_id, expected, entity["status"], merged)
        self.audit.record(
            entity_id,
            actor,
            "patch",
            entity["status"],
            updated["status"],
            {"patch": dict(data or {})},
        )
        return updated

    # ------------------------------------------------------------------
    # Offline batch receive pipeline
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_hash(items):
        blob = json.dumps(items, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def submit_batch(self, actor, batch_id, items):
        """Accept a reconnected batch of offline observation edits.

        - Resending the same batch_id with identical content replays the
          stored result of the first acceptance.
        - Same batch_id with different content is rejected as a conflict;
          no row is stored for the altered attempt.
        - Stale base versions leave the entity untouched and register a
          pending conflict with the failure reason.
        """
        if not batch_id or not isinstance(batch_id, str):
            raise ValidationError("batch_id is required")
        if not isinstance(items, list) or not items:
            raise ValidationError("items must be a non-empty list")

        payload_hash = self._batch_hash(items)
        previous = self.repository.get_batch(batch_id)
        if previous:
            if previous["payload_hash"] != payload_hash:
                raise ConflictError(
                    "batch %s already received with different content" % batch_id
                )
            return previous["result"]

        results = []
        had_conflict = False
        had_failure = False

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValidationError("batch item %d must be an object" % index)
            op = item.get("op", "create")
            kind = item.get("kind", "observation")
            data = item.get("data", {})
            item_id = item.get("id") or (data or {}).get("id")
            try:
                if op == "create":
                    create_data = dict(data or {})
                    if item_id:
                        create_data["id"] = item_id
                    # (batch_id, index) derived keys make a crashed-then-replayed
                    # batch unable to create duplicate entities.
                    entity = self.create(
                        actor,
                        kind,
                        create_data,
                        idempotency_key="batch:%s:%d" % (batch_id, index),
                    )
                    results.append(
                        {"index": index, "op": op, "status": "created", "entity_id": entity["id"]}
                    )
                elif op == "patch":
                    if not item_id:
                        raise ValidationError("patch item requires id")
                    entity = self.patch_entity(
                        actor, item_id, data, base_version=item.get("base_version")
                    )
                    results.append(
                        {"index": index, "op": op, "status": "applied",
                         "entity_id": entity["id"], "version": entity["version"]}
                    )
                elif op == "action":
                    action = item.get("action")
                    if not action:
                        raise ValidationError("action item requires action")
                    if not item_id:
                        raise ValidationError("action item requires id")
                    entity = self.transition(
                        actor, item_id, action, data, expected_version=item.get("base_version")
                    )
                    results.append(
                        {"index": index, "op": op, "status": "applied",
                         "entity_id": entity["id"], "version": entity["version"]}
                    )
                else:
                    raise ValidationError("unknown batch op: " + str(op))
            except ConflictError as exc:
                had_conflict = True
                conflict_id = self.repository.add_conflict(
                    batch_id=batch_id,
                    item_index=index,
                    entity_id=item_id if op != "create" else None,
                    op=op,
                    reason=str(exc),
                    payload=item,
                )
                results.append(
                    {
                        "index": index,
                        "op": op,
                        "status": "conflict",
                        "reason": str(exc),
                        "conflict_id": conflict_id,
                    }
                )
            except DomainError as exc:
                had_failure = True
                results.append(
                    {"index": index, "op": op, "status": "failed", "reason": str(exc)}
                )

        cursor = self.repository.max_audit_id()
        if had_conflict:
            batch_status = "conflict"
        elif had_failure:
            batch_status = "failed"
        else:
            batch_status = "applied"
        result = {
            "batch_id": batch_id,
            "status": batch_status,
            "cursor": cursor,
            "items": results,
        }
        self.repository.save_batch(
            batch_id, actor.user_id, payload_hash, batch_status, cursor, result
        )
        return result

    def list_batches(self, limit=100):
        return self.repository.list_batches(limit=limit)

    def list_conflicts(self, status=None, limit=100):
        return self.repository.list_conflicts(status=status, limit=limit)

    def changes(self, cursor=0, limit=200):
        items = self.repository.list_changes(after_id=int(cursor or 0), limit=limit)
        next_cursor = items[-1]["id"] if items else int(cursor or 0)
        return {"changes": items, "next_cursor": next_cursor, "has_more": len(items) >= limit}

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
