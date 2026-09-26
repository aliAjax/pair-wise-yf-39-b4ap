import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def observation(event_id="E-1", observed_at="2026-04-01"):
    return {
        "event_id": event_id,
        "species": "deer",
        "location": "North",
        "observed_at": observed_at,
        "lat": 40.0,
        "lon": 116.0,
    }


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"
        self.repo = SQLiteRepository(self.db_path)
        self.service = DomainService(self.repo, RuleEngine())
        self.actor = Actor("field-1", "field")

    def tearDown(self):
        self.tmp.cleanup()

    def _items(self, event_id="E-100"):
        return [{"op": "create", "kind": "observation", "data": observation(event_id)}]

    def test_successful_batch_returns_increasing_cursor(self):
        result = self.service.submit_batch(self.actor, "batch-1", self._items("E-100"))
        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["items"][0]["status"], "created")
        self.assertGreaterEqual(result["cursor"], 1)

        second = self.service.submit_batch(
            self.actor, "batch-2", self._items("E-101")
        )
        self.assertGreaterEqual(second["cursor"], result["cursor"])

        feed = self.service.changes(cursor=0)
        self.assertEqual(feed["next_cursor"], second["cursor"])
        self.assertTrue(any(change["action"] == "create" for change in feed["changes"]))

        after = self.service.changes(cursor=result["cursor"])
        self.assertTrue(all(change["id"] > result["cursor"] for change in after["changes"]))

    def test_resending_same_batch_replays_first_result(self):
        first = self.service.submit_batch(self.actor, "dup", self._items("E-200"))
        second = self.service.submit_batch(self.actor, "dup", self._items("E-200"))
        self.assertEqual(first, second)
        self.assertEqual(
            len(self.service.list(kind="observation", status="captured")), 1
        )

    def test_same_batch_id_with_different_content_conflicts(self):
        self.service.submit_batch(self.actor, "morph", self._items("E-300"))
        altered = self._items("E-301")
        with self.assertRaises(ConflictError):
            self.service.submit_batch(self.actor, "morph", altered)
        # The altered attempt is not stored as a new batch.
        self.assertEqual(len(self.service.list_batches()), 1)

    def test_stale_base_version_keeps_entity_and_records_conflict(self):
        entity = self.service.create(self.actor, "observation", observation("E-400"))
        self.assertEqual(entity["version"], 1)

        stale = [
            {
                "op": "patch",
                "kind": "observation",
                "id": entity["id"],
                "base_version": 1,
                "data": {"location": "Ridge"},
            }
        ]
        first = self.service.submit_batch(self.actor, "stale-1", stale)
        self.assertEqual(first["status"], "applied")
        self.assertEqual(first["items"][0]["status"], "applied")

        # Server-side edit advances the version.
        self.service.patch_entity(
            self.actor, entity["id"], {"location": "Valley"}, base_version=2
        )

        # Replaying the offline batch now hits a stale base version.
        resent = self.service.submit_batch(self.actor, "stale-2", stale)
        self.assertEqual(resent["status"], "conflict")
        item = resent["items"][0]
        self.assertEqual(item["status"], "conflict")
        self.assertIn("version conflict", item["reason"])
        self.assertTrue(item["conflict_id"])

        untouched = self.service.get(entity["id"])
        self.assertEqual(untouched["data"]["location"], "Valley")
        self.assertEqual(untouched["version"], 3)

        conflicts = self.service.list_conflicts(status="pending")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["entity_id"], entity["id"])
        self.assertEqual(conflicts[0]["batch_id"], "stale-2")
        self.assertIn("version conflict", conflicts[0]["reason"])

    def test_records_survive_server_restart(self):
        result = self.service.submit_batch(self.actor, "persist", self._items("E-500"))
        entity_id = result["items"][0]["entity_id"]
        cursor = result["cursor"]

        # Simulate a process restart: a fresh repository over the same file.
        restarted_repo = SQLiteRepository(self.db_path)
        restarted = DomainService(restarted_repo, RuleEngine())

        replay = restarted.submit_batch(self.actor, "persist", self._items("E-500"))
        self.assertEqual(replay, result)
        self.assertEqual(replay["items"][0]["entity_id"], entity_id)
        self.assertEqual(restarted.get(entity_id)["data"]["event_id"], "E-500")
        self.assertEqual(restarted.changes(cursor=0)["next_cursor"], cursor)
        self.assertEqual(restarted.list_batches()[0]["batch_id"], "persist")

    def test_duplicate_create_in_batch_becomes_pending_conflict(self):
        first = self.service.submit_batch(
            self.actor, "d1", [{"op": "create", "kind": "observation",
                               "data": observation("E-600")}]
        )
        # Another batch legitimately carries the same event_id; the rule
        # engine rejects it and a pending conflict is recorded.
        second = self.service.submit_batch(
            self.actor, "d2", [{"op": "create", "kind": "observation",
                                "data": observation("E-600")}]
        )
        self.assertEqual(second["status"], "conflict")
        self.assertEqual(len(self.service.list_conflicts()), 1)
        self.assertEqual(first["items"][0]["entity_id"],
                         self.service.list(kind="observation")[0]["id"])


if __name__ == "__main__":
    unittest.main()
