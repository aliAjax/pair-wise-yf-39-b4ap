import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService

OBSERVATION = {
    "event_id": "E-1",
    "species": "deer",
    "location": "North",
    "observed_at": "2026-04-01",
    "lat": 40.0,
    "lon": 116.0,
}


def make_service(db_path):
    repo = SQLiteRepository(db_path)
    return DomainService(repo, RuleEngine())


def create_op(op_id="c1", **overrides):
    data = dict(OBSERVATION)
    data.update(overrides)
    return {"op": "create", "kind": "observation", "client_op_id": op_id, "data": data}


class SyncBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"
        self.service = make_service(self.db_path)
        self.actor = Actor("field-1", "field")
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def test_batch_applies_and_returns_cursor(self):
        result = self.service.submit_batch(
            self.actor, {"batch_id": "B-1", "operations": [create_op()]}
        )
        self.assertEqual(result["status"], "applied")
        self.assertFalse(result["replayed"])
        self.assertGreater(result["cursor"], 0)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(len(self.service.list("observation")), 1)

    def test_replay_returns_first_result_without_duplicates(self):
        batch = {"batch_id": "B-1", "operations": [create_op()]}
        first = self.service.submit_batch(self.actor, batch)
        replay = self.service.submit_batch(self.actor, batch)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["cursor"], first["cursor"])
        self.assertEqual(replay["results"], first["results"])
        self.assertEqual(len(self.service.list("observation")), 1)

    def test_same_batch_id_with_different_content_conflicts(self):
        self.service.submit_batch(
            self.actor, {"batch_id": "B-1", "operations": [create_op()]}
        )
        changed = {"batch_id": "B-1", "operations": [create_op(species="boar")]}
        with self.assertRaises(ConflictError):
            self.service.submit_batch(self.actor, changed)

    def test_stale_base_version_keeps_entity_and_records_conflict(self):
        created = self.service.create(self.admin, "observation", dict(OBSERVATION))
        self.service.transition(
            self.admin,
            created["id"],
            "submit",
            {"location": "North", "observed_at": "2026-04-01"},
        )
        batch = {
            "batch_id": "B-2",
            "operations": [
                {
                    "op": "transition",
                    "entity_id": created["id"],
                    "action": "reject",
                    "base_version": 1,
                    "data": {"reason": "outdated edit"},
                }
            ],
        }
        result = self.service.submit_batch(self.admin, batch)
        self.assertEqual(result["status"], "failed")
        entity = self.service.get(created["id"])
        self.assertEqual(entity["status"], "submitted")
        self.assertEqual(entity["version"], 2)
        conflicts = self.service.list_conflicts(status="pending")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["batch_id"], "B-2")
        self.assertEqual(conflicts[0]["entity_id"], created["id"])
        self.assertIn("stale base version", conflicts[0]["reason"])

    def test_partial_batch_applies_valid_ops_only(self):
        created = self.service.create(self.admin, "observation", dict(OBSERVATION))
        batch = {
            "batch_id": "B-3",
            "operations": [
                create_op(op_id="c2", event_id="E-2", observed_at="2026-04-02"),
                {
                    "op": "transition",
                    "entity_id": created["id"],
                    "action": "submit",
                    "base_version": 99,
                    "data": {"location": "North", "observed_at": "2026-04-01"},
                },
            ],
        }
        result = self.service.submit_batch(self.admin, batch)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["applied"], 1)
        self.assertEqual(result["conflicts"], 1)
        self.assertEqual(len(self.service.list("observation")), 2)
        self.assertEqual(self.service.get(created["id"])["status"], "captured")

    def test_changes_feed_uses_incrementing_cursor(self):
        first = self.service.submit_batch(
            self.actor, {"batch_id": "B-1", "operations": [create_op()]}
        )
        feed = self.service.sync_changes(0)
        self.assertEqual(feed["cursor"], first["cursor"])
        self.assertEqual(len(feed["items"]), 1)
        self.assertEqual(feed["items"][0]["batch_id"], "B-1")
        second = self.service.submit_batch(
            self.actor,
            {
                "batch_id": "B-2",
                "operations": [create_op(op_id="c2", event_id="E-2", observed_at="2026-04-02")],
            },
        )
        self.assertGreater(second["cursor"], first["cursor"])
        delta = self.service.sync_changes(first["cursor"])
        self.assertEqual(delta["cursor"], second["cursor"])
        self.assertEqual(len(delta["items"]), 1)
        self.assertEqual(delta["items"][0]["batch_id"], "B-2")

    def test_records_survive_restart(self):
        first = self.service.submit_batch(
            self.actor, {"batch_id": "B-1", "operations": [create_op()]}
        )
        stale = self.service.create(self.admin, "observation", dict(OBSERVATION, event_id="E-9"))
        self.service.submit_batch(
            self.admin,
            {
                "batch_id": "B-2",
                "operations": [
                    {
                        "op": "transition",
                        "entity_id": stale["id"],
                        "action": "submit",
                        "base_version": 7,
                        "data": {"location": "N", "observed_at": "2026-04-01"},
                    }
                ],
            },
        )
        restarted = make_service(self.db_path)
        self.assertEqual(len(restarted.list("observation")), 2)
        batches = restarted.list_batches()
        self.assertEqual([item["batch_id"] for item in batches], ["B-1", "B-2"])
        self.assertEqual(len(restarted.list_conflicts(status="pending")), 1)
        replay = restarted.submit_batch(
            self.actor, {"batch_id": "B-1", "operations": [create_op()]}
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["cursor"], first["cursor"])
        followup = restarted.submit_batch(
            self.actor,
            {
                "batch_id": "B-3",
                "operations": [create_op(op_id="c3", event_id="E-3", observed_at="2026-04-03")],
            },
        )
        self.assertGreater(followup["cursor"], first["cursor"])


class SyncHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        service = make_service(Path(self.tmp.name) / "test.db")
        static_dir = Path(__file__).resolve().parent.parent / "static"
        self.server = create_server("127.0.0.1", 0, service, RuleEngine(), str(static_dir))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def _request(self, method, path, body=None, raw=False):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        payload = json.dumps(body) if body is not None else None
        headers = {"X-User-Id": "field-1", "X-Role": "field"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        if raw:
            return response.status, response.getheader("Content-Type"), data
        return response.status, json.loads(data.decode("utf-8"))

    def test_batch_roundtrip_and_conflict_over_http(self):
        batch = {"batch_id": "B-1", "operations": [create_op()]}
        status, data = self._request("POST", "/api/sync/batches", batch)
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "applied")
        status, replay = self._request("POST", "/api/sync/batches", batch)
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        changed = {"batch_id": "B-1", "operations": [create_op(species="boar")]}
        status, data = self._request("POST", "/api/sync/batches", changed)
        self.assertEqual(status, 409)
        status, data = self._request("GET", "/api/sync/batches")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["items"]), 1)
        status, data = self._request("GET", "/api/sync/changes?since=0")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["items"]), 1)
        status, data = self._request("GET", "/api/sync/conflicts")
        self.assertEqual(status, 200)
        self.assertEqual(data["items"], [])

    def test_static_assets_served(self):
        status, content_type, data = self._request("GET", "/static/style.css", raw=True)
        self.assertEqual(status, 200)
        self.assertIn("text/css", content_type)
        self.assertIn(b"status-applied", data)
        status, content_type, data = self._request("GET", "/", raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b"/static/style.css", data)
        status, _, _ = self._request("GET", "/static/../app.py", raw=True)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
