import json
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def observation(event_id="E-1"):
    return {
        "event_id": event_id,
        "species": "deer",
        "location": "North",
        "observed_at": "2026-04-01",
        "lat": 40.0,
        "lon": 116.0,
    }


class HttpBatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        repo = SQLiteRepository(Path(cls.tmp.name) / "http.db")
        service = DomainService(repo, RuleEngine())
        static_dir = Path(__file__).resolve().parent.parent / "static"
        cls.server = create_server("127.0.0.1", 0, service, RuleEngine(), str(static_dir))
        cls.port = cls.server.server_address[1]
        import threading

        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def _url(self, path):
        return "http://127.0.0.1:%d%s" % (self.port, path)

    def _post(self, path, payload):
        request = urllib.request.Request(
            self._url(path),
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-User-Id": "field-1",
                "X-Role": "field",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def _get(self, path):
        with urllib.request.urlopen(self._url(path)) as response:
            return response.status, json.loads(response.read().decode("utf-8"))

    def test_batch_roundtrip_over_http(self):
        batch = {
            "batch_id": "http-1",
            "items": [{"op": "create", "kind": "observation", "data": observation("E-900")}],
        }
        status, first = self._post("/api/batches", batch)
        self.assertEqual(status, 200)
        self.assertEqual(first["status"], "applied")
        self.assertGreaterEqual(first["cursor"], 1)

        # Identical resend replays the stored result without new entities.
        status, replay = self._post("/api/batches", batch)
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        _, listing = self._get("/api/observation")
        self.assertEqual(len(listing["items"]), 1)

        # Same batch id with altered content is rejected as a conflict.
        altered = {
            "batch_id": "http-1",
            "items": [{"op": "create", "kind": "observation", "data": observation("E-901")}],
        }
        status, conflict = self._post("/api/batches", altered)
        self.assertEqual(status, 409)
        self.assertIn("different content", conflict["error"])

        # Batches and changes are queryable with the cursor.
        _, batches = self._get("/api/batches")
        self.assertEqual(batches["items"][0]["batch_id"], "http-1")
        _, feed = self._get("/api/changes?cursor=0")
        self.assertEqual(feed["next_cursor"], first["cursor"])
        _, empty = self._get("/api/changes?cursor=%d" % first["cursor"])
        self.assertEqual(empty["changes"], [])

    def test_stylesheet_and_index_are_served(self):
        with urllib.request.urlopen(self._url("/static/styles.css")) as response:
            self.assertEqual(response.status, 200)
            self.assertIn("text/css", response.headers["Content-Type"])
            self.assertIn("badge", response.read().decode("utf-8"))
        with urllib.request.urlopen(self._url("/")) as response:
            html = response.read().decode("utf-8")
            self.assertIn("/static/styles.css", html)
            self.assertIn("/api/batches", html)


if __name__ == "__main__":
    unittest.main()
