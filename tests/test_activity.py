"""Live view contracts, isolated from production profiles and real workers."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from test_pi_manager import PiManagerTestCase, FakePiProcess, fake_popen_factory, TEST_THRESHOLDS, wait_until
from activity import ActivityRecorder, Projection, MAX_BYTES, snapshot_path, read_activity
from core import PiManager
from registry_db import Registry


def message_update(delta, kind="text_delta", **kw):
    return {"type": "message_update", "assistantMessageEvent": {
        "type": kind, "contentIndex": 0, "delta": delta, **kw}}


class ProjectionTests(unittest.TestCase):
    def test_real_pi_deltas_without_message_and_authoritative_end(self):
        p = Projection("pi-test")
        p.apply(message_update("Hel"), 1)
        p.apply(message_update("lo"), 2)
        self.assertEqual(p.data["text"], "Hello")
        p.apply(message_update("", "text_end", content="Hello!"), 3)
        p.apply({"type": "message_end", "message": {"role": "assistant", "content": [
            {"type": "thinking", "thinking": "private"}, {"type": "text", "text": "Hello!"}]}}, 4)
        self.assertEqual(p.data["text"], "Hello!")
        self.assertEqual([e["text"] for e in p.data["entries"]], ["Hello!"])
        self.assertNotIn("private", json.dumps(p.data))

    def test_cumulative_tool_output_replaces_instead_of_duplicating(self):
        p = Projection("pi-test")
        p.apply({"type": "tool_execution_start", "toolCallId": "t1", "toolName": "bash"}, 1)
        for value in ("one\n", "one\ntwo\n"):
            p.apply({"type": "tool_execution_update", "toolCallId": "t1", "partialResult": {
                "content": [{"type": "text", "text": value}]}}, 2)
        self.assertEqual(p.data["tool"]["text"], "one\ntwo\n")
        end = {"type": "tool_execution_end", "toolCallId": "t1", "result": {
            "content": [{"type": "text", "text": "one\ntwo\nthree"}]}}
        p.apply(end, 3)
        p.apply(end, 4)
        self.assertIsNone(p.data["tool"])
        self.assertEqual(p.data["tools_completed"], 1)
        self.assertEqual(p.data["entries"][0]["text"], "one\ntwo\nthree")

    def test_late_tool_end_and_thinking_do_not_replace_current_output(self):
        p = Projection("pi-test")
        p.apply({"type": "tool_execution_start", "toolCallId": "new", "toolName": "bash"}, 1)
        self.assertFalse(p.apply({"type": "tool_execution_end", "toolCallId": "old"}, 2))
        self.assertFalse(p.apply(message_update("secret thought", "thinking_delta"), 3))
        self.assertEqual(p.data["tool"]["id"], "new")
        self.assertNotIn("secret", json.dumps(p.data))


class SnapshotTests(unittest.TestCase):
    def test_background_write_is_bounded_private_and_redacted(self):
        with tempfile.TemporaryDirectory() as directory:
            recorder = ActivityRecorder(Path(directory) / "activity", interval=0.02,
                                        redact=lambda text: text.replace("secret", "[redacted]"))
            self.addCleanup(recorder.close)
            recorder.observe("../pi-test", message_update("secret"))
            path = snapshot_path(recorder.directory, "../pi-test")
            self.assertTrue(wait_until(path.exists))
            self.assertEqual(json.loads(path.read_text())["text"], "[redacted]")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            for i in range(20):
                recorder.observe("../pi-test", {"type": "message_end", "message": {
                    "role": "assistant", "content": "😀" * 50000}})
            recorder.close()
            self.assertLess(path.stat().st_size, MAX_BYTES)
            self.assertEqual(len(json.loads(path.read_text())["entries"]), 8)


class ActivityReadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.registry = Registry(hermes_home=self.tmp.name)
        self.addCleanup(self.registry.close)
        self.registry.create_task("pi-view", cwd=self.tmp.name, origin=json.dumps({
            "source": "desktop", "session_id": "parent", "hermes_home": self.tmp.name}))

    def test_read_does_not_mutate_registry_and_rejects_foreign_session(self):
        before = self.registry.get_task("pi-view")
        view = read_activity(self.home, "pi-view", "parent")
        self.assertEqual(view["task_id"], "pi-view")
        self.assertIsNone(view["activity"])
        self.assertNotIn("origin", view)
        with self.assertRaises(KeyError):
            read_activity(self.home, "pi-view", "different")
        self.assertEqual(self.registry.get_task("pi-view"), before)

    def test_compression_continuation_is_allowed_but_branch_is_not(self):
        with sqlite3.connect(self.home / "state.db") as conn:
            conn.execute("CREATE TABLE sessions (id TEXT, parent_session_id TEXT, ended_at REAL, end_reason TEXT, model_config TEXT, source TEXT)")
            conn.executemany("INSERT INTO sessions VALUES (?,?,?,?,?,?)", [
                ("parent", None, 1, "compression", "{}", "desktop"),
                ("tip", "parent", None, None, "{}", "desktop"),
                ("branch", "parent", None, None, '{"_branched_from":"parent"}', "desktop"),
            ])
        self.assertEqual(read_activity(self.home, "pi-view", "tip")["session_id"], "tip")
        with self.assertRaises(KeyError):
            read_activity(self.home, "pi-view", "branch")

    def test_missing_other_profile_does_not_create_a_database(self):
        other = self.home / "other"
        with self.assertRaises(sqlite3.OperationalError):
            read_activity(other, "pi-view", "parent")
        self.assertFalse(other.exists())

    def test_api_mount_has_no_manager_side_effects_and_is_no_store(self):
        try:
            from fastapi import FastAPI
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest("FastAPI/httpx supplied by the Hermes runtime")
        path = Path(__file__).resolve().parents[1] / "dashboard" / "plugin_api.py"
        spec = importlib.util.spec_from_file_location("pi_activity_test_api", path)
        api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api)
        app = FastAPI()
        app.include_router(api.router, prefix="/api/plugins/pi-manager")
        with patch.object(api, "_profile_home", return_value=self.home), TestClient(app) as client:
            response = client.get("/api/plugins/pi-manager/activity", params={"task_id": "pi-view", "session_id": "parent"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["cache-control"], "no-store")
            self.assertEqual(client.get("/api/plugins/pi-manager/activity", params={
                "task_id": "pi-view", "session_id": "foreign"}).status_code, 404)
            self.assertEqual(client.post("/api/plugins/pi-manager/activity").status_code, 405)


class ManagerActivityTests(PiManagerTestCase):
    def test_start_ack_supplies_desktop_card_without_polling_or_notifications(self):
        import tools
        from unittest.mock import Mock
        manager = Mock()
        manager.start_task.return_value = {"task_id": "pi-card", "execution_state": "STARTING"}
        with patch.object(tools, "get_manager", return_value=manager), patch.object(
                tools, "_capture_routing", return_value={"source": "desktop", "session_id": "parent"}):
            result = json.loads(tools.handle_pi_task({"prompt": "test", "cwd": str(self.cwd_dir)}))
        self.assertEqual(result["desktop_live_view"]["directive"], '::pi-live{task="pi-card"}')
        self.assertEqual(len(manager.mock_calls), 1)

    def test_gateway_ack_does_not_gain_a_desktop_directive(self):
        import tools
        from unittest.mock import Mock
        manager = Mock()
        manager.start_task.return_value = {"task_id": "pi-card"}
        with patch.object(tools, "get_manager", return_value=manager), patch.object(
                tools, "_capture_routing", return_value={"platform": "telegram", "session_key": "telegram:a"}):
            result = json.loads(tools.handle_pi_task({"prompt": "test", "cwd": str(self.cwd_dir)}))
        self.assertNotIn("desktop_live_view", result)

    def test_real_transport_events_reach_view_without_changing_settlement(self):
        recorder = ActivityRecorder(self.tmp / "activity", interval=0.01)
        process = FakePiProcess()
        manager = PiManager(self.registry, popen_factory=fake_popen_factory(process),
                            default_thresholds=TEST_THRESHOLDS, activity_recorder=recorder)
        self.addCleanup(manager.shutdown)
        task = manager.start_task(prompt="test", cwd=str(self.cwd_dir))["task_id"]
        self.assertTrue(wait_until(lambda: manager._rt(task) is not None))
        self.emit_and_sync(manager, process, task, message_update("live chunk"))
        path = snapshot_path(recorder.directory, task)
        self.assertTrue(wait_until(lambda: path.exists() and "live chunk" in path.read_text()))
        self.emit_and_sync(manager, process, task, {"type": "agent_settled"})
        self.assertEqual(self.registry.get_task(task)["execution_state"], "SETTLED")

    def test_observer_failure_does_not_break_task_or_replay_old_events(self):
        class Broken:
            def observe(self, *args):
                raise RuntimeError("display unavailable")
            def close(self):
                pass
        manager = PiManager(self.registry, activity_recorder=Broken())
        self.registry.create_task("pi-observe", cwd=str(self.cwd_dir))
        with self.assertLogs("core", level="WARNING"):
            manager._on_event("pi-observe", message_update("x"))
        self.assertEqual(self.registry.get_task("pi-observe")["last_event_type"], "message_update")
        with patch.object(manager._activity_recorder, "observe") as observe:
            manager._on_event("pi-observe", message_update("old"), source="recovery_replay")
            observe.assert_not_called()
