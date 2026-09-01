"""A finished delegate should announce itself.

Before this, a task settling was recorded and nothing more: on 2026-08-28 one
settled at 06:41:00 and was noticed at 06:47:22, when a human asked.

Since pi_wait was removed later that day, this notice is now the ONLY way a
caller learns a delegate finished — so these tests guard the single path, not
a convenience on top of a blocking alternative.
"""
from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from test_pi_manager import PiManagerTestCase, FakePiProcess, fake_popen_factory, TEST_THRESHOLDS  # type: ignore
from core import PiManager, VerifierSpec  # type: ignore
from registry_db import Registry  # type: ignore


class TestCompletionNotice(PiManagerTestCase):
    def _manager(self, notifier, process):
        return PiManager(
            registry=self.registry, popen_factory=fake_popen_factory(process),
            clock=self.clock, default_thresholds=TEST_THRESHOLDS, notifier=notifier,
        )

    def test_notifies_after_the_verifier_resolves(self):
        seen = []
        process = FakePiProcess()
        manager = self._manager(lambda t, p: seen.append((t, p)), process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=["true"], timeout_seconds=10.0))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})

        from test_pi_manager import wait_until  # type: ignore
        self.assertTrue(wait_until(lambda: bool(seen)), "no notice was delivered")
        tid, payload = seen[-1]
        self.assertEqual(tid, task_id)
        self.assertEqual(payload["execution_state"], "SETTLED")
        # The verdict must be resolved, never still PENDING: announcing "done"
        # without saying whether the gate passed is the confusion this plugin
        # keeps two separate axes to avoid.
        self.assertEqual(payload["verification_state"], "PASS")

    def test_ungated_task_is_announced_too(self):
        seen = []
        process = FakePiProcess()
        manager = self._manager(lambda t, p: seen.append((t, p)), process)
        task_id = self.start_and_boot(manager, process)   # no verifier
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})

        from test_pi_manager import wait_until  # type: ignore
        self.assertTrue(wait_until(lambda: bool(seen)),
                        "a read-only task must still announce itself")
        self.assertEqual(seen[-1][1]["verification_state"], "NOT_RUN")

    def test_a_raising_notifier_cannot_break_a_task(self):
        def explode(_t, _p):
            raise RuntimeError("notifier is broken")

        process = FakePiProcess()
        manager = self._manager(explode, process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertEqual(manager.status(task_id)["execution_state"], "SETTLED",
                         "a broken notifier must not disturb settlement")

        # The assertion above passed even while the handler was broken: state
        # is persisted BEFORE the notifier runs. core.py logged the swallowed
        # error with a `logger` that nothing defined, so the except block
        # raised NameError and the failure escaped _notify_done instead of
        # being dropped. Call it directly, where nothing can mask that.
        manager._notify_done(task_id)

    def test_no_notifier_configured_is_fine(self):
        process = FakePiProcess()
        manager = self._manager(None, process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertEqual(manager.status(task_id)["execution_state"], "SETTLED")




class TestCompletionViaOutbox(PiManagerTestCase):
    """The completion notice now rides the plugin's own durable outbox and
    the public send_message dispatch — never the private
    process_registry.completion_queue rail.
    """

    def setUp(self):
        super().setUp()
        from outbox import NotificationOutbox, OutboxWorker  # type: ignore
        import json as _json
        self._json = _json
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)
        self.origin = {
            "platform": "telegram",
            "chat_id": "-1001234567890",
            "thread_id": "3315",
            "session_key": "agent:main:telegram:group:-1001234567890:3315",
            "scope_id": "tenant-7",
        }

    def _manager(self, process):
        return PiManager(
            registry=self.registry, popen_factory=fake_popen_factory(process),
            clock=self.clock, default_thresholds=TEST_THRESHOLDS,
            outbox=self.outbox,
        )

    def test_completion_notice_carries_real_verdict_and_reaches_chat(self):
        process = FakePiProcess()
        manager = self._manager(process)
        task_id = self.start_and_boot(
            manager, process, origin=self.origin,
            verifier=VerifierSpec(argv=["true"], timeout_seconds=10.0))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        from test_pi_manager import wait_until  # type: ignore
        self.assertTrue(wait_until(
            lambda: self.registry.get_notification(f"term:{task_id}:verifier") is not None,
            timeout=10.0), "no verifier notification was enqueued")

        from outbox import OutboxWorker  # type: ignore

        delivery_calls = []

        def delivery(args, **kw):
            delivery_calls.append(args)
            return '{"sent": true}'

        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=self.clock()), 1)
        n = self.registry.get_notification(f"term:{task_id}:verifier")
        self.assertEqual(n["status"], "sent")
        self.assertEqual(n["target"], "telegram:-1001234567890:3315")
        self.assertIn("Weryfikacja: PASS", n["message"])
        self.assertIn("Pi zakończył zadanie", n["message"])
        self.assertNotIn("pi_digest", n["message"])
        self.assertNotIn("pi_status", n["message"])
        self.assertIn(task_id, n["message"])
        self.assertEqual(len(delivery_calls), 1)
        self.assertEqual(delivery_calls[0]["action"], "send")

    def test_failed_gate_is_reported_as_failed(self):
        process = FakePiProcess()
        manager = self._manager(process)
        task_id = self.start_and_boot(
            manager, process, origin=self.origin,
            verifier=VerifierSpec(argv=["false"], timeout_seconds=10.0))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        from test_pi_manager import wait_until  # type: ignore
        self.assertTrue(wait_until(
            lambda: any(n["kind"] == "verifier" for n in
                        self.registry.list_notifications(task_id=task_id))))
        n = self.registry.get_notification(f"term:{task_id}:verifier")
        self.assertIn("Weryfikacja: FAIL", n["message"])
        self.assertNotIn("Weryfikacja: PASS", n["message"])

    def test_ungated_task_says_nothing_was_checked(self):
        process = FakePiProcess()
        manager = self._manager(process)
        task_id = self.start_and_boot(manager, process, origin=self.origin)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        from test_pi_manager import wait_until  # type: ignore
        self.assertTrue(wait_until(
            lambda: any(n["kind"] == "settled" for n in
                        self.registry.list_notifications(task_id=task_id))))
        n = self.registry.get_notification(f"term:{task_id}:settled")
        self.assertIn("Weryfikacja: nie uruchomiono", n["message"])


class TestOldPrivateRailIsGone(unittest.TestCase):
    """Regression guard: the delivery path must not touch
    tools.process_registry.completion_queue, a fake async_delegation event,
    inject_message, or the Telegram Bot API — in source or in behavior.
    """

    def setUp(self):
        import tools  # type: ignore
        self.tools = tools

    def test_private_rail_symbols_are_gone(self):
        self.assertFalse(hasattr(self.tools, "_deliver_notice"))
        self.assertFalse(hasattr(self.tools, "_deliver_progress"))
        self.assertFalse(hasattr(self.tools, "_routing"))
        self.assertFalse(hasattr(self.tools, "_format_notice"))

    def test_source_never_references_the_old_rails(self):
        """Code-level guard (AST, not prose): the old private rails are gone
        from every plugin module, and the ONLY ``inject_message`` CALL in
        the codebase is the user-approved terminal wake in
        ``wake_worker.py``. Docstrings and comments may explain the wake
        path — that is what this very test documents.
        """
        import ast
        from pathlib import Path
        plugin_dir = Path(self.tools.__file__).resolve().parent
        forbidden_names = {"completion_queue", "async_delegation",
                           "dispatch_tool", "send_message_bridge"}
        inject_users: set = set()
        for name in sorted(p.name for p in plugin_dir.glob("*.py")):
            tree = ast.parse((plugin_dir / name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute):
                    if node.attr in forbidden_names:
                        self.fail(f"{name} still references {node.attr!r} in code")
                    if node.attr == "inject_message":
                        inject_users.add(name)
                elif isinstance(node, ast.Name):
                    if node.id in forbidden_names:
                        self.fail(f"{name} still references {node.id!r} in code")
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    for alias in node.names:
                        if alias.name.split(".")[-1] in forbidden_names:
                            self.fail(
                                f"{name} still imports {alias.name!r}")
        self.assertEqual(inject_users, {"wake_worker.py"},
                         "inject_message may only be CALLED by the "
                         "user-approved terminal wake worker")
        self.assertFalse((plugin_dir / "send_message_bridge.py").exists(),
                          "the removed bridge module must stay removed")

    def test_delivery_never_imports_process_registry(self):
        import sys
        import types
        touched = []

        class _Sentinel:
            @property
            def completion_queue(self):
                touched.append(1)
                raise AssertionError("completion_queue must never be read")

        fake = types.ModuleType("tools.process_registry")
        fake.process_registry = _Sentinel()
        saved = sys.modules.get("tools.process_registry")
        sys.modules["tools.process_registry"] = fake
        self.addCleanup(
            lambda: sys.modules.__setitem__("tools.process_registry", saved)
            if saved is not None else sys.modules.pop("tools.process_registry", None))

        # Stub the native host tool at its import boundary so the REAL
        # delivery path (worker -> plugin-local host adapter ->
        # tools.send_message_tool) runs here without the hermes tree.
        fake_host = types.ModuleType("tools.send_message_tool")
        host_calls = []

        def fake_send_message_tool(args, **kw):
            host_calls.append(dict(args))
            return '{"sent": true}'

        fake_host.send_message_tool = fake_send_message_tool
        saved_host = sys.modules.get("tools.send_message_tool")
        sys.modules["tools.send_message_tool"] = fake_host
        self.addCleanup(
            lambda: sys.modules.__setitem__("tools.send_message_tool", saved_host)
            if saved_host is not None else sys.modules.pop("tools.send_message_tool", None))

        from outbox import NotificationOutbox, OutboxWorker  # type: ignore
        tmp = Path(tempfile.mkdtemp(prefix="pi-manager-rail-"))
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        registry = Registry(db_path=tmp / "registry.sqlite3")
        self.addCleanup(registry.close)
        outbox = NotificationOutbox(registry)
        import json as _json
        registry.create_task(
            task_id="pi-rail", execution_state="SETTLED",
            origin=_json.dumps({"platform": "telegram", "chat_id": "424242"}))
        outbox.enqueue("pi-rail", "settled", "body")

        # Default deliver: the plugin-local host adapter, not an injected
        # fake — this pins the production delivery shape.
        worker = OutboxWorker(outbox)
        worker.run_once()
        self.assertEqual(registry.get_notification("term:pi-rail:settled")["status"],
                         "sent")
        self.assertEqual(host_calls, [{"action": "send",
                                       "target": "telegram:424242",
                                       "message": "body"}])
        self.assertEqual(touched, [], "the private queue must never be read")


if __name__ == "__main__":
    unittest.main()
