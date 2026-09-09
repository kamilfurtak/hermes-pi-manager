"""Passive CLI delivery: real prompt_toolkit loop, isolated DB, no model."""
import json
import queue
import threading
from types import SimpleNamespace
from unittest.mock import patch

from test_outbox import OutboxTestCase, FakeDelivery
from outbox import OutboxWorker
import cli_host


class TestCLIDelivery(OutboxTestCase):
    def setUp(self):
        super().setUp()
        try:
            from prompt_toolkit.application import Application
            from prompt_toolkit.input import create_pipe_input
            from prompt_toolkit.output import DummyOutput
            from prompt_toolkit.layout import Layout
            from prompt_toolkit.widgets import TextArea
        except ImportError:
            self.skipTest("prompt_toolkit required for native CLI integration")
        self.pipe_context = create_pipe_input()
        self.pipe = self.pipe_context.__enter__()
        self.addCleanup(self.pipe_context.__exit__, None, None, None)
        self.app = Application(layout=Layout(TextArea()), input=self.pipe, output=DummyOutput())
        self.prints = []
        self.cli = SimpleNamespace(session_id="session-a", _app=self.app, _agent_running=False,
                                   _pending_input=queue.Queue(), _interrupt_queue=queue.Queue(),
                                   _console_print=lambda *a, **kw: self.prints.append((a, kw)))
        self.manager = SimpleNamespace(_cli_ref=self.cli)
        self.saved = (cli_host._manager, cli_host._runtime_id)
        self.addCleanup(self.restore)
        cli_host.bind(SimpleNamespace(_manager=self.manager), "runtime-a")
        self.ready = threading.Event()
        self.thread = threading.Thread(target=lambda: self.app.run(pre_run=self.ready.set), daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_app)
        self.assertTrue(self.ready.wait(3))
        self.origin = dict(cli_host.capture({}), host_runtime_id="runtime-a")
        self.delivery = FakeDelivery()
        self.worker = OutboxWorker(self.outbox, deliver=self.delivery, now_fn=self.clock)

    def restore(self):
        cli_host._manager, cli_host._runtime_id = self.saved

    def stop_app(self):
        self.app.loop.call_soon_threadsafe(self.app.exit)
        self.thread.join(3)

    def enqueue(self, task_id="pi-cli", origin=None, kind="progress", message="Pi: trwa analiza"):
        self.registry.create_task(task_id, origin=json.dumps(origin or self.origin))
        return self.outbox.enqueue(task_id, kind, message)

    def test_notice_prints_without_model_input_and_retry_deduplicates(self):
        nid = self.enqueue(message="Pi [bold]literal[/bold] \x1b\x07\rstatus\u202e")
        self.app.current_buffer.text = "unfinished human input"
        self.assertEqual(self.worker.run_once(), 1)
        self.assertEqual(self.registry.get_notification(nid)["status"], "sent")
        self.assertEqual(len(self.prints), 1)
        text = self.prints[0][0][0]
        self.assertIn("[bold]literal[/bold]", text)
        for char in ("\x1b", "\x07", "\r", "\u202e"):
            self.assertNotIn(char, text)
        self.assertEqual(self.prints[0][1], {"markup": False, "highlight": False})
        self.assertEqual(self.app.current_buffer.text, "unfinished human input")
        self.assertTrue(self.cli._pending_input.empty())
        self.assertTrue(self.cli._interrupt_queue.empty())
        self.assertEqual(self.delivery.calls, [])
        self.assertTrue(cli_host.emit_status(self.origin, "again", nid)["duplicate"])
        self.assertEqual(len(self.prints), 1)

    def test_foreign_process_cannot_steal_notice_then_owner_delivers(self):
        nid = self.enqueue()
        cli_host.bind(SimpleNamespace(_manager=self.manager), "runtime-b")
        self.enqueue("pi-telegram", origin={"platform": "telegram", "chat_id": "123"})
        self.assertEqual(self.worker.run_once(), 1)
        self.assertEqual(self.prints, [])
        self.assertEqual(self.delivery.calls[0]["target"], "telegram:123")
        self.assertEqual(self.registry.get_notification(nid)["attempts"], 0)
        cli_host.bind(SimpleNamespace(_manager=self.manager), "runtime-a")
        self.assertEqual(self.worker.run_once(), 1)

    def test_new_session_and_busy_parent_defer_without_spending_attempts(self):
        nid = self.enqueue()
        self.cli.session_id = "new-session"
        self.assertEqual(self.worker.run_once(), 0)
        self.cli.session_id = "session-a"
        self.cli._agent_running = True
        self.assertEqual(self.worker.run_once(), 0)
        self.assertEqual(self.registry.get_notification(nid)["attempts"], 0)
        self.cli._agent_running = False
        self.assertEqual(self.worker.run_once(), 1)

    def test_compressed_session_receives_notice(self):
        self.enqueue()
        self.cli.session_id = "tip"
        self.cli._session_db = SimpleNamespace(resolve_resume_session_id=lambda sid: "tip")
        self.assertEqual(self.worker.run_once(), 1)

    def test_recheck_on_ui_loop_blocks_session_switch_after_claim(self):
        nid = self.enqueue()
        row = self.outbox.claim("test", 30)[0]
        from prompt_toolkit.application import run_in_terminal

        async def switch_before_render(display):
            self.cli.session_id = "new-session"
            return await run_in_terminal(display)

        with patch("prompt_toolkit.application.run_in_terminal", switch_before_render):
            self.worker._deliver_one(row, self.clock())
        self.assertEqual(self.prints, [])
        self.assertNotEqual(self.registry.get_notification(nid)["status"], "sent")

    def test_telegram_keeps_native_messaging_path_even_with_cli_reference(self):
        origin = {"platform": "telegram", "chat_id": "123", "thread_id": "4",
                  "session_key": "telegram-session", "host_runtime_id": "runtime-a"}
        self.assertEqual(cli_host.capture(origin), {})
        self.enqueue(origin=origin)
        self.worker.run_once()
        self.assertEqual(self.prints, [])
        self.assertEqual(self.delivery.calls[0]["target"], "telegram:123:4")

    def test_desktop_never_acquires_cli_destination(self):
        for origin in ({"source": "desktop"}, {"ui_session_id": "tab"}, {"source": "tui"}):
            self.assertEqual(cli_host.capture(origin), {})
            self.assertFalse(cli_host.is_cli(dict(origin, cli_session_id="session-a")))

    def test_missing_cli_at_registration_can_become_available_later(self):
        self.manager._cli_ref = None
        self.assertEqual(cli_host.capture({}), {})
        self.manager._cli_ref = self.cli
        self.assertEqual(cli_host.capture({}), {"cli_session_id": "session-a"})
