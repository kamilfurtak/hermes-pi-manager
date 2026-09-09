"""Passive CLI delivery: real prompt_toolkit loop, isolated DB, no model."""
import json
import asyncio
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

    def test_monitor_progress_does_not_spam_scrollback_but_failure_still_prints(self):
        nid = self.enqueue()
        monitor = SimpleNamespace(cli=self.cli, closed=False, rows={'pi-cli': {}})
        with patch.object(cli_host, '_monitor', monitor):
            self.worker.run_once()
            self.assertEqual(self.prints, [])
            self.assertEqual(self.registry.get_notification(nid)['status'], 'sent')
            self.outbox.enqueue('pi-cli', 'failed', 'Pi: błąd procesu')
            self.worker.run_once()
            self.assertIn('błąd procesu', self.prints[0][0][0])

    def test_quiet_start_attaches_on_ui_loop_once_and_releases_after_the_dispatch_turn(self):
        from unittest.mock import Mock
        from cli_startup import ACKNOWLEDGEMENT
        self.cli.agent = SimpleNamespace(stream_delta_callback=None, _stream_callback=None)
        self.cli._agent_running = True
        self.cli._chat_render_turn = renderer = Mock()
        self.cli._flush_stream = Mock()
        monitor = SimpleNamespace(cli=self.cli, closed=False)
        with patch.object(cli_host, '_monitor', monitor), patch.object(cli_host, '_quiet_start', None):
            view = cli_host.startup_view(self.origin)
            self.assertTrue(view['quiet_start'])
            self.assertEqual(view['reply'], ACKNOWLEDGEMENT)
            installed = self.cli._chat_render_turn
            self.assertEqual(cli_host.startup_view(self.origin), view)
            self.assertIs(self.cli._chat_render_turn, installed, 'parallel starts must not stack filters')
            quiet = cli_host._quiet_start
            try:
                async def finish():
                    self.cli._chat_render_turn(SimpleNamespace(result={
                        'completed': True, 'final_response': view['reply']}), None, None)
                asyncio.run_coroutine_threadsafe(finish(), self.app.loop).result(3)
                self.assertTrue(quiet.closed)
                self.assertIsNone(cli_host._quiet_start, 'do not retain the finished parent agent')
                self.assertIs(self.cli._chat_render_turn, renderer)
            finally:
                quiet.close()

    def test_quiet_start_falls_back_when_the_host_cannot_render_it(self):
        monitor = SimpleNamespace(cli=self.cli, closed=False)
        with patch.object(cli_host, '_monitor', monitor), patch.object(cli_host, '_quiet_start', None):
            for origin in (self.origin, dict(self.origin, platform='telegram'),
                           dict(self.origin, cli_session_id='foreign')):
                view = cli_host.startup_view(origin)
                self.assertNotIn('quiet_start', view)
                self.assertNotIn('reply', view)
                self.assertIsNone(cli_host._quiet_start)

    def test_timed_out_startup_attachment_does_not_leak_into_the_next_turn(self):
        from unittest.mock import Mock
        from concurrent.futures import Future, TimeoutError
        self.cli.agent = SimpleNamespace(stream_delta_callback=None, _stream_callback=None)
        self.cli._chat_render_turn = renderer = Mock()
        self.cli._flush_stream = Mock()
        pending = []
        ready = Future()
        with patch.object(cli_host, '_monitor', SimpleNamespace(cli=self.cli, closed=False)), patch.object(
                cli_host, '_quiet_start', None), patch.object(self.app.loop, 'call_soon_threadsafe', pending.append), patch(
                'concurrent.futures.Future', return_value=ready), patch.object(ready, 'result', side_effect=TimeoutError):
            view = cli_host.startup_view(self.origin)
            self.assertNotIn('quiet_start', view)
            self.assertTrue(ready.cancelled())
            pending[0]()
            self.assertIsNone(cli_host._quiet_start)
            self.assertIs(self.cli._chat_render_turn, renderer)

    def test_wake_uses_fifo_preserves_draft_and_never_interrupts_busy_parent(self):
        consumed = []
        self.cli._tui_process_one_input = consumed.append
        self.app.current_buffer.text = 'unfinished draft'
        self.cli._agent_running = True
        self.assertFalse(cli_host.can_wake(self.origin))
        self.assertEqual(cli_host.deliver_wake(self.origin, 'result'), 'unavailable')
        self.cli._agent_running = False
        self.assertEqual(cli_host.deliver_wake(self.origin, 'result'), 'accepted')
        self.assertFalse(cli_host.can_wake(self.origin))
        self.cli._tui_process_one_input(self.cli._pending_input.get_nowait())
        self.assertEqual(consumed, ['result'])
        self.assertTrue(self.cli._interrupt_queue.empty())
        self.assertEqual(self.app.current_buffer.text, 'unfinished draft')

    def test_wake_waits_for_native_inspector_and_drops_if_session_switches_before_consumption(self):
        consumed = []
        self.cli._tui_process_one_input = consumed.append
        self.cli._subagent_monitor = SimpleNamespace(opening=True)
        self.assertFalse(cli_host.can_wake(self.origin))
        self.cli._subagent_monitor.opening = False
        self.assertEqual(cli_host.deliver_wake(self.origin, 'old result'), 'accepted')
        self.cli.session_id = 'new-session'
        self.cli._tui_process_one_input(self.cli._pending_input.get_nowait())
        self.assertEqual(consumed, [])
        self.cli._tui_process_one_input('human message')
        self.assertEqual(consumed, ['human message'])

    def test_terminal_notice_waits_for_inspector_without_poisoning_future_prints(self):
        from prompt_toolkit.application import in_terminal, run_in_terminal
        from prompt_toolkit.application.current import set_app
        from test_pi_manager import wait_until

        opened = threading.Event()
        release = None
        self.cli._subagent_monitor = SimpleNamespace(opening=True, app=None)

        async def inspect():
            nonlocal release
            with set_app(self.app):
                async with in_terminal():
                    release = asyncio.Event()
                    self.cli._subagent_monitor.app = object()
                    opened.set()
                    await release.wait()
            self.cli._subagent_monitor.app = None
            self.cli._subagent_monitor.opening = False

        inspector = asyncio.run_coroutine_threadsafe(inspect(), self.app.loop)
        self.assertTrue(opened.wait(3))
        owner = self.app._running_in_terminal_f
        nid = self.enqueue(kind='verifier', message='Pi: testy PASS')
        try:
            self.assertEqual(self.worker.run_once(), 0)
            self.assertEqual(self.registry.get_notification(nid)['attempts'], 0)
            self.assertIs(self.app._running_in_terminal_f, owner)
            self.assertFalse(owner.cancelled())
        finally:
            self.app.loop.call_soon_threadsafe(release.set)
            inspector.result(3)
        self.assertEqual(self.worker.run_once(), 1)
        self.assertIn('testy PASS', self.prints[0][0][0])

        async def next_response():
            with set_app(self.app):
                await run_in_terminal(lambda: self.cli._console_print('5 × 5 = 25'))
        asyncio.run_coroutine_threadsafe(next_response(), self.app.loop).result(3)
        self.assertTrue(wait_until(lambda: any('25' in str(p) for p in self.prints)))

    def test_timeout_during_terminal_ownership_race_does_not_cancel_the_print_queue(self):
        from prompt_toolkit.application import run_in_terminal
        from test_pi_manager import wait_until
        from concurrent.futures import TimeoutError

        previous = None
        nid = self.enqueue(kind='verifier', message='Pi: recovered queue')

        def race(display):
            nonlocal previous
            # A competing terminal owner arrives after our UI-loop check.
            previous = self.app.loop.create_future()
            self.app._running_in_terminal_f = previous
            return run_in_terminal(display)

        with patch('prompt_toolkit.application.run_in_terminal', race):
            with self.assertRaises(TimeoutError):
                cli_host.emit_status(self.origin, 'Pi: recovered queue', nid, kind='verifier')
        try:
            self.assertFalse(previous.cancelled(), 'timeout must not cancel the prior terminal owner')
            self.assertFalse(cli_host.available(self.origin), 'do not enqueue retries behind an active terminal owner')
        finally:
            if not previous.done():
                self.app.loop.call_soon_threadsafe(previous.set_result, None)
        self.assertTrue(wait_until(lambda: len(self.prints) == 1))
        self.assertTrue(wait_until(lambda: cli_host.available(self.origin)))
        self.assertTrue(cli_host.emit_status(self.origin, 'duplicate', nid)['duplicate'])
        cli_host.emit_status(self.origin, 'NEXT RESPONSE VISIBLE', 'next')
        self.assertEqual(len(self.prints), 2)
        self.assertIn('NEXT RESPONSE VISIBLE', self.prints[-1][0][0])

    def test_wake_worker_defers_busy_without_attempts_then_accepts_once(self):
        from core import HOST_RUNTIME_ID
        from wake_worker import TerminalWakeWorker
        cli_host.bind(SimpleNamespace(_manager=self.manager), HOST_RUNTIME_ID)
        origin = dict(self.origin, host_runtime_id=HOST_RUNTIME_ID)
        self.registry.create_task('pi-wake-cli', origin=json.dumps(origin), execution_state='SETTLED',
                                  verification_state='PASS', continuation_enabled=1, wake_state='pending',
                                  wake_requested_at=self.clock())
        self.cli._tui_process_one_input = lambda value: None
        ctx = SimpleNamespace(inject_message=lambda *a, **kw: self.fail('must not inject an interrupt'))
        worker = TerminalWakeWorker(self.registry, ctx, now_fn=self.clock)
        self.cli._agent_running = True
        self.assertEqual(worker.run_once(), 0)
        self.assertEqual(self.registry.get_task('pi-wake-cli')['wake_attempts'], 0)
        self.cli._agent_running = False
        self.assertEqual(worker.run_once(), 1)
        self.assertEqual(worker.run_once(), 0)
        self.assertEqual(self.registry.get_task('pi-wake-cli')['wake_state'], 'accepted')
        self.assertEqual(self.cli._pending_input.qsize(), 1)
        self.assertTrue(self.cli._interrupt_queue.empty())

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

    def test_legacy_workers_cannot_claim_cli_rows_even_after_retry_or_expiry(self):
        import sqlite3
        nid = self.enqueue()
        legacy = sqlite3.connect(self.registry.path)
        self.addCleanup(legacy.close)

        def legacy_tick():
            # Exact predicates used by pre-CLI Registry.claim_notifications.
            legacy.execute("UPDATE notifications SET status='pending', lease_until=NULL, "
                           "worker_id=NULL WHERE status='leased' AND lease_until <= ?", (self.clock() + 100,))
            rows = legacy.execute("SELECT notification_id FROM notifications WHERE status='pending' "
                                  "AND (next_retry_at IS NULL OR next_retry_at <= ?)",
                                  (self.clock() + 100,)).fetchall()
            legacy.commit()
            self.assertEqual(rows, [])

        legacy_tick()
        row = self.outbox.claim("owner", 1)[0]
        self.assertEqual(row["status"], "cli_leased")
        legacy_tick()
        self.registry.requeue_expired_leases(self.clock() + 2)
        self.assertEqual(self.registry.get_notification(nid)["status"], "cli_pending")
        legacy_tick()
        row = self.outbox.claim("owner", 1)[0]
        self.outbox.mark_failed(row, "temporary renderer failure", transient=True)
        self.assertEqual(self.registry.get_notification(nid)["status"], "cli_pending")
        legacy_tick()
        self.assertEqual(self.worker.run_once(now=self.clock() + 100), 1)
        self.assertEqual(self.registry.get_notification(nid)["status"], "sent")

    def test_desktop_never_acquires_cli_destination(self):
        for origin in ({"source": "desktop"}, {"ui_session_id": "tab"}, {"source": "tui"}):
            self.assertEqual(cli_host.capture(origin), {})
            self.assertFalse(cli_host.is_cli(dict(origin, cli_session_id="session-a")))

    def test_missing_cli_at_registration_can_become_available_later(self):
        self.manager._cli_ref = None
        self.assertEqual(cli_host.capture({}), {})
        self.manager._cli_ref = self.cli
        self.assertEqual(cli_host.capture({}), {"cli_session_id": "session-a"})
