"""Ordinary Desktop progress and terminal turns; no real model or messaging."""
import json
import os
import threading
from contextlib import nullcontext
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from test_terminal_wake import _OutboxManagerCase, FakePluginContext, ORIGIN
from wake_worker import TerminalWakeWorker
from outbox import OutboxWorker
import desktop_host


class TestDesktopDelivery(_OutboxManagerCase):
    def setUp(self):
        super().setUp()
        self.origin = {"source": "tui", "session_key": "session-a",
                       "session_id": "session-a", "ui_session_id": "tab-a"}
        self.frames, self.turns = [], []
        self.host = ModuleType("tui_gateway.server")
        self.host._sessions_lock = threading.RLock()
        self.host._sessions = {sid: {
            "session_key": key, "agent": object(), "history_lock": threading.Lock(),
            "running": False, "active_session_lease": SimpleNamespace(released=False),
        } for sid, key in (("tab-a", "session-a"), ("tab-b", "session-b"))}
        self.host._get_db = lambda: SimpleNamespace(resolve_resume_session_id=lambda key: key)
        self.host._hermes_home = "/test/hermes"
        self.host._session_home = lambda session: session.get("profile_home") or self.host._hermes_home
        self.host._session_db = lambda session: nullcontext(self.host._get_db())
        self.bound_home = None
        def set_home(home):
            previous, self.bound_home = self.bound_home, home
            return previous
        self.host.set_hermes_home_override = set_home
        self.host.reset_hermes_home_override = lambda previous: setattr(self, "bound_home", previous)
        self.host._event_frame = lambda event, sid, data: (event, sid, data)
        self.host.write_json = lambda frame: self.frames.append(frame) or True
        def submit(rid, sid, session, text, *, image_paths=None, terminal_callback=None):
            self.turns.append((rid, sid, text))
            terminal_callback({"status": "settled", "text": "acknowledged"})
            session["running"] = False
            return True
        self.host._run_prompt_submit = submit
        self.modules = patch.dict("sys.modules", {"tui_gateway.server": self.host})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.ctx = FakePluginContext()
        self.ctx._gateway_injection_allowed = lambda: True
        self.worker = TerminalWakeWorker(self.registry, self.ctx, now_fn=self.clock)

    def test_idle_wake_uses_exact_owner_and_only_one_native_turn(self):
        task = self._wake("pi-desktop", origin=self.origin)
        self.worker.run_once(now=self.clock())
        self.worker.run_once(now=self.clock())
        self.assertEqual(len(self.turns), 1)
        self.assertEqual(self.turns[0][1], "tab-a")
        self.assertEqual(self.ctx.injects, [])
        self.assertEqual(self.registry.get_task(task)["wake_state"], "accepted")
        receipts = [event for event in self.registry.recent_events(task, limit=20)
                    if event["event_type"] == "wake_turn_finished"]
        self.assertEqual(len(receipts), 1)

    def test_busy_and_queued_human_prompts_do_not_spend_wake_attempts(self):
        task = self._wake("pi-busy", origin=self.origin)
        session = self.host._sessions["tab-a"]
        for key, value in (("running", True), ("queued_prompt", {"text": "human"}),
                           ("queued_prompts", [{"text": "human"}]), ("_auto_continue_scheduled", True)):
            session[key] = value
            self.worker.run_once(now=self.clock())
            self.assertEqual(self.registry.get_task(task)["wake_attempts"], 0)
            self.assertEqual(self.turns, [])
            session.pop(key)
        self.worker.run_once(now=self.clock())
        self.assertEqual(len(self.turns), 1)

    def test_desktop_origin_never_falls_back_to_gateway_or_cli(self):
        task = self._wake("pi-foreign", origin=self.origin)
        self.host._sessions.clear()
        self.ctx._manager = SimpleNamespace(has_gateway_message_injector=True)
        self.worker.run_once(now=self.clock())
        row = self.registry.get_task(task)
        self.assertEqual(row["wake_state"], "pending")
        self.assertEqual(row["wake_attempts"], 0)
        self.assertEqual(self.ctx.injects, [])

    def test_reused_tab_does_not_receive_an_old_session_result(self):
        self._wake("pi-reused", origin=self.origin)
        self.host._sessions["tab-a"]["session_key"] = "new-human-session"
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.turns, [])

    def test_compression_tip_is_the_target(self):
        self._wake("pi-tip", origin=self.origin)
        self.host._sessions["tab-a"]["session_key"] = "compressed-tip"
        self.host._get_db = lambda: SimpleNamespace(resolve_resume_session_id=lambda key: "compressed-tip")
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.turns[0][1], "tab-a")

    def test_missing_grant_refuses_native_admission(self):
        task = self._wake("pi-denied", origin=self.origin)
        self.ctx._gateway_injection_allowed = lambda: False
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.turns, [])
        self.assertIn("grant", self.registry.get_task(task)["wake_last_error"])

    def test_ambiguous_native_exception_is_never_replayed(self):
        task = self._wake("pi-ambiguous", origin=self.origin)
        def broken(*args, terminal_callback=None, **kwargs):
            raise OSError("could have started before connection failed")
        self.host._run_prompt_submit = broken
        self.worker.run_once(now=self.clock())
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.registry.get_task(task)["wake_state"], "uncertain")
        self.assertEqual(self.ctx.injects, [])

    def test_busy_race_is_deferred_without_consuming_retry_budget(self):
        task = self._wake("pi-race", origin=self.origin)
        with patch.object(desktop_host, "deliver_wake", return_value="busy"):
            self.worker.run_once(now=self.clock())
        row = self.registry.get_task(task)
        self.assertEqual(row["wake_state"], "pending")
        self.assertEqual(row["wake_attempts"], 0)

    def test_passive_progress_while_busy_never_starts_a_turn(self):
        task = self._wake("pi-progress", origin=self.origin)
        self.host._sessions["tab-a"]["running"] = True
        self.outbox.enqueue(task, "progress", "Pi is editing the requested files.")
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=self.clock()), 1)
        self.assertEqual(self.frames[0][0:2], ("notification.show", "tab-a"))
        self.assertEqual(self.frames[0][2]["key"], "pi-manager:" + task)
        self.assertEqual(self.turns, [])
        self.assertEqual(self.ctx.injects, [])
        self.assertEqual(self.registry.list_notifications(task_id=task)[0]["status"], "sent")

    def test_status_dedup_and_missing_owner_preserve_delivery(self):
        task = self._wake("pi-status", origin=self.origin)
        nid = self.outbox.enqueue(task, "progress", "working")
        session = self.host._sessions.pop("tab-a")
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=self.clock()), 0)
        row = self.registry.list_notifications(task_id=task)[0]
        self.assertEqual(row["attempts"], 0)
        self.host._sessions["tab-a"] = session
        self.assertEqual(worker.run_once(now=self.clock()), 1)
        desktop_host.emit_status(self.origin, "working", nid)
        self.assertEqual(len(self.frames), 1)

    def test_foreign_desktop_notices_do_not_starve_telegram(self):
        desktop = self._wake("pi-unowned", origin=self.origin)
        telegram = self._wake("pi-telegram", origin=ORIGIN)
        self.outbox.enqueue(desktop, "progress", "desktop")
        self.outbox.enqueue(telegram, "progress", "telegram")
        self.host._sessions.clear()
        sent = []
        worker = OutboxWorker(self.outbox, deliver=lambda args: sent.append(args) or {"success": True},
                              max_per_tick=1, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=self.clock()), 1)
        self.assertTrue(sent[0]["target"].startswith("telegram:"))
        self.assertEqual(self.registry.list_notifications(task_id=desktop)[0]["attempts"], 0)

    def test_foreign_worker_does_not_mark_live_dispatch_uncertain(self):
        task = self._wake("pi-live-dispatch", origin=self.origin)
        self.registry.claim_terminal_wake(task, self.clock())
        self.assertEqual(self.registry.get_task(task)["wake_owner_pid"], os.getpid())
        self.assertEqual(self.registry.settle_stale_wake_dispatching(self.clock()), [])
        self.assertEqual(self.registry.get_task(task)["wake_state"], "dispatching")

    def test_disconnected_transport_does_not_mark_notice_sent(self):
        task = self._wake("pi-disconnected", origin=self.origin)
        self.outbox.enqueue(task, "progress", "working")
        self.host.write_json = lambda frame: False
        OutboxWorker(self.outbox, now_fn=self.clock).run_once(now=self.clock())
        self.assertNotEqual(self.registry.list_notifications(task_id=task)[0]["status"], "sent")

    def test_unavailable_desktop_wakes_do_not_starve_gateway(self):
        for i in range(self.worker.max_per_tick + 1):
            self._wake("pi-closed-%s" % i, origin=self.origin)
        self.host._sessions.clear()
        task = self._wake("pi-live-telegram", origin=ORIGIN)
        self.ctx._manager = SimpleNamespace(has_gateway_message_injector=True)
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.registry.get_task(task)["wake_state"], "accepted")
        self.assertEqual(len(self.ctx.injects), 1)

    def test_cold_resume_can_claim_lease_through_native_admission(self):
        self._wake("pi-resumed", origin=self.origin)
        self.host._sessions["tab-a"].pop("active_session_lease")
        self.worker.run_once(now=self.clock())
        self.assertEqual(len(self.turns), 1)

    def test_profile_identity_is_required_for_another_profile(self):
        self.host._sessions["tab-a"]["profile_home"] = "/test/hermes/profiles/work"
        self._wake("pi-default-home", origin=self.origin)
        self.worker.run_once(now=self.clock())
        self.assertEqual(self.turns, [])
        origin = dict(self.origin, hermes_home="/test/hermes/profiles/work")
        self.ctx._gateway_injection_allowed = lambda: self.bound_home == origin["hermes_home"]
        self._wake("pi-work-home", origin=origin)
        self.worker.run_once(now=self.clock())
        self.assertEqual(len(self.turns), 1)
        self.assertIsNone(self.bound_home)
