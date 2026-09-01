"""Monitoring correction acceptance tests (2026-09-01, simplified).

The watchdog is progress-based, not wall-clock-based:

- no hard timeout as primary control — the operator safety cap is OFF by
  default, and a long task with real progress survives longer than the old
  hard cap;
- the persisted ``last_progress_at`` and current-tool (``active_tool``)
  fields drive the stale timer; every meaningful RPC event refreshes
  progress, tool start sets the current tool, tool end clears it;
- no progress ~450 s outside a tool / ~1200 s with an active tool (real
  production defaults) -> Pi RPC abort, then a 120 s grace: settlement
  during the grace preserves the true result, only an agent that ignores
  the abort is finalized STALLED;
- the watchdog runs on its own ~30 s cadence, independent of the
  user-facing notification rate limit (first progress notice after 90 s,
  then at most one per 180 s);
- restart recovery is simple: probe the live task via get_state, set a
  fresh last_progress_at, resume monitoring — downtime itself never stalls
  a task.

Companion coverage already in test_outbox.py / test_completion_notice.py /
test_tools_integration.py: terminal notice enqueue, host-adapter delivery
with no LLM/agent turn, two topics with no cross-routing, stale sending-
lease retry, and restart retention of routing + pending outbox rows.

Deterministic: FakeClock + FakePiProcess, manual watchdog ticks.
"""

from __future__ import annotations

import json
import unittest

from test_outbox import (  # type: ignore
    ORIGIN_A,
    FakePiProcess,
    OutboxTestCase,
    STATUS_PENDING,
    STATUS_SENT,
)
from test_pi_manager import (  # type: ignore
    EXEC_ABORTED,
    EXEC_SETTLED,
    EXEC_STALLED,
    EXEC_TOOL_RUNNING,
    EXEC_WAITING,
    TEST_THRESHOLDS,
    VerifierSpec,
    wait_until,
)
from core import Thresholds  # type: ignore
from outbox import NotificationOutbox, OutboxWorker  # type: ignore
from registry_db import Registry  # type: ignore
from test_pi_manager import TEST_THRESHOLDS  # type: ignore


def _prod() -> Thresholds:
    """Production defaults: the monitoring-correction policy under test."""
    return Thresholds()


def _no_os_activity(manager):
    """The fake process has no real OS tree; 'no proof of work' is the
    deterministic choice (and the safe one)."""
    manager._tool_process_active = lambda rt, window: None  # type: ignore[method-assign]
    return manager


def _dormant_watchdog(thresholds: Thresholds) -> Thresholds:
    """Tests drive watchdog_tick manually: the auto-loop must be dormant so
    a background tick can never race the FakeClock."""
    return Thresholds(**{**thresholds.__dict__,
                         "watchdog_interval_seconds": 999999.0})


class TestNoHardTimeoutAsPrimaryControl(OutboxTestCase):
    def test_hard_cap_is_disabled_by_default(self):
        self.assertIsNone(Thresholds().emergency_cap_seconds,
                          "the operator safety cap must be OFF by default")

    def test_longer_than_old_hard_timeout_with_progress_survives(self):
        """1200 s > the old 900 s hard cap, real progress every 30 s: the
        task must never be killed or stalled on elapsed time."""
        process = FakePiProcess()
        manager = self.make_manager(process, default_thresholds=_dormant_watchdog(_prod()))
        _no_os_activity(manager)
        task_id = self.start_and_boot(manager, process)
        for k in range(40):
            self.clock.advance(30)
            self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
            manager.watchdog_tick(task_id)
            state = manager.status(task_id)["execution_state"]
            self.assertNotIn(state, (EXEC_ABORTED, EXEC_STALLED),
                             f"t+{(k + 1) * 30}s: {state}")
        self.assertNotEqual(manager.status(task_id)["execution_state"], EXEC_ABORTED)

    def test_operator_can_still_install_an_explicit_cap(self):
        process = FakePiProcess()
        cap_th = Thresholds(**{**_prod().__dict__, "emergency_cap_seconds": 100.0})
        manager = self.make_manager(process, default_thresholds=_dormant_watchdog(cap_th))
        _no_os_activity(manager)
        task_id = self.start_and_boot(manager, process)
        self.clock.advance(101)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_ABORTED)


class TestProgressFieldsDriveTheStaleTimer(OutboxTestCase):
    def test_events_refresh_progress_and_tool_start_end_set_clear_current_tool(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        base = self.registry.get_task(task_id)
        self.assertEqual(base["last_progress_at"], base["started_at"])
        self.assertIsNone(base["active_tool"])

        self.clock.advance(20)
        self.emit_and_sync(manager, process, task_id,
                           {"type": "tool_execution_start", "tool": "bash",
                            "toolCallId": "t9"})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["active_tool"], "bash", "tool start sets current tool")
        self.assertEqual(row["last_progress_at"], self.clock(),
                         "a meaningful RPC event refreshes progress")

        self.clock.advance(10)
        self.emit_and_sync(manager, process, task_id,
                           {"type": "tool_execution_end", "toolCallId": "t9"})
        row = self.registry.get_task(task_id)
        self.assertIsNone(row["active_tool"], "tool end clears current tool")
        self.assertEqual(row["last_progress_at"], self.clock())

    def test_streaming_counts_as_progress(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        for _ in range(3):
            self.clock.advance(60)
            self.emit_and_sync(manager, process, task_id, {"type": "message_update"})
            manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], "RUNNING")


class TestActiveToolUsesTheLongerThreshold(OutboxTestCase):
    def test_long_active_tool_uses_1200s_threshold(self):
        """Production defaults: an active tool survives 600 s of RPC
        silence (the longer, 1200 s, threshold applies), and silence past
        1200 s + the 120 s abort grace finalizes STALLED after exactly one
        RPC abort request."""
        process = FakePiProcess()
        manager = self.make_manager(process, default_thresholds=_dormant_watchdog(_prod()))
        _no_os_activity(manager)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id,
                           {"type": "tool_execution_start", "tool": "nx",
                            "toolCallId": "t1"})
        self.assertEqual(manager.status(task_id)["execution_state"],
                         EXEC_TOOL_RUNNING)

        for _ in range(10):  # 600 s of pure silence
            self.clock.advance(60)
            manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"],
                         EXEC_TOOL_RUNNING,
                         "600 s < 1200 s tool threshold: the tool still owns the task")
        self.assertEqual(len([c for c in process.commands_received
                              if c.get("type") == "abort"]), 0,
                         "no abort request inside the tool threshold")

        for _ in range(14):  # 600 s more: past 1200 s + 120 s grace
            self.clock.advance(60)
            manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)
        self.assertEqual(len([c for c in process.commands_received
                              if c.get("type") == "abort"]), 1,
                         "one RPC abort per grace period")
        events = [e["event_type"] for e in self.registry.recent_events(task_id, 500)]
        self.assertIn("stall_abort_sent", events)


class TestStallAbortGrace(OutboxTestCase):
    """Uses the suite's TEST_THRESHOLDS (quiet 90 s, grace 15 s) so the
    same policy shape is exercised at test speed."""

    def test_no_progress_outside_tool_aborts_then_stalls(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        for _ in range(5):  # 150 s of pure silence (threshold 90 s)
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
            if manager.status(task_id)["execution_state"] == EXEC_STALLED:
                break
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)
        self.assertTrue(any(c.get("type") == "abort" for c in process.commands_received),
                        "the stall path must request an RPC abort")
        events = [e["event_type"] for e in self.registry.recent_events(task_id, 400)]
        self.assertIn("stall_abort_sent", events)

    def test_settle_during_grace_preserves_the_true_result(self):
        """The agent settles AFTER the abort request but BEFORE the grace
        expires: the true result (SETTLED + real verdict) wins and the task
        is never read as STALLED."""
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=["true"], timeout_seconds=10.0))
        for _ in range(3):  # t=90: quiet threshold trips, RPC abort requested
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
        self.assertTrue(any(c.get("type") == "abort" for c in process.commands_received))
        self.assertEqual(self.registry.get_task(task_id)["execution_state"],
                         EXEC_WAITING)

        # Within the grace the agent settles.
        self.clock.advance(5)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        manager.watchdog_tick(task_id)

        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_SETTLED)
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["verification_state"] == "PASS"))
        events = self.registry.recent_events(task_id, 400)
        self.assertFalse(any(e["event_type"] == "state_transition"
                             and e["state_after"] == EXEC_STALLED for e in events),
                         "settlement during grace must never be overwritten by STALLED")
        # The terminal notice is the verifier notice carrying the real
        # verdict — not a stall notice.
        kinds = [n["kind"] for n in self.registry.list_notifications(task_id=task_id)]
        self.assertEqual(kinds, ["verifier"])
        self.assertIn("Weryfikacja: PASS",
                      self.registry.get_notification(f"term:{task_id}:verifier")["message"])

    def test_abort_ignored_finalizes_stalled_with_terminal_notice(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        for _ in range(6):  # silence past threshold + grace
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)
        n = self.registry.get_notification(f"term:{task_id}:stalled")
        self.assertIsNotNone(n, "a stall is a terminal notice")
        self.assertEqual(n["status"], STATUS_PENDING, "terminal notices are immediate")
        self.assertIn("STALLED", n["message"])


class TestWatchdogCadenceIndependentOfNotificationRateLimit(OutboxTestCase):
    def test_stall_detected_while_notifications_stay_rate_limited(self):
        """The watchdog runs on its own cadence and still detects the stall
        while user-facing progress notices stay bounded to one."""
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        rt = manager._rt(task_id)

        # 420 s of real progress: exactly one progress notice. The first
        # fires after the 90 s window; after that the fake's get_state
        # snapshot (constant hash, constant messageCount) means the
        # real-progress signature gate — together with the 180 s interval —
        # suppresses the other 13 ticks.
        for _ in range(14):
            self.clock.advance(30)
            self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
            manager.watchdog_tick(task_id)
            manager._notify_progress(task_id, rt)
        progress_rows = [n for n in self.registry.list_notifications(task_id=task_id)
                         if n["kind"] == "progress"]
        self.assertEqual(len(progress_rows), 1, "progress notices stay rate-limited")
        self.assertEqual(progress_rows[0]["created_at"],
                         self.registry.get_task(task_id)["started_at"] + 90,
                         "the first notice arrives right at the 90 s boundary")

        # Then pure silence: the watchdog — NOT the notification gate —
        # carries the stall detection through to STALLED.
        for _ in range(6):
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
            manager._notify_progress(task_id, rt)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)
        progress_rows = [n for n in self.registry.list_notifications(task_id=task_id)
                         if n["kind"] == "progress"]
        self.assertEqual(len(progress_rows), 1, "silence must not produce more notices")
        stalled = [n for n in self.registry.list_notifications(task_id=task_id)
                   if n["kind"] == "stalled"]
        self.assertEqual(len(stalled), 1)


class TestSimpleRestartRecovery(OutboxTestCase):
    def test_restart_probes_resets_progress_and_keeps_routing_and_outbox(self):
        """A task live for 2 h while the plugin was down: after recovery the
        routing and the pending outbox row are intact, the stale timer is
        fresh from the live-state probe, and the downtime itself never
        stalls the task."""
        session_file = self.tmp / "sess-recover.jsonl"
        session_file.write_text('{"type": "message", "message": {"role": "user"}}\n')
        self.registry.create_task(
            task_id="pi-down", execution_state="RUNNING",
            session_file=str(session_file),
            expected_session_file=str(session_file),
            expected_session_id="sess-fake-1",
            origin=json.dumps(ORIGIN_A),
            message_count=37, is_streaming=1,
            # started/last_progress live on the FakeClock timeline: the test
            # simulates the plugin being down for 2 h by simply not ticking
            # the clock in between.
            started_at=self.clock() - 2 * 3600,
            last_progress_at=self.clock() - 2 * 3600,
            last_event_at=self.clock() - 2 * 3600,
        )
        pending_nid = self.outbox.enqueue("pi-down", "progress", "was working")

        # RESTART: fresh Registry + outbox on the same file.
        path = self.registry.path
        self.registry.close()
        self.registry = Registry(db_path=path)
        self.addCleanup(self.registry.close)
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)

        process = FakePiProcess()
        manager = self.make_manager(process, default_thresholds=_dormant_watchdog(_prod()))
        result = manager.recover_task("pi-down")
        self.assertTrue(result["recovered"])

        row = self.registry.get_task("pi-down")
        self.assertEqual(row["execution_state"], EXEC_WAITING)
        self.assertEqual(json.loads(row["origin"])["platform"], "telegram",
                         "routing survives the restart")
        self.assertEqual(self.registry.get_notification(pending_nid)["status"],
                         STATUS_PENDING, "pending outbox rows survive the restart")
        # The probe established a FRESH progress baseline at recovery time:
        # the 2 h of downtime cannot be the input to a stall decision.
        self.assertGreaterEqual(row["last_progress_at"], self.clock())

        for _ in range(3):  # 3 fresh watchdog passes (60 s of test time)
            self.clock.advance(20)
            manager.watchdog_tick("pi-down")
        self.assertNotEqual(self.registry.get_task("pi-down")["execution_state"],
                            EXEC_STALLED, "downtime itself must never stall a task")

    def test_recovered_task_still_delivers_to_its_origin(self):
        session_file = self.tmp / "sess-recover2.jsonl"
        session_file.write_text('{"type": "message", "message": {"role": "user"}}\n')
        self.registry.create_task(
            task_id="pi-down2", execution_state="RUNNING",
            session_file=str(session_file),
            expected_session_file=str(session_file),
            expected_session_id="sess-fake-1",
            origin=json.dumps(ORIGIN_A),
            started_at=self.clock() - 2 * 3600,
            last_progress_at=self.clock() - 2 * 3600,
        )
        path = self.registry.path
        self.registry.close()
        self.registry = Registry(db_path=path)
        self.addCleanup(self.registry.close)
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)

        process = FakePiProcess()
        manager = self.make_manager(process, default_thresholds=_dormant_watchdog(_prod()))
        self.assertTrue(manager.recover_task("pi-down2")["recovered"])
        # The recovered task is already ~2h into its runtime, so its watchdog
        # may legitimately enqueue one bounded progress notice as well. Assert
        # specifically on the COMPLETION notice: exactly one, routed to the
        # persisted origin, delivered through the public send_message tool.
        self.emit_and_sync(manager, process, "pi-down2", {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status("pi-down2")["execution_state"] == EXEC_SETTLED))
        ctx_calls = []

        def delivery(args, **kw):
            ctx_calls.append(args)
            return '{"sent": true}'

        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        for _ in range(5):  # drain everything due; bounded, no real sleep
            worker.run_once(now=self.clock())
            if any("Pi zakończył" in a["message"] for a in ctx_calls):
                break
        settled_calls = [a for a in ctx_calls if "Pi zakończył" in a["message"]]
        self.assertEqual(len(settled_calls), 1, "exactly one completion notice")
        self.assertEqual(settled_calls[0]["action"], "send")
        self.assertEqual(settled_calls[0]["target"], "telegram:-1001234567890:77",
                         "post-restart settlement delivers to the persisted origin")
        self.assertEqual(self.registry.get_notification("term:pi-down2:settled")["status"],
                         STATUS_SENT)
        for args in ctx_calls:  # every delivery is one send, no LLM turn
            self.assertEqual(args["action"], "send")


if __name__ == "__main__":
    unittest.main()