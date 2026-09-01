"""Lifecycle must be driven by progress, not by time since task start.

Motivating incident (2026-08-27): task pi-16d3ff7c3f14 was killed at ~14 min
with reason=hard_execution_timeout while making real progress. The wall-clock
check ran FIRST in watchdog_tick, ahead of every progress and state signal, so
a healthy long task could not survive it. Measured real delegations on this
box run 16 min and 49 min; the old cap was 15 min.

Deterministic: FakeClock + FakePiProcess, no real Pi, no sleeps.
"""

from __future__ import annotations

import unittest

from test_pi_manager import (  # type: ignore
    PiManagerTestCase, FakePiProcess, fake_popen_factory, TEST_THRESHOLDS,
    EXEC_ABORTED, EXEC_RUNNING, EXEC_STALLED, EXEC_TOOL_RUNNING, EXEC_SETTLED,
)
from core import PiManager, Thresholds  # type: ignore


def _th(**over):
    return Thresholds(**{**TEST_THRESHOLDS.__dict__, **over})


class TestNoWallClockKill(PiManagerTestCase):
    """A: a long task that keeps making progress is never killed on age."""

    def test_two_hours_of_progress_is_not_aborted(self):
        process = FakePiProcess()
        # Production default cap (6 h), NOT the suite's shortened 900 s: the
        # point of this test is that a progressing task survives well past the
        # old 15 min deadline, so the backstop must be the real one.
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                            clock=self.clock,
                            default_thresholds=_th(emergency_cap_seconds=21600.0))
        task_id = self.start_and_boot(manager, process)

        # 2 h of work, a progress event every 30 s — far past the old 900 s cap.
        for _ in range(240):
            self.clock.advance(30)
            self.emit_and_sync(manager, process, task_id, {"type": "message_update"})
            manager.watchdog_tick(task_id)
            state = manager.status(task_id)["execution_state"]
            self.assertNotEqual(state, EXEC_ABORTED,
                                "a progressing task must never be aborted on elapsed time")
            self.assertNotEqual(state, EXEC_STALLED)

        self.assertNotEqual(manager.status(task_id)["execution_state"], EXEC_ABORTED)


class TestEmergencyCapIsDistinct(PiManagerTestCase):
    """G: the backstop still fires, and reports its own reason."""

    def test_cap_reports_emergency_reason_not_stall(self):
        process = FakePiProcess()
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                            clock=self.clock, default_thresholds=_th(emergency_cap_seconds=100.0))
        task_id = self.start_and_boot(manager, process)

        self.clock.advance(101)
        manager.watchdog_tick(task_id)

        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_ABORTED)
        self.assertNotEqual(status["execution_state"], EXEC_STALLED)
        events = self.registry.recent_events(task_id, limit=50)
        reasons = [e.get("summary") for e in events if e["event_type"] == "abort_requested"]
        self.assertTrue(any("EMERGENCY_CAP_EXCEEDED" in str(r) for r in reasons),
                        f"abort reason must be EMERGENCY_CAP_EXCEEDED, got {reasons}")

    def test_cap_can_be_disabled_entirely(self):
        process = FakePiProcess()
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                            clock=self.clock, default_thresholds=_th(emergency_cap_seconds=None))
        task_id = self.start_and_boot(manager, process)

        self.clock.advance(10 * 3600)  # 10 h
        self.emit_and_sync(manager, process, task_id, {"type": "message_update"})
        manager.watchdog_tick(task_id)

        self.assertNotEqual(manager.status(task_id)["execution_state"], EXEC_ABORTED)


class TestRealStallStillDetected(PiManagerTestCase):
    """C: removing the wall clock must not blind the stall detector."""

    def test_silence_with_no_activity_still_reaches_stalled(self):
        process = FakePiProcess()
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                            clock=self.clock, default_thresholds=_th())
        task_id = self.start_and_boot(manager, process)

        # No events, no state change, no tool: pure silence past soft stall + grace.
        for _ in range(12):
            self.clock.advance(30)
            manager.watchdog_tick(task_id)

        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED,
                         "a genuinely silent task must still be detected as STALLED")

        # ...and it must say what was hanging. A bare "STALLED" sent an
        # operator walking the process table by hand on 2026-08-28; the
        # answer (an orphaned `lsof` that outlived its tool_execution_end)
        # was already visible to the manager and simply never recorded.
        diag = [e for e in self.registry.recent_events(task_id, limit=200)
                if e["event_type"] == "stall_diagnosis"]
        self.assertTrue(diag, "entering STALLED must record a stall_diagnosis")
        import json as _json
        payload = _json.loads(diag[-1]["summary"])
        for field in ("active_tool", "idle_seconds", "live_descendants"):
            self.assertIn(field, payload)

    def test_stall_diagnosis_is_recorded_once_not_per_tick(self):
        """The watchdog ticks every couple of seconds while a task stays
        stalled. Re-recording the diagnosis on each tick would bury the event
        log the way diagnostic_snapshot already can."""
        process = FakePiProcess()
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                            clock=self.clock, default_thresholds=_th())
        task_id = self.start_and_boot(manager, process)
        for _ in range(24):
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
        diag = [e for e in self.registry.recent_events(task_id, limit=400)
                if e["event_type"] == "stall_diagnosis"]
        self.assertEqual(len(diag), 1,
                         f"expected one diagnosis on entering STALLED, got {len(diag)}")


if __name__ == "__main__":
    unittest.main()
