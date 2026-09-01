"""Does the manager support concurrent tasks, or only one at a time?

Nothing in start_task() serializes: it returns as soon as the task is
registered, _runtimes is keyed by task_id, and each task gets its own
process, reader thread and watchdog. These tests assert that directly
rather than inferring it from a reading of the code.
"""
from __future__ import annotations

import unittest

from test_pi_manager import (  # type: ignore
    PiManagerTestCase, FakePiProcess, fake_popen_factory, TEST_THRESHOLDS,
    EXEC_RUNNING, EXEC_SETTLED,
)
from core import PiManager  # type: ignore


class TestParallelTasks(PiManagerTestCase):
    def _manager_for(self, process):
        return PiManager(
            registry=self.registry,
            popen_factory=fake_popen_factory(process),
            clock=self.clock,
            default_thresholds=TEST_THRESHOLDS,
        )

    def test_two_tasks_run_at_the_same_time(self):
        pa, pb = FakePiProcess(), FakePiProcess()
        ma, mb = self._manager_for(pa), self._manager_for(pb)

        ta = self.start_and_boot(ma, pa)
        tb = self.start_and_boot(mb, pb)

        # Both live, neither waiting on the other.
        self.assertEqual(ma.status(ta)["execution_state"], EXEC_RUNNING)
        self.assertEqual(mb.status(tb)["execution_state"], EXEC_RUNNING)

        live = [r for r in self.registry.list_tasks()
                if r["execution_state"] not in ("SETTLED", "ABORTED", "CRASHED")]
        self.assertEqual(len(live), 2, "both tasks must be live simultaneously")

        # Settling one must not disturb the other.
        self.emit_and_sync(ma, pa, ta, {"type": "agent_settled"})
        self.assertEqual(ma.status(ta)["execution_state"], EXEC_SETTLED)
        self.assertEqual(mb.status(tb)["execution_state"], EXEC_RUNNING)

        self.emit_and_sync(mb, pb, tb, {"type": "agent_settled"})
        self.assertEqual(mb.status(tb)["execution_state"], EXEC_SETTLED)

    def test_concurrent_tasks_keep_separate_identities(self):
        pa, pb = FakePiProcess(), FakePiProcess()
        ma, mb = self._manager_for(pa), self._manager_for(pb)
        ta = self.start_and_boot(ma, pa)
        tb = self.start_and_boot(mb, pb)

        self.assertNotEqual(ta, tb)
        ra, rb = self.registry.get_task(ta), self.registry.get_task(tb)
        self.assertNotEqual(ra["session_file"], rb["session_file"],
                            "concurrent tasks must not share a session file")

    def test_events_are_not_cross_attributed(self):
        pa, pb = FakePiProcess(), FakePiProcess()
        ma, mb = self._manager_for(pa), self._manager_for(pb)
        ta = self.start_and_boot(ma, pa)
        tb = self.start_and_boot(mb, pb)

        before_b = self.registry.get_task(tb)["message_count"] or 0
        for _ in range(3):
            self.emit_and_sync(ma, pa, ta, {"type": "message_start"})
        self.assertEqual((self.registry.get_task(tb)["message_count"] or 0), before_b,
                         "task A's events must not land on task B")


class TestVerifierOmissionIsVisible(PiManagerTestCase):
    """A task with no verifier can only ever end at NOT_RUN, so "settled"
    carries no evidence the work is any good. That is legitimate for a
    read-only audit and wrong for everything else — either way it should be
    recorded rather than silent, so the omission can be counted afterwards."""

    def _events(self, task_id, kind):
        return [e for e in self.registry.recent_events(task_id, limit=200)
                if e["event_type"] == kind]

    def test_missing_verifier_is_recorded(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.assertTrue(self._events(task_id, "verifier_absent"),
                        "a task dispatched without a verifier must say so")

    def test_verifier_present_records_nothing(self):
        from core import VerifierSpec  # type: ignore
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=["true"], timeout_seconds=10.0))
        self.assertFalse(self._events(task_id, "verifier_absent"),
                         "a gated task must not be flagged")


if __name__ == "__main__":
    unittest.main()
