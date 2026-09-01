"""Regression: a finished task's reported runtime must stop growing.

``status()`` used to compute ``runtime_seconds`` as ``now - started_at``
unconditionally. For a task that had already settled, that number kept
climbing with wall-clock time, so the same 154 s task read as 745 s twelve
minutes later. The practical damage was worse than a cosmetic number: two
tasks that ran strictly back to back both got stretched to the instant of
the report, which made them look like they had overlapped — and that was
read as evidence of parallel execution that never happened.
"""
from __future__ import annotations

import unittest

from test_pi_manager import (  # type: ignore
    PiManagerTestCase, FakePiProcess, EXEC_SETTLED, EXEC_ABORTED,
)


class TestStatusRuntimeFreezes(PiManagerTestCase):
    def _settled_task(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.clock.advance(154.0)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        return manager, task_id

    def test_runtime_freezes_at_settlement(self):
        manager, task_id = self._settled_task()
        at_settle = manager.status(task_id)["runtime_seconds"]
        self.assertAlmostEqual(at_settle, 154.0, places=1)

        # Twelve minutes later the answer must be identical.
        self.clock.advance(720.0)
        later = manager.status(task_id)["runtime_seconds"]
        self.assertAlmostEqual(later, at_settle, places=3,
                               msg="runtime of a settled task must not grow")

    def test_progress_age_also_freezes(self):
        manager, task_id = self._settled_task()
        first = manager.status(task_id)["last_progress_age_seconds"]
        self.clock.advance(720.0)
        self.assertAlmostEqual(manager.status(task_id)["last_progress_age_seconds"],
                               first, places=3)

    def test_running_task_still_measures_against_now(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.clock.advance(30.0)
        first = manager.status(task_id)["runtime_seconds"]
        self.clock.advance(30.0)
        second = manager.status(task_id)["runtime_seconds"]
        self.assertGreater(second, first,
                           "a live task must still report a growing runtime")
        self.assertAlmostEqual(second, 60.0, places=1)

    def test_back_to_back_tasks_do_not_appear_to_overlap(self):
        """The bug's real symptom, asserted directly."""
        p1 = FakePiProcess()
        m1 = self.make_manager(p1)
        t1 = self.start_and_boot(m1, p1)
        t1_start = self.registry.get_task(t1)["started_at"]
        self.clock.advance(154.0)
        self.emit_and_sync(m1, p1, t1, {"type": "agent_settled"})

        self.clock.advance(217.0)  # gap: nothing running

        p2 = FakePiProcess()
        m2 = self.make_manager(p2)
        t2 = self.start_and_boot(m2, p2)
        t2_start = self.registry.get_task(t2)["started_at"]
        self.clock.advance(192.0)
        self.emit_and_sync(m2, p2, t2, {"type": "agent_settled"})

        self.clock.advance(180.0)  # report written well after both finished

        t1_end = t1_start + m1.status(t1)["runtime_seconds"]
        self.assertLessEqual(
            t1_end, t2_start,
            "the first task's reported window must not reach into the second's",
        )


if __name__ == "__main__":
    unittest.main()
