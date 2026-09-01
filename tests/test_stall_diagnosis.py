"""A stall should say what is hanging, not just that something is.

On 2026-08-28 a task sat STALLED for seven minutes because
`lsof -nP -iTCP:... | head` never returned. Pi had already emitted
tool_execution_end for that call, so active_tool was empty and nothing in the
registry suggested a child process was still alive. Identifying it took a
manual walk of the process table. These tests use real processes — a fake
would not have caught that the orphan is a *descendant* of the agent rather
than its active tool.
"""
from __future__ import annotations

import subprocess
import time
import unittest

from test_pi_manager import PiManagerTestCase, FakePiProcess  # type: ignore


class _RtWithPid:
    """Minimal stand-in: _live_descendants only ever reads transport.pid."""

    class _T:
        def __init__(self, pid): self.pid = pid

    def __init__(self, pid):
        self.transport = self._T(pid)


class TestLiveDescendants(PiManagerTestCase):
    def setUp(self):
        super().setUp()
        self.manager = self.make_manager(FakePiProcess())

    def test_reports_a_hanging_child(self):
        # A child that sleeps is exactly the shape of the lsof hang: alive,
        # owned by us, burning no CPU.
        child = subprocess.Popen(["sleep", "30"])
        self.addCleanup(child.wait)   # reap it, or the run leaks a ResourceWarning
        self.addCleanup(child.kill)
        time.sleep(0.3)

        import os
        kids = self.manager._live_descendants(_RtWithPid(os.getpid()))
        pids = [k["pid"] for k in kids]
        self.assertIn(child.pid, pids, "a live child must appear in the diagnosis")
        entry = next(k for k in kids if k["pid"] == child.pid)
        self.assertIn("sleep", entry["command"])
        for field in ("elapsed", "cpu_time", "command"):
            self.assertIn(field, entry)

    def test_no_children_reports_empty(self):
        child = subprocess.Popen(["true"])
        child.wait()
        # A leaf process with nothing under it must not invent entries.
        kids = self.manager._live_descendants(_RtWithPid(child.pid))
        self.assertEqual(kids, [])

    def test_missing_pid_is_survivable(self):
        class _NoPid:
            transport = None
        self.assertEqual(self.manager._live_descendants(_NoPid()), [])

    def test_output_is_bounded(self):
        import os
        kids = self.manager._live_descendants(_RtWithPid(os.getpid()), limit=2)
        self.assertLessEqual(len(kids), 2)
        for k in kids:
            self.assertLessEqual(len(k["command"]), 200)


if __name__ == "__main__":
    unittest.main()
