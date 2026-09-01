"""Scenario B: a silent-but-working tool must not be called a stall.

`nx run-many -t build,test,lint --skipNxCache` over this monorepo was measured
at 5 m 19 s producing no RPC output. Silence on the event stream is therefore
not evidence of a stall, and the manager now asks the OS instead: walk the
agent's process tree and look for real CPU burn.

These tests exercise the real `_tool_process_active` against real processes —
no fake — because the whole point is the `ps` parsing and the CPU delta.
"""

from __future__ import annotations

import subprocess
import sys
import time
import unittest

sys.path.insert(0, str(__file__.rsplit("/tests/", 1)[0]))

from core import PiManager, Thresholds  # type: ignore


class _FakeTransport:
    def __init__(self, pid):
        self.pid = pid


class _FakeRt:
    def __init__(self, pid):
        self.transport = _FakeTransport(pid)


class TestToolProcessActivity(unittest.TestCase):
    """The detector must see a busy child and must not invent activity."""

    def setUp(self):
        self.mgr = PiManager.__new__(PiManager)  # no registry/IO needed
        self.mgr._now = time.time

    def test_busy_child_process_is_detected_as_activity(self):
        # A real child that burns CPU, standing in for a long `nx` build.
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time\nt=time.time()\nwhile time.time()-t<6: pass"]
        )
        try:
            rt = _FakeRt(proc.pid)
            # First call only establishes the baseline — by contract it
            # returns None rather than guessing.
            self.assertIsNone(self.mgr._tool_process_active(rt, 60.0),
                              "first sample must establish a baseline, not a verdict")
            time.sleep(3)
            activity = self.mgr._tool_process_active(rt, 60.0)
            self.assertIsNotNone(activity, "a CPU-burning child must be reported as activity")
            self.assertGreater(activity["cpu_delta_seconds"], 0.5)
        finally:
            proc.kill()
            proc.wait()

    def test_idle_child_process_is_not_reported_as_activity(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            rt = _FakeRt(proc.pid)
            self.mgr._tool_process_active(rt, 60.0)  # baseline
            time.sleep(3)
            self.assertIsNone(self.mgr._tool_process_active(rt, 60.0),
                              "a sleeping child must not be mistaken for work")
        finally:
            proc.kill()
            proc.wait()

    def test_missing_pid_is_no_proof_not_a_stall_verdict(self):
        class _NoTransport:
            transport = None
        self.assertIsNone(self.mgr._tool_process_active(_NoTransport(), 60.0))

    def test_detector_never_raises_on_a_dead_pid(self):
        # A pid that certainly does not exist must degrade quietly: the
        # detector's failure must never fail a task.
        rt = _FakeRt(999999)
        try:
            self.assertIsNone(self.mgr._tool_process_active(rt, 60.0))
        except Exception as exc:  # pragma: no cover
            self.fail(f"detector raised instead of degrading: {exc}")


if __name__ == "__main__":
    unittest.main()
