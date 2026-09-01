"""The event log must stop growing without bound.

Nothing had ever deleted an event row. By 2026-08-28 the live registry held
336 353 events in 46 MB, of which 315 195 (94%) were message_update — a field
tasks.last_event_at already records to the second. The audit trail this plugin
exists to provide is the structural events; the chatter is not evidence.
"""
from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLUGIN_DIR))
sys.path.insert(0, str(PLUGIN_DIR / "tests"))

from test_pi_manager import PiManagerTestCase, FakePiProcess  # type: ignore
from registry_db import HIGH_VOLUME_EVENTS, PRUNE_MIN_AGE_SECONDS  # type: ignore


class TestEventRetention(PiManagerTestCase):
    def _count(self, event_type: str | None = None) -> int:
        sql = "SELECT COUNT(*) FROM events"
        args: tuple = ()
        if event_type:
            sql += " WHERE event_type = ?"
            args = (event_type,)
        with self.registry._lock:
            return self.registry._conn.execute(sql, args).fetchone()[0]

    def _add(self, event_type: str, n: int, age_seconds: float) -> None:
        ts = time.time() - age_seconds
        for _ in range(n):
            self.registry.append_event("t1", "rpc", event_type, ts=ts)

    def test_aged_chatter_is_dropped(self):
        self._add("message_update", 200, age_seconds=48 * 3600)
        self.assertEqual(self.registry.prune_events(), 200)
        self.assertEqual(self._count("message_update"), 0)

    def test_structural_events_are_never_dropped(self):
        # Old enough and numerous enough to be pruned if type were ignored.
        for etype in ("state_transition", "verifier_run", "abort_requested",
                      "agent_settled", "identity_established"):
            self._add(etype, 50, age_seconds=90 * 24 * 3600)
        before = self._count()
        self.assertEqual(self.registry.prune_events(), 0)
        self.assertEqual(self._count(), before,
                         "the audit trail must survive retention entirely")

    def test_chatter_inside_the_window_is_kept(self):
        # A task being diagnosed right now needs its full trace.
        self._add("message_update", 500, age_seconds=60)
        self.assertEqual(self.registry.prune_events(), 0)
        self.assertEqual(self._count("message_update"), 500)

    def test_only_rows_past_the_window_go(self):
        # Reads the shipped window rather than a literal, so shortening it
        # cannot leave this test silently asserting the old boundary.
        window = PRUNE_MIN_AGE_SECONDS
        self._add("message_update", 10, age_seconds=window - 60)   # inside
        self._add("message_update", 7, age_seconds=window + 60)    # outside
        self.assertEqual(self.registry.prune_events(), 7)
        self.assertEqual(self._count("message_update"), 10)

    def test_the_window_outlasts_the_longest_real_delegation(self):
        # The slowest measured delegations are 49 min and 27.5 min; a task
        # still running must never have its trace pruned underneath it.
        self.assertGreaterEqual(PRUNE_MIN_AGE_SECONDS, 2 * 3600)

    def test_every_high_volume_type_is_covered(self):
        for etype in HIGH_VOLUME_EVENTS:
            self._add(etype, 30, age_seconds=48 * 3600)
        self.registry.prune_events()
        self.assertEqual(self._count(), 0)


class TestSnapshotIsNotRepeated(PiManagerTestCase):
    """An unchanged diagnostic_snapshot is noise, not evidence.

    The watchdog ticks every 2 s, so a quiet task wrote an identical row every
    tick: on 2026-08-28 pi-2c445bccbdf7 filled the log with snapshots all
    reporting messageCount=53, isStreaming=1, process_alive=true.
    """

    def _snapshots(self, task_id: str):
        return [e for e in self.registry.recent_events(task_id, limit=500)
                if e["event_type"] == "diagnostic_snapshot"]

    def setUp(self):
        super().setUp()
        self.process = FakePiProcess()
        self.manager = self.make_manager(self.process)
        self.task_id = self.start_and_boot(self.manager, self.process)
        self.rt = self.manager._rt(self.task_id)

    def test_identical_snapshots_are_suppressed(self):
        state = {"isStreaming": True, "messageCount": 53}
        for _ in range(30):
            self.manager._diagnostic_snapshot(self.task_id, self.rt, state)
        self.assertEqual(len(self._snapshots(self.task_id)), 1,
                         "30 identical ticks must leave one row, not 30")

    def test_a_changed_snapshot_is_recorded_at_once(self):
        self.manager._diagnostic_snapshot(
            self.task_id, self.rt, {"isStreaming": True, "messageCount": 53})
        self.manager._diagnostic_snapshot(
            self.task_id, self.rt, {"isStreaming": True, "messageCount": 54})
        self.assertEqual(len(self._snapshots(self.task_id)), 2,
                         "progress must never be suppressed by the noise filter")

    def test_a_quiet_task_still_leaves_a_periodic_trace(self):
        state = {"isStreaming": True, "messageCount": 53}
        self.manager._diagnostic_snapshot(self.task_id, self.rt, state)
        self.clock.advance(self.rt.thresholds.snapshot_repeat_seconds + 1)
        self.manager._diagnostic_snapshot(self.task_id, self.rt, state)
        self.assertEqual(len(self._snapshots(self.task_id)), 2,
                         "suppression must be rate-limiting, never silence")


if __name__ == "__main__":
    unittest.main()
