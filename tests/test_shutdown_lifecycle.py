"""Shutdown / teardown lifecycle — focused tests.

Demonstrated race (2026-09-01): after a test closes the SQLite
connection, the daemon ``pi-task`` / ``pi-watchdog`` threads can still
reach the Registry, and a late writer raised
``sqlite3.ProgrammingError: Cannot operate on a closed database`` out of
the daemon thread on every ordinary teardown (observed in the
``pi-task-pi-rt1`` thread at ``_boot_and_run -> _apply_state_snapshot ->
registry.update_task``).

These tests pin the contract:

- a closed Registry is a no-op: every public method returns its safe
  default instead of raising, including from a late writer thread;
- ``close()`` is idempotent (setUp/tearDown and addCleanup both close);
- ``PiManager.shutdown()`` stops AND joins every watchdog (bounded),
  and no new watchdog can be started afterwards;
- a live task (booted, RUNNING, watchdog and event-reader threads still
  alive) can be torn down with ``shutdown()`` + ``registry.close()``
  with no thread exception: a late event after close is dropped
  silently and the daemon threads drain to zero.
"""

from __future__ import annotations

import threading
import time
import unittest

from test_pi_manager import (  # type: ignore
    FakePiProcess,
    PiManagerTestCase,
    wait_until,
)
from core import EXEC_RUNNING  # type: ignore


class TestClosedRegistryNoOps(PiManagerTestCase):
    """The demonstrated crash site: registry calls made after close()."""

    def test_public_methods_return_safe_defaults_after_close(self):
        self.registry.create_task(task_id="pi-cl1", execution_state="RUNNING")
        self.registry.append_event("pi-cl1", "test", "structural_event")
        self.registry.close()

        # Every call a surviving daemon thread makes must be a silent
        # no-op with a type-correct default — never a ProgrammingError.
        self.assertIsNone(self.registry.update_task("pi-cl1", execution_state="SETTLED"))
        self.assertIsNone(self.registry.get_task("pi-cl1"))
        self.assertEqual(self.registry.list_tasks(), [])
        self.assertIsNone(self.registry.append_event("pi-cl1", "late", "late_event"))
        self.assertEqual(self.registry.recent_events("pi-cl1"), [])
        self.assertIsNone(self.registry.create_task(task_id="pi-cl2",
                                                     execution_state="STARTING"))
        self.assertFalse(self.registry.insert_notification(
            {"notification_id": "x", "task_id": "pi-cl1", "kind": "settled",
             "message": "late", "status": "pending", "attempts": 0,
             "created_at": 1.0}))
        self.assertEqual(self.registry.progress_seq("pi-cl1"), 0)
        self.assertEqual(self.registry.requeue_expired_leases(1.0), 0)
        self.assertEqual(self.registry.claim_notifications(1.0, "w", 30.0), [])
        self.assertIsNone(self.registry.set_notification_sent("x", 1.0))
        self.assertIsNone(self.registry.set_notification_retry("x", 1.0, "e"))
        self.assertIsNone(self.registry.fail_notification("x", "e"))
        self.assertIsNone(self.registry.get_notification("x"))
        self.assertEqual(self.registry.list_notifications(task_id="pi-cl1"), [])
        self.assertEqual(self.registry.prune_events(), 0)

    def test_late_writer_thread_after_close_is_exception_free(self):
        self.registry.create_task(task_id="pi-cl3", execution_state="RUNNING")
        self.registry.close()

        errors = []

        def late_writer():
            # The exact call shape of the demonstrated race
            # (_apply_state_snapshot -> update_task with snapshot fields).
            try:
                self.registry.update_task(
                    "pi-cl3", execution_state="RUNNING", last_state_at=1.0,
                    last_state_hash="deadbeef", last_progress_at=1.0,
                    message_count=1, is_streaming=0, is_compacting=0,
                )
                self.registry.append_event("pi-cl3", "manager", "state_transition",
                                           None, "RUNNING")
                self.registry.get_task("pi-cl3")
                self.registry.recent_events("pi-cl3")
                self.registry.list_tasks()
                self.registry.prune_events()
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=late_writer, name="late-writer", daemon=True)
        thread.start()
        thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [],
                         "a registry call after close() must never raise")

    def test_close_is_idempotent(self):
        self.registry.create_task(task_id="pi-cl4", execution_state="RUNNING")
        self.registry.close()
        self.registry.close()  # setUp/cleanup and addCleanup both close
        self.assertIsNone(self.registry.get_task("pi-cl4"))

    def test_close_races_a_hot_writer_without_any_exception(self):
        """The demonstrated crash was a TOCTOU: the post-close check had to
        share the same lock as the execute. A writer thread hammering
        update_task while the main thread closes the registry must never
        see a ProgrammingError — either it holds the lock and finishes on
        an open connection, or it observes _closed and no-ops."""
        self.registry.create_task(task_id="pi-cl5", execution_state="RUNNING")
        errors = []
        stop = threading.Event()

        def hot_writer():
            i = 0
            while not stop.is_set():
                try:
                    self.registry.update_task("pi-cl5", message_count=i % 1000)
                    self.registry.get_task("pi-cl5")
                except Exception as exc:
                    errors.append(exc)
                    return
                i += 1

        writer = threading.Thread(target=hot_writer, name="hot-writer", daemon=True)
        writer.start()
        time.sleep(0.1)  # let the writer get hot
        self.registry.close()
        stop.set()
        writer.join(timeout=5.0)
        self.assertFalse(writer.is_alive())
        self.assertEqual(errors, [],
                         "close() racing a writer must never raise")


class TestManagerShutdownLifecycle(PiManagerTestCase):
    def test_shutdown_stops_and_joins_watchdog_and_blocks_new_ones(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        rt = manager._rt(task_id)
        self.assertTrue(wait_until(
            lambda: (rt.watchdog_thread is not None
                     and rt.watchdog_thread.is_alive())),
            "booted task must have a live watchdog thread")

        manager.shutdown()
        self.assertFalse(rt.watchdog_thread.is_alive(),
                         "shutdown() must join the watchdog thread")
        # Idempotent.
        manager.shutdown()
        # A still-booting task thread must not resurrect a watchdog.
        manager._start_watchdog(task_id)
        watchdogs = [t for t in threading.enumerate()
                     if t.name == f"pi-watchdog-{task_id}"]
        self.assertEqual(watchdogs, [])

    def test_live_task_torn_down_with_registry_close_no_thread_errors(self):
        """The demonstrated end-to-end race: a RUNNING task (boot thread
        done, watchdog + event-reader threads alive) is shut down and the
        registry closed; a late event after close must be dropped without
        any thread exception, and all daemon threads for the task drain
        to zero."""
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_start"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_RUNNING))

        manager.shutdown()
        self.registry.close()

        # Late event after close: the reader thread delivers it to
        # _on_event, which now touches the closed registry — the exact
        # path that raised ProgrammingError before the fix.
        process.emit({"type": "agent_settled"})
        process.finish(0)  # close the fake stdout so the reader thread exits

        deadline = time.time() + 5.0
        alive = []
        while time.time() < deadline:
            alive = [t for t in threading.enumerate()
                     if t.name.startswith(("pi-task-", "pi-watchdog-",
                                           "pi-verify-")) and task_id in t.name]
            if not alive:
                break
            time.sleep(0.05)
        self.assertEqual(
            [t.name for t in alive], [],
            "daemon threads for the torn-down task must drain; a thread "
            "still alive here would mean it blocked on the closed registry")
        # The registry stayed exception-free throughout (tearDown closes
        # again; a broken registry would have surfaced here or above).
        self.assertIsNone(self.registry.get_task(task_id))


if __name__ == "__main__":
    unittest.main()