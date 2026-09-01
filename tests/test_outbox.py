"""Plugin-owned durable notification outbox — focused tests.

Covers: routing snapshot persistence, backward-compatible schema migration,
enqueue/dedupe, claim/lease-expiry/requeue, retry/backoff, fresh-worker
delivery, exact host-adapter delivery arguments, no-LLM-turn delivery,
topic routing, two simultaneous tasks with different origins and no
cross-routing, gateway/send failure retry, terminal notices (settled/
failed/stalled/verifier), progress gating (first after 90 s, 180 s
interval, real progress only), and plugin shutdown/restart recovery.

Delivery goes through the plugin-local host adapter (injectable as the
worker's ``deliver`` callable here); the adapter's contract against the
stubbed native send_message_tool is covered in
``test_outbox_host_adapter.py``.

Deterministic: FakeClock + FakePiProcess; the outbox and the worker accept
explicit ``now`` values, so no real sleeps except the one threaded-worker
test, which uses a 50 ms loop and waits on a real condition (bounded).
All state lives in temp directories — never production $HERMES_HOME.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import threading
import time
import unittest
from pathlib import Path
from types import ModuleType

from test_pi_manager import (  # type: ignore
    PiManagerTestCase,
    FakePiProcess,
    fake_popen_factory,
    TEST_THRESHOLDS,
    EXEC_ABORTED,
    EXEC_CRASHED,
    EXEC_SETTLED,
    EXEC_STALLED,
    wait_until,
)
import registry_db  # type: ignore
from core import (  # type: ignore
    EXEC_RUNNING,
    PiManager,
    Thresholds,
    VerifierSpec,
)
from fakes import FakeClock  # type: ignore
from outbox import (  # type: ignore
    MAX_ATTEMPTS,
    RETRY_BACKOFF_SECONDS,
    STATUS_FAILED,
    STATUS_LEASED,
    STATUS_PENDING,
    STATUS_SENT,
    NotificationOutbox,
    OutboxWorker,
    build_target,
    classify_transient,
    extract_tool_error,
    progress_notification_id,
    terminal_notification_id,
)
from registry_db import Registry  # type: ignore

ORIGIN_A = {
    "platform": "telegram",
    "chat_id": "-1001234567890",
    "thread_id": "77",
    "session_key": "agent:main:telegram:group:-1001234567890:77",
    "scope_id": "tenant-1",
    "user_id": "u1",
    "session_id": "20260901_010000_aaa111",
}
ORIGIN_B = {
    "platform": "discord",
    "chat_id": "999888777",
    "session_key": "agent:main:discord:guild:999888777",
    "scope_id": "guild-2",
}
TARGET_A = "telegram:-1001234567890:77"
TARGET_B = "discord:999888777"


class FakeDelivery:
    """A host-adapter double: records every delivery call and returns
    scripted results. ``fail_times`` responses are a 429 (transient),
    ``permanent_error`` (when set) is returned instead of the 429, and
    ``raise_times`` responses raise instead of returning. Called exactly
    like the real adapter: ``deliver(args, **kw) -> str``."""

    def __init__(self, fail_times: int = 0, permanent_error: str = None,
                 raise_times: int = 0):
        self.calls = []
        self._fail_times = fail_times
        self._raise_times = raise_times
        self.permanent_error = permanent_error

    def __call__(self, args: dict, **kw) -> str:
        self.calls.append(dict(args))
        if self._raise_times > 0:
            self._raise_times -= 1
            raise RuntimeError("host adapter exploded")
        if self.permanent_error is not None:
            return json.dumps({"error": self.permanent_error})
        if self._fail_times > 0:
            self._fail_times -= 1
            return json.dumps({"error": "429 Too Many Requests — try again later"})
        return json.dumps({"sent": True})


class OutboxTestCase(PiManagerTestCase):
    """Adds a durable outbox bound to the temp registry and the FakeClock."""

    def setUp(self):
        super().setUp()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)

    def make_manager(self, process: FakePiProcess, **kw) -> PiManager:
        thresholds = kw.pop("default_thresholds", TEST_THRESHOLDS)
        return PiManager(
            registry=self.registry,
            popen_factory=kw.pop("popen_factory", fake_popen_factory(process)),
            clock=self.clock,
            default_thresholds=thresholds,
            outbox=self.outbox,
            **kw,
        )

    def _progress_rows(self, task_id: str):
        return self.registry.list_notifications(task_id=task_id)

    def _kinds(self, task_id: str):
        return [n["kind"] for n in self._progress_rows(task_id)]


# ---------------------------------------------------------------------------
# Target construction and error classification (pure functions)
# ---------------------------------------------------------------------------


class TestTargetConstruction(unittest.TestCase):
    def test_platform_chat_and_thread(self):
        self.assertEqual(build_target("telegram", "-100123", "77"),
                         "telegram:-100123:77")

    def test_platform_chat_only(self):
        self.assertEqual(build_target("telegram", "12345", ""), "telegram:12345")

    def test_platform_case_normalized(self):
        self.assertEqual(build_target("Telegram", "123", ""), "telegram:123")
        self.assertEqual(build_target("DISCORD", "42", None), "discord:42")

    def test_platform_neutral_not_telegram_specific(self):
        # Slack channel, Discord thread, Matrix room id with embedded colon.
        self.assertEqual(build_target("slack", "C0123ABC", "thread-9"),
                         "slack:C0123ABC:thread-9")
        self.assertEqual(build_target("matrix", "!room:server.org", ""),
                         "matrix:!room:server.org")

    def test_malformed_values_rejected_defensively(self):
        self.assertIsNone(build_target("", "123", ""))
        self.assertIsNone(build_target("telegram", "", ""))
        self.assertIsNone(build_target("telegram", None, ""))
        self.assertIsNone(build_target("has space", "123", ""))
        self.assertIsNone(build_target("telegram", "12 34", ""))
        self.assertIsNone(build_target("telegram", "a\nb", ""))
        self.assertIsNone(build_target("telegram", "x" * 201, ""))
        # Thread component must be unambiguous (no colons, no inner spaces).
        self.assertIsNone(build_target("telegram", "123", "7:7"))
        self.assertIsNone(build_target("telegram", "123", "a b"))

    def test_classification(self):
        self.assertTrue(classify_transient("429 Too Many Requests"))
        self.assertTrue(classify_transient("request timed out"))
        self.assertTrue(classify_transient("connection reset"))
        self.assertTrue(classify_transient("some unknown error"))  # default
        self.assertFalse(classify_transient("Unknown platform: foo"))
        self.assertFalse(classify_transient("No chat specified and no home channel set"))
        self.assertFalse(classify_transient("invalid target"))

    def test_extract_tool_error(self):
        self.assertEqual(extract_tool_error('{"error": "boom"}'), "boom")
        self.assertIsNone(extract_tool_error('{"sent": true}'))
        self.assertIsNone(extract_tool_error(""))
        self.assertIsNone(extract_tool_error(None))
        self.assertEqual(extract_tool_error({"error": "x"}), "x")
        self.assertEqual(extract_tool_error({"ok": True}), None)
        # Non-JSON prose back from the tool is a refusal, not a receipt.
        self.assertEqual(extract_tool_error("nope"), "nope")

    def test_stable_ids(self):
        self.assertEqual(terminal_notification_id("pi-1", "verifier"),
                         "term:pi-1:verifier")
        self.assertEqual(progress_notification_id("pi-1", 3), "prog:pi-1:3")
        self.assertNotEqual(terminal_notification_id("pi-1", "settled"),
                            terminal_notification_id("pi-1", "verifier"))


# ---------------------------------------------------------------------------
# Routing snapshot + persistence, schema migration
# ---------------------------------------------------------------------------


class TestRoutingSnapshotAndPersistence(OutboxTestCase):
    def test_origin_persisted_with_the_task(self):
        # Direct core-level check (no process needed for the registry row):
        process = FakePiProcess()
        manager = self.make_manager(process)
        result = manager.start_task(
            prompt="do it", cwd=str(self.cwd_dir), task_id="pi-rt1",
            origin=ORIGIN_A,
        )
        row = self.registry.get_task(result["task_id"])
        self.assertIsNotNone(row.get("origin"),
                             "routing snapshot must be persisted with the task")
        origin = json.loads(row["origin"])
        for key in ("platform", "chat_id", "thread_id", "session_key",
                    "scope_id", "user_id", "session_id"):
            self.assertEqual(origin[key], ORIGIN_A[key])

        nid = self.outbox.enqueue("pi-rt1", "progress", "still working")
        n = self.registry.get_notification(nid)
        self.assertEqual(n["platform"], "telegram")
        self.assertEqual(n["chat_id"], "-1001234567890")
        self.assertEqual(n["thread_id"], "77")
        self.assertEqual(n["session_key"], ORIGIN_A["session_key"])
        self.assertEqual(n["scope_id"], "tenant-1")
        self.assertEqual(n["target"], TARGET_A)
        self.assertEqual(n["status"], STATUS_PENDING)

    def test_task_without_origin_is_unroutable_but_recorded(self):
        self.registry.create_task(task_id="pi-cli", execution_state="RUNNING")
        nid = self.outbox.enqueue("pi-cli", "settled", "done")
        n = self.registry.get_notification(nid)
        self.assertIsNone(n["target"])
        self.assertIsNone(n["platform"])
        self.assertEqual(n["status"], STATUS_PENDING)

    def test_schema_migration_adds_missing_columns(self):
        # Build a pre-outbox database: the current schema minus the origin
        # column and minus the whole notifications table.
        schema = registry_db._SCHEMA
        schema = re.sub(r"CREATE TABLE IF NOT EXISTS notifications \(.*?\);",
                        "", schema, flags=re.S)
        schema = re.sub(
            r"CREATE INDEX IF NOT EXISTS idx_notifications_\w+ ON notifications\([^)]*\);\n?",
            "", schema)
        schema = schema.replace("    origin              TEXT,\n", "")
        legacy_db = self.tmp / "legacy.sqlite3"
        conn = sqlite3.connect(str(legacy_db))
        conn.executescript(schema)
        conn.execute(
            "INSERT INTO tasks (task_id, execution_state, created_at, updated_at) "
            "VALUES ('old-task-1', 'SETTLED', 1.0, 2.0)")
        conn.commit()
        conn.close()

        reg = Registry(db_path=legacy_db)
        self.addCleanup(reg.close)
        cols = {r[1] for r in reg._conn.execute("PRAGMA table_info(tasks)")}
        self.assertIn("origin", cols, "migration must add the origin column")
        old = reg.get_task("old-task-1")
        self.assertEqual(old["execution_state"], "SETTLED")
        self.assertIsNone(old["origin"], "old rows read the new column as NULL")
        # New capabilities work on the migrated database.
        reg.create_task(task_id="new-task", execution_state="RUNNING")
        reg.update_task("new-task", origin=json.dumps(ORIGIN_A))
        self.assertEqual(json.loads(reg.get_task("new-task")["origin"])["platform"],
                         "telegram")
        self.assertEqual(reg.list_notifications(limit=5), [])

    def test_reopen_same_file_keeps_rows(self):
        self.registry.create_task(task_id="pi-keep", execution_state="RUNNING")
        self.outbox.enqueue("pi-keep", "settled", "done")
        first = self.registry
        first.close()
        reopened = Registry(db_path=first.path)
        self.addCleanup(reopened.close)
        self.assertEqual(len(reopened.list_notifications(task_id="pi-keep")), 1)


# ---------------------------------------------------------------------------
# Enqueue / dedupe
# ---------------------------------------------------------------------------


class TestEnqueueDedupe(OutboxTestCase):
    def test_terminal_id_deterministic_and_deduped(self):
        self.registry.create_task(task_id="pi-d", execution_state="SETTLED",
                                  origin=json.dumps(ORIGIN_A))
        first = self.outbox.enqueue("pi-d", "verifier", "msg")
        second = self.outbox.enqueue("pi-d", "verifier", "msg again")
        third = self.outbox.enqueue("pi-d", "verifier", "msg third")
        self.assertEqual(first, terminal_notification_id("pi-d", "verifier"))
        self.assertIsNone(second, "same (task, kind) must not enqueue again")
        self.assertIsNone(third)
        row = self.registry.get_notification(first)
        self.assertEqual(row["status"], STATUS_PENDING)
        rows = self.registry.list_notifications(task_id="pi-d")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["notification_id"], first)
        self.assertEqual(rows[0]["attempts"], 0)

    def test_different_kinds_do_not_collide(self):
        self.registry.create_task(task_id="pi-d2", execution_state="SETTLED",
                                  origin=json.dumps(ORIGIN_A))
        a = self.outbox.enqueue("pi-d2", "settled", "s")
        b = self.outbox.enqueue("pi-d2", "stalled", "t")
        self.assertNotEqual(a, b)
        self.assertEqual(len(self.registry.list_notifications(task_id="pi-d2")), 2)

    def test_progress_ids_monotonic_and_across_restart(self):
        self.registry.create_task(task_id="pi-p", execution_state="RUNNING",
                                  origin=json.dumps(ORIGIN_A))
        first = self.outbox.enqueue("pi-p", "progress", "p1")
        second = self.outbox.enqueue("pi-p", "progress", "p2")
        self.assertEqual(first, "prog:pi-p:1")
        self.assertEqual(second, "prog:pi-p:2")
        # Manager restart: a fresh Registry on the same file must keep the
        # sequence monotonic.
        self.registry.close()
        reopened = Registry(db_path=self.registry.path)
        self.addCleanup(reopened.close)
        reopened_outbox = NotificationOutbox(reopened, now_fn=self.clock)
        third = reopened_outbox.enqueue("pi-p", "progress", "p3")
        self.assertEqual(third, "prog:pi-p:3")

    def test_message_is_bounded(self):
        self.registry.create_task(task_id="pi-b", execution_state="SETTLED",
                                  origin=json.dumps(ORIGIN_A))
        nid = self.outbox.enqueue("pi-b", "settled", "x" * 9000)
        n = self.registry.get_notification(nid)
        self.assertLessEqual(len(n["message"]), 4100)
        self.assertIn("truncated", n["message"])


# ---------------------------------------------------------------------------
# Claim / lease / expiry / requeue
# ---------------------------------------------------------------------------


class TestClaimLeaseRequeue(OutboxTestCase):
    def _seed(self, task_id="pi-c", kind="settled"):
        self.registry.create_task(task_id=task_id, execution_state=EXEC_SETTLED,
                                  origin=json.dumps(ORIGIN_A))
        return self.outbox.enqueue(task_id, kind, "body")

    def test_claim_leases_and_bumps_attempts(self):
        nid = self._seed()
        rows = self.outbox.claim("w1", lease_seconds=30.0, now=1000.0)
        self.assertEqual(len(rows), 1)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_LEASED)
        self.assertEqual(row["worker_id"], "w1")
        self.assertEqual(row["lease_until"], 1030.0)
        self.assertEqual(row["attempts"], 1)
        # A second claim at the same instant gets nothing (row is leased).
        self.assertEqual(self.outbox.claim("w2", lease_seconds=30.0, now=1000.0), [])

    def test_not_yet_due_rows_are_not_claimed(self):
        self._seed()
        self.registry.set_notification_retry("term:pi-c:settled", 1500.0, "429")
        self.assertEqual(self.outbox.claim("w1", lease_seconds=30.0, now=1000.0), [])
        rows = self.outbox.claim("w1", lease_seconds=30.0, now=1500.0)
        self.assertEqual([r["notification_id"] for r in rows], ["term:pi-c:settled"])
        self.assertEqual(self.registry.get_notification("term:pi-c:settled")["status"],
                         STATUS_LEASED)

    def test_expired_lease_returns_to_pending_and_requeues(self):
        nid = self._seed()
        self.outbox.claim("w1", lease_seconds=30.0, now=1000.0)
        # w1 dies mid-send (no mark_sent). Before expiry nothing is due:
        self.assertEqual(self.outbox.claim("w2", lease_seconds=30.0, now=1029.0), [])
        # Past expiry the stale lease is requeued for the new worker.
        rows = self.outbox.claim("w2", lease_seconds=30.0, now=1031.0)
        self.assertEqual([r["notification_id"] for r in rows], [nid])
        row = self.registry.get_notification(nid)
        self.assertEqual(row["worker_id"], "w2")
        self.assertEqual(row["attempts"], 2, "a redelivered row counts a new attempt")

    def test_expired_lease_survives_a_full_restart(self):
        nid = self._seed()
        self.outbox.claim("worker-old", lease_seconds=30.0, now=1000.0)
        # "Restart": a brand-new Registry + outbox on the same file.
        self.registry.close()
        reopened = Registry(db_path=self.registry.path)
        self.addCleanup(reopened.close)
        fresh = NotificationOutbox(reopened, now_fn=self.clock)
        rows = fresh.claim("worker-new", lease_seconds=30.0, now=1040.0)
        self.assertEqual([r["notification_id"] for r in rows], [nid])
        self.assertEqual(reopened.get_notification(nid)["worker_id"], "worker-new")

    def test_claim_respects_limit(self):
        for i in range(5):
            self.registry.create_task(task_id=f"pi-l{i}", execution_state=EXEC_SETTLED,
                                      origin=json.dumps(ORIGIN_A))
            self.outbox.enqueue(f"pi-l{i}", "settled", f"body {i}")
        rows = self.outbox.claim("w1", lease_seconds=30.0, limit=3, now=1000.0)
        self.assertEqual(len(rows), 3)

    def test_mark_sent_clears_lease(self):
        nid = self._seed()
        self.outbox.claim("w1", lease_seconds=30.0, now=1000.0)
        self.outbox.mark_sent(nid, now=1005.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_SENT)
        self.assertEqual(row["sent_at"], 1005.0)
        self.assertIsNone(row["lease_until"])
        self.assertIsNone(row["worker_id"])
        self.assertEqual(self.outbox.claim("w2", lease_seconds=30.0, now=2000.0), [])


# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------


class TestRetryBackoff(OutboxTestCase):
    def test_transient_failures_follow_the_bounded_schedule(self):
        self.registry.create_task(task_id="pi-r", execution_state=EXEC_SETTLED,
                                  origin=json.dumps(ORIGIN_A))
        nid = self.outbox.enqueue("pi-r", "settled", "done")
        delivery = FakeDelivery(fail_times=99)
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)

        t = 0.0
        for attempt in range(1, MAX_ATTEMPTS + 1):
            # Due for attempt 1 (created, never retried); afterwards each
            # retry becomes due one second past its backoff deadline.
            self.clock.advance(t if attempt == 1 else RETRY_BACKOFF_SECONDS[attempt - 2] + 1.0)
            now = self.clock()
            self.assertEqual(worker.run_once(now=now), 1,
                             f"attempt {attempt} must claim exactly the row")
            row = self.registry.get_notification(nid)
            if attempt < MAX_ATTEMPTS:
                delay = RETRY_BACKOFF_SECONDS[attempt - 1]
                self.assertEqual(row["status"], STATUS_PENDING,
                                 f"attempt {attempt} should requeue")
                self.assertEqual(row["attempts"], attempt)
                self.assertAlmostEqual(row["next_retry_at"], now + delay)
                self.assertIn("429", row["last_error"])
            else:
                # The 6th failed attempt is the bounded end of the line.
                self.assertEqual(row["status"], STATUS_FAILED,
                                 "after MAX_ATTEMPTS the row must stop retrying")
                self.assertEqual(row["attempts"], MAX_ATTEMPTS)
                self.assertEqual(len(delivery.calls), MAX_ATTEMPTS)

        # Nothing further is ever claimed: the failure is terminal and the
        # worker loop has bounded work.
        self.clock.advance(10 * 3600)
        self.assertEqual(worker.run_once(now=self.clock()), 0)
        self.assertEqual(len(delivery.calls), MAX_ATTEMPTS)

        self.assertEqual(list(RETRY_BACKOFF_SECONDS), [5, 15, 60, 300, 900])

    def test_permanent_error_fails_immediately(self):
        self.registry.create_task(task_id="pi-pm", execution_state=EXEC_SETTLED,
                                  origin=json.dumps(ORIGIN_A))
        nid = self.outbox.enqueue("pi-pm", "settled", "done")
        delivery = FakeDelivery(permanent_error="Unknown platform: foobar")
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_FAILED)
        self.assertEqual(row["attempts"], 1)
        self.assertIn("Unknown platform", row["last_error"])
        # And it stays there, no matter how much time passes.
        self.clock.advance(10 * 3600)
        self.assertEqual(worker.run_once(now=self.clock()), 0)
        self.assertEqual(len(delivery.calls), 1)

    def test_delivery_raising_is_transient_and_bounded(self):
        self.registry.create_task(task_id="pi-e", execution_state=EXEC_SETTLED,
                                  origin=json.dumps(ORIGIN_A))
        nid = self.outbox.enqueue("pi-e", "settled", "done")
        delivery = FakeDelivery(raise_times=1)
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_PENDING)
        self.assertEqual(row["attempts"], 1)
        self.assertIn("host adapter raised", row["last_error"])
        # Recovery: the next due pass delivers fine.
        self.clock.advance(6.0)
        worker.run_once(now=self.clock())
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_SENT)
        self.assertEqual(len(delivery.calls), 2)

    def test_unroutable_target_fails_permanently_with_reason(self):
        self.registry.create_task(task_id="pi-x", execution_state=EXEC_SETTLED)
        nid = self.outbox.enqueue("pi-x", "settled", "done")
        delivery = FakeDelivery()
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_FAILED)
        self.assertEqual(row["attempts"], 1)
        self.assertIn("no routable target", row["last_error"])
        self.assertEqual(delivery.calls, [],
                         "nothing may be delivered for a dead target")


# ---------------------------------------------------------------------------
# Fresh-worker delivery + exact host-adapter arguments
# ---------------------------------------------------------------------------


class TestWorkerDelivery(OutboxTestCase):
    def _seed(self, task_id="pi-w", origin=ORIGIN_A, kind="settled"):
        self.registry.create_task(task_id=task_id, execution_state=EXEC_SETTLED,
                                  origin=json.dumps(origin))
        return self.outbox.enqueue(task_id, kind, "Pi task " + task_id + " finished.")

    def test_host_adapter_arguments_are_exact(self):
        nid = self._seed("pi-arg")
        delivery = FakeDelivery()
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        worker.run_once(now=1000.0)
        self.assertEqual(len(delivery.calls), 1)
        args = delivery.calls[0]
        self.assertEqual(args, {
            "action": "send",
            "target": TARGET_A,
            "message": "Pi task pi-arg finished.",
        })
        self.assertEqual(self.registry.get_notification(nid)["status"], STATUS_SENT)

    def test_only_send_action_is_ever_delivered(self):
        """The whole delivery path is one host-adapter call per row:
        no LLM/agent turn, no other action, nothing from the private
        rails."""
        for i in range(3):
            self._seed(f"pi-no{i}")
        delivery = FakeDelivery()
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=1000.0), 3)
        self.assertEqual(worker.run_once(now=1001.0), 0)
        self.assertTrue(delivery.calls,
                        "the worker must deliver through the host adapter")
        for args in delivery.calls:
            self.assertEqual(args["action"], "send")
        self.assertEqual(len(delivery.calls), 3)

    def test_fresh_worker_at_registration_replaces_stale_one(self):
        nid = self._seed("pi-fresh")
        stale = FakeDelivery()
        stale_worker = OutboxWorker(self.outbox, deliver=stale, now_fn=self.clock)
        fresh = FakeDelivery()
        fresh_worker = OutboxWorker(self.outbox, deliver=fresh, now_fn=self.clock)
        # The re-registration story: the new worker runs, the old one is
        # stopped and must never deliver again.
        fresh_worker.run_once(now=1000.0)
        stale_worker.stop()
        self.assertEqual(fresh.calls and len(fresh.calls), 1)
        self.assertEqual(stale.calls, [],
                         "the stale worker must deliver nothing")
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_SENT)

    def test_send_failure_then_success_is_retried(self):
        nid = self._seed("pi-flaky")
        delivery = FakeDelivery(fail_times=1)
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_PENDING)
        self.assertEqual(row["attempts"], 1)
        self.clock.advance(6.0)
        worker.run_once(now=self.clock())
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_SENT)
        self.assertEqual(len(delivery.calls), 2)

    def test_threaded_worker_delivers_and_stops_cleanly(self):
        # Real clock + short loop: this is the one test that uses wall time,
        # bounded by a 5 s deadline and a clean stop().
        self._seed("pi-thread")
        delivery = FakeDelivery()
        worker = OutboxWorker(self.outbox, deliver=delivery, interval_seconds=0.05)
        worker.start()
        self.addCleanup(worker.stop)
        self.assertTrue(wait_until(
            lambda: self.registry.get_notification(
                "term:pi-thread:settled")["status"] == STATUS_SENT,
            timeout=5.0), "threaded worker did not deliver within 5 s")
        worker.stop(timeout=5.0)
        self.assertFalse(worker.running())
        # After a clean stop no further delivery happens, even after time.
        calls_after = len(delivery.calls)
        time.sleep(0.2)
        self.assertEqual(len(delivery.calls), calls_after)

    def test_stop_is_a_noop_when_never_started(self):
        worker = OutboxWorker(self.outbox, deliver=FakeDelivery())
        worker.stop(timeout=0.1)  # must not raise or block


# ---------------------------------------------------------------------------
# Two simultaneous tasks, different origins, no cross-routing
# ---------------------------------------------------------------------------


class TestTwoTasksNoCrossRouting(OutboxTestCase):
    def test_each_task_delivered_to_its_own_origin(self):
        pa, pb = FakePiProcess(), FakePiProcess()
        pa.state["sessionId"] = "sess-a"
        pb.state["sessionId"] = "sess-b"
        spawned = []

        def factory(argv, cwd):
            p = pa if len(spawned) == 0 else pb
            spawned.append(p)
            p.sync_session_state_from_argv(argv)
            return p

        manager = self.make_manager(pa, popen_factory=factory)
        task_a = self.start_and_boot(manager, pa, origin=ORIGIN_A)
        # Rewire: task B boots against process B.
        task_b = manager.start_task(prompt="second", cwd=str(self.cwd_dir),
                                    task_id="pi-task-b", origin=ORIGIN_B)
        task_b = task_b["task_id"]
        self.assertTrue(pb.wait_until_command("prompt"))
        self.assertTrue(wait_until(
            lambda: manager.status(task_b)["execution_state"] == EXEC_RUNNING))

        pa.emit({"type": "agent_settled"})
        pb.emit({"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_a)["execution_state"] == EXEC_SETTLED
            and manager.status(task_b)["execution_state"] == EXEC_SETTLED))
        self.assertTrue(wait_until(
            lambda: len(self.registry.list_notifications(task_id=task_a)) >= 1
            and len(self.registry.list_notifications(task_id=task_b)) >= 1))

        delivery = FakeDelivery()
        worker = OutboxWorker(self.outbox, deliver=delivery, now_fn=self.clock)
        delivered = 0
        while delivered < 2:
            delivered += worker.run_once(now=self.clock())
            if delivered < 2:
                time.sleep(0.01)

        by_target = {}
        for args in delivery.calls:
            by_target.setdefault(args["target"], []).append(args["message"])
        self.assertEqual(set(by_target), {TARGET_A, TARGET_B},
                         f"cross-routing suspected: {sorted(by_target)}")
        a_msgs = by_target[TARGET_A]
        b_msgs = by_target[TARGET_B]
        self.assertTrue(all(task_a in m for m in a_msgs))
        self.assertTrue(all(task_b in m for m in b_msgs))
        self.assertTrue(all(task_b not in m for m in a_msgs),
                        "task B's id must never reach task A's target")
        self.assertTrue(all(task_a not in m for m in b_msgs))


# ---------------------------------------------------------------------------
# Terminal notices: settled / failed / stalled / verifier
# ---------------------------------------------------------------------------


class TestTerminalNotices(OutboxTestCase):
    def test_ungated_settle_produces_settled_notice(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: any(n["kind"] == "settled" for n in
                        self.registry.list_notifications(task_id=task_id))))
        n = self.registry.get_notification(terminal_notification_id(task_id, "settled"))
        self.assertIn("Pi zakończył zadanie", n["message"])
        self.assertIn(task_id, n["message"])
        self.assertIn("Weryfikacja: nie uruchomiono (brak weryfikatora)", n["message"])
        self.assertNotIn("pi_digest", n["message"])
        self.assertNotIn("pi_status", n["message"])
        self.assertEqual(n["status"], STATUS_PENDING)  # immediate: due at created_at

    def test_verifier_notice_carries_the_real_verdict_pass(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(0)"]))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: any(n["kind"] == "verifier" for n in
                        self.registry.list_notifications(task_id=task_id))))
        n = self.registry.get_notification(terminal_notification_id(task_id, "verifier"))
        self.assertIn("Weryfikacja: PASS", n["message"])
        self.assertIn("Pi zakończył zadanie", n["message"])
        self.assertNotIn("Weryfikacja: nie uruchomiono", n["message"])
        # No second, older-style completion notice of a different kind.
        self.assertEqual(self._kinds(task_id), ["verifier"])

    def test_verifier_notice_carries_fail_verdict(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(1)"]))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: any(n["kind"] == "verifier" for n in
                        self.registry.list_notifications(task_id=task_id))))
        n = self.registry.get_notification(terminal_notification_id(task_id, "verifier"))
        self.assertIn("Weryfikacja: FAIL", n["message"])
        self.assertIn("wymagają przeglądu", n["message"])

    def test_crash_produces_failed_notice(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.finish(7)  # dies before settlement
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_CRASHED)
        n = self.registry.get_notification(terminal_notification_id(task_id, "failed"))
        self.assertIsNotNone(n)
        self.assertIn("CRASHED", n["message"])
        self.assertIn("kod wyjścia 7", n["message"])

    def test_abort_produces_failed_notice(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        manager.abort_task(task_id, reason="manual_abort")
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_ABORTED)
        n = self.registry.get_notification(terminal_notification_id(task_id, "failed"))
        self.assertIsNotNone(n)
        self.assertIn("ABORTED", n["message"])

    def test_stall_produces_stalled_notice_exactly_once(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        for _ in range(12):  # pure silence past soft stall + grace
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)
        rows = [n for n in self.registry.list_notifications(task_id=task_id)
                if n["kind"] == "stalled"]
        self.assertEqual(len(rows), 1, "entering STALLED notifies exactly once")
        self.assertIn("STALLED", rows[0]["message"])

    def test_notification_ids_dedupe_across_hooks(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(
            manager, process,
            verifier=VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(0)"]))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["verification_state"] == "PASS"))
        # Double-fire the terminal path (e.g. recovery re-ran the gate):
        manager._notify_done(task_id)
        manager._notify_failed(task_id)
        kinds = sorted(self._kinds(task_id))
        self.assertEqual(kinds, ["failed", "verifier"],
                         f"each (task, kind) may appear once: {kinds}")

    def test_recovery_rerun_of_verifier_does_not_duplicate(self):
        # Reconstruct the exact state a restart finds: a task that settled
        # while its verifier was still PENDING (the process died between
        # settlement and the verdict). Recovery re-runs the gate and
        # announces — and a second recovery pass must not announce again.
        import json as _json
        argv = [sys.executable, "-c", "import sys; sys.exit(0)"]
        self.registry.create_task(
            task_id="pi-recover", execution_state=EXEC_SETTLED,
            verification_state="PENDING",
            verifier_spec=_json.dumps({"argv": argv, "timeout_seconds": 10.0}),
            origin=_json.dumps(ORIGIN_A),
            started_at=1.0, settled_at=2.0, created_at=1.0, updated_at=2.0,
        )
        manager = self.make_manager(FakePiProcess())
        result = manager._recover_settled_verification(
            "pi-recover", self.registry.get_task("pi-recover"),
            VerifierSpec(argv=argv, timeout_seconds=10.0))
        self.assertEqual(result["verification_run"], True)
        self.assertEqual(result["verification_state"], "PASS")
        rows = self.registry.list_notifications(task_id="pi-recover")
        self.assertEqual([n["kind"] for n in rows], ["verifier"])
        # Recovery pass again (idempotent: verification already resolved).
        result2 = manager._recover_settled_verification(
            "pi-recover", self.registry.get_task("pi-recover"),
            VerifierSpec(argv=argv, timeout_seconds=10.0))
        self.assertEqual(result2["verification_run"], False)
        self.assertEqual(len(self.registry.list_notifications(task_id="pi-recover")), 1)
        # And an explicit re-announcement is still a local no-op.
        manager._notify_done("pi-recover")
        self.assertEqual(len(self.registry.list_notifications(task_id="pi-recover")), 1)


# ---------------------------------------------------------------------------
# Progress gating: first after 90 s, at most every 180 s, real progress only
# ---------------------------------------------------------------------------


class TestProgressGating(OutboxTestCase):
    def test_progress_gate_first_interval_and_real_progress_only(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        rt = manager._rt(task_id)
        self.assertEqual(self._kinds(task_id), [])

        # (1) Within the first window (90 s) nothing goes out even on
        # real progress.
        self.clock.advance(60)
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._kinds(task_id), [])

        # (2) Past the first window, real progress emits exactly one notice.
        self.clock.advance(40)  # t=100 > 90
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._kinds(task_id), ["progress"])
        self.assertEqual(self._progress_rows(task_id)[0]["notification_id"],
                         f"prog:{task_id}:1")

        # (3) Inside the 180 s interval nothing more, even on progress.
        self.clock.advance(50)  # t=150, 50 s since the last notice
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(len(self._kinds(task_id)), 1)

        # (4) Past the interval with NEW real progress: second notice.
        self.clock.advance(150)  # t=300, 200 s since the last notice
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(len(self._kinds(task_id)), 2)
        self.assertEqual(self._progress_rows(task_id)[1]["notification_id"],
                         f"prog:{task_id}:2")

        # (5) Past the interval but NO real progress (silence): no notice.
        self.clock.advance(400)
        manager._notify_progress(task_id, rt)
        self.assertEqual(len(self._kinds(task_id)), 2)

        # (6) Real progress again: third, still monotonic.
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(len(self._kinds(task_id)), 3)
        self.assertEqual(self._progress_rows(task_id)[2]["notification_id"],
                         f"prog:{task_id}:3")

        # Bodies are bounded chat messages, not transcripts.
        for n in self._progress_rows(task_id):
            self.assertLessEqual(len(n["message"]), 4001)
            self.assertIn(task_id, n["message"])

    def test_no_progress_notices_after_final_state(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        rt = manager._rt(task_id)
        self.clock.advance(3600)
        manager._notify_progress(task_id, rt)  # must be a no-op
        self.assertEqual([k for k in self._kinds(task_id) if k == "progress"], [])
        self.assertEqual([k for k in self._kinds(task_id) if k == "settled"],
                         ["settled"])


# ---------------------------------------------------------------------------
# Plugin shutdown / restart recovery end to end
# ---------------------------------------------------------------------------


class TestShutdownRestartRecovery(OutboxTestCase):
    def test_worker_death_and_restart_delivers_everything(self):
        self.registry.create_task(task_id="pi-sr", execution_state=EXEC_SETTLED,
                                  origin=json.dumps(ORIGIN_A))
        pending_nid = self.outbox.enqueue("pi-sr", "settled", "completion body",
                                          now=999.0)
        # A queued-but-unclaimed row (the manager died before first drain).
        self.registry.create_task(task_id="pi-sr2", execution_state=EXEC_CRASHED,
                                  origin=json.dumps(ORIGIN_B))
        queued_nid = self.outbox.enqueue("pi-sr2", "failed", "failure body",
                                         now=1000.0)

        # Worker A claims the first row, then dies mid-send.
        outbox_a = self.outbox
        worker_a = OutboxWorker(outbox_a, deliver=FakeDelivery(), now_fn=self.clock)
        claimed = outbox_a.claim(worker_a.worker_id, 30.0, limit=1, now=1000.0)
        self.assertEqual([r["notification_id"] for r in claimed], [pending_nid])

        # RESTART: fresh Registry on the same file, fresh outbox, fresh
        # delivery.
        self.registry.close()
        reopened = Registry(db_path=self.registry.path)
        self.addCleanup(reopened.close)
        outbox_b = NotificationOutbox(reopened, now_fn=self.clock)
        delivery_b = FakeDelivery()
        worker_b = OutboxWorker(outbox_b, deliver=delivery_b, now_fn=self.clock)
        self.clock.advance(40.0)  # past the dead worker's lease
        worker_b.run_once(now=self.clock())
        worker_b.run_once(now=self.clock())  # drain both rows

        sent_a = reopened.get_notification(pending_nid)
        sent_b = reopened.get_notification(queued_nid)
        self.assertEqual(sent_a["status"], STATUS_SENT)
        self.assertEqual(sent_a["attempts"], 2, "redelivered after lease expiry")
        self.assertEqual(sent_b["status"], STATUS_SENT)
        self.assertEqual(sent_b["attempts"], 1, "the unclaimed row needed no retry")
        self.assertEqual(len(delivery_b.calls), 2)
        targets = sorted(args["target"] for args in delivery_b.calls)
        self.assertEqual(targets, sorted([TARGET_A, TARGET_B]))
        bodies = sorted(args["message"] for args in delivery_b.calls)
        self.assertEqual(bodies, ["completion body", "failure body"])


if __name__ == "__main__":
    unittest.main()