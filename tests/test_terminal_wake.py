"""Terminal continuation wake — focused tests.

Covers the user-approved terminal wake of the original orchestrator session
(one durable wake per task, after the verifier resolves; NEVER on manual
abort; progress stays passive; the Telegram outbox rail is untouched):

- registry migration + defaults (old DBs gain the wake columns; existing
  rows stay wake-disabled; new gateway/Telegram tasks with a valid
  origin.session_key default enabled);
- exactly one wake per terminal task (idempotent across re-observations
  and recovery re-runs);
- verifier resolution strictly before the wake;
- manual abort -> no wake; CRASHED / STALLED / recovery failure -> wake;
- crash-safe state machine: pending -> dispatching -> accepted on True;
  False/exception -> bounded retry pending; a dispatching row that survives
  a restart becomes 'uncertain' and is NEVER retried (no duplicate
  orchestrator turn);
- exact session_key routing and the small wake message shape;
- no progress wake, silent first-progress suppression (baseline fix);
- no main-agent turn caused by the Telegram notification rail.

Deterministic: FakeClock + FakePiProcess + a PluginContext double pinning
the REAL public signature
``inject_message(content, role='user', *, session_key=None) -> bool``
(verified against hermes_cli/plugins.py).
"""

from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from types import ModuleType

from test_pi_manager import (  # type: ignore
    PiManagerTestCase,
    FakePiProcess,
    fake_popen_factory,
    EXEC_SETTLED,
    wait_until,
)
import registry_db  # type: ignore
from core import (  # type: ignore
    PiManager,
    VerifierSpec,
    VERIFY_FAIL,
    VERIFY_NOT_RUN,
    VERIFY_PASS,
    VERIFY_PENDING,
    continuation_id_for,
    format_terminal_wake_message,
)
from outbox import (  # type: ignore
    NotificationOutbox,
    OutboxWorker,
)
from registry_db import Registry  # type: ignore
from wake_worker import TerminalWakeWorker  # type: ignore

ORIGIN = {
    "platform": "telegram",
    "chat_id": "-1001234567890",
    "thread_id": "77",
    "session_key": "agent:main:telegram:group:-1001234567890:77",
    "scope_id": "tenant-1",
    "user_id": "u1",
}
SESSION_KEY = ORIGIN["session_key"]

_NEW_WAKE_COLUMNS = {
    "continuation_enabled", "wake_state", "wake_attempts",
    "wake_requested_at", "wake_accepted_at", "wake_last_error",
}


class FakePluginContext:
    """PluginContext double pinning the real public signature
    ``inject_message(content, role='user', *, session_key=None) -> bool``."""

    def __init__(self) -> None:
        self.injects = []
        self._behavior: object = True

    def behavior(self, result: object) -> None:
        """Script the next results: True/False, or an Exception to raise."""
        self._behavior = result

    def inject_message(self, content: str, role: str = "user", *,
                       session_key=None) -> bool:
        self.injects.append({"content": content, "role": role,
                             "session_key": session_key})
        if isinstance(self._behavior, Exception):
            raise self._behavior
        return self._behavior


class _OutboxManagerCase(PiManagerTestCase):
    """PiManagerTestCase + a durable outbox and the wake worker helpers."""

    def setUp(self) -> None:
        super().setUp()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)

    def make_manager(self, process: FakePiProcess) -> PiManager:
        from test_pi_manager import TEST_THRESHOLDS
        return PiManager(
            registry=self.registry,
            popen_factory=fake_popen_factory(process),
            clock=self.clock,
            default_thresholds=TEST_THRESHOLDS,
            outbox=self.outbox,
        )

    def _progress_kinds(self, task_id: str):
        return [n["kind"] for n in self.registry.list_notifications(task_id=task_id)
                if n["kind"] == "progress"]

    def _wake(self, task_id: str, *, execution_state=EXEC_SETTLED,
              verification_state=VERIFY_PASS, continuation_enabled=1,
              origin: dict = None) -> str:
        """Create a terminal task row and open its (single) wake."""
        self.registry.create_task(
            task_id=task_id,
            execution_state=execution_state,
            verification_state=verification_state,
            origin=json.dumps(origin if origin is not None else ORIGIN),
            continuation_enabled=continuation_enabled,
        )
        self.registry.begin_terminal_wake(task_id, self.clock())
        return task_id


# ---------------------------------------------------------------------------
# Registry migration + defaults
# ---------------------------------------------------------------------------


class TestRegistryMigrationAndDefaults(PiManagerTestCase):

    def test_migration_adds_wake_columns_with_safe_defaults(self):
        """A pre-wake database (tasks table as of commit 2d8ee6c, one row)
        must gain the wake columns with wake-disabled defaults on open."""
        old_cols = [c for c in registry_db.TASK_FIELDS if c not in _NEW_WAKE_COLUMNS]
        old_db = self.tmp / "old.sqlite3"
        conn = sqlite3.connect(str(old_db))
        conn.execute(
            "CREATE TABLE tasks (" + ", ".join(f"{c} TEXT" for c in old_cols) + ")"
        )
        values = ["pi-old"] + [None] * (len(old_cols) - 1)
        conn.execute(
            f"INSERT INTO tasks ({', '.join(old_cols)}) VALUES ({', '.join('?' * len(old_cols))})",
            values,
        )
        conn.execute(
            "UPDATE tasks SET execution_state = 'SETTLED', verification_state = 'PASS', "
            "origin = ? WHERE task_id = 'pi-old'", (json.dumps(ORIGIN),))
        conn.commit()
        conn.close()

        old_registry = Registry(db_path=old_db)
        self.addCleanup(old_registry.close)
        columns = {r[1] for r in sqlite3.connect(str(old_db)).execute("PRAGMA table_info(tasks)")}
        for column in _NEW_WAKE_COLUMNS:
            self.assertIn(column, columns, f"migration must add {column}")

        row = old_registry.get_task("pi-old")
        # Existing rows default to wake-disabled, no wake in flight.
        self.assertEqual(row["continuation_enabled"], 0)
        self.assertIsNone(row["wake_state"])
        self.assertEqual(row["wake_attempts"], 0)
        self.assertIsNone(row["wake_requested_at"])
        self.assertIsNone(row["wake_accepted_at"])
        self.assertIsNone(row["wake_last_error"])

    def test_cli_task_defaults_disabled(self):
        manager = self.make_manager(FakePiProcess())
        result = manager.start_task(prompt="p", cwd=str(self.cwd_dir))
        row = self.registry.get_task(result["task_id"])
        self.assertEqual(row["continuation_enabled"], 0)

    def test_telegram_task_with_valid_session_key_defaults_enabled(self):
        manager = self.make_manager(FakePiProcess())
        result = manager.start_task(
            prompt="p", cwd=str(self.cwd_dir), origin=dict(ORIGIN))
        row = self.registry.get_task(result["task_id"])
        self.assertEqual(row["continuation_enabled"], 1)

    def test_telegram_task_without_session_key_defaults_disabled(self):
        origin = {k: v for k, v in ORIGIN.items() if k != "session_key"}
        manager = self.make_manager(FakePiProcess())
        result = manager.start_task(
            prompt="p", cwd=str(self.cwd_dir), origin=origin)
        row = self.registry.get_task(result["task_id"])
        self.assertEqual(row["continuation_enabled"], 0)


# ---------------------------------------------------------------------------
# State machine on terminal transitions (core side)
# ---------------------------------------------------------------------------


class TestWakeOnTerminalTransitions(PiManagerTestCase):

    def test_settled_without_verifier_requests_exactly_one_wake(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, origin=dict(ORIGIN))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: (self.registry.get_task(task_id) or {}).get("wake_state") == "pending"))
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_SETTLED)
        self.assertEqual(row["verification_state"], VERIFY_NOT_RUN)
        self.assertIsNotNone(row["wake_requested_at"])

        # Second and third terminal observations (recovery re-runs, explicit
        # re-announcement) must NOT open a second wake.
        manager._notify_done(task_id)
        manager._notify_done(task_id)
        manager.recover_all()
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "pending")
        events = [e for e in self.registry.recent_events(task_id, limit=200)
                  if e["event_type"] == "wake_pending"]
        self.assertEqual(len(events), 1, "exactly one logical wake per task")
        self.assertEqual(
            json.loads(events[0]["summary"])["continuation_id"],
            continuation_id_for(task_id))

    def test_verifier_must_resolve_before_the_wake(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        # Gate not resolved yet: shadow the verifier so settlement parks the
        # task at SETTLED/PENDING without announcing or waking.
        manager._run_verifier = lambda tid: None
        task_id = self.start_and_boot(
            manager, process, origin=dict(ORIGIN),
            verifier=VerifierSpec(argv=["true"]))
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: self.registry.get_task(task_id)["verification_state"] == VERIFY_PENDING))
        row = self.registry.get_task(task_id)
        self.assertIsNone(row["wake_state"],
                          "no wake may be requested while the gate is still PENDING")

        # Once the gate resolves, the announcement (and only it) opens the
        # wake, carrying the resolved verdict.
        outcome = manager.run_verifier_now(task_id, VerifierSpec(argv=["true"]))
        self.assertEqual(outcome, VERIFY_PASS)
        manager._notify_done(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "pending")
        self.assertEqual(row["verification_state"], VERIFY_PASS)

    def test_manual_abort_never_wakes(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, origin=dict(ORIGIN))
        result = manager.abort_task(task_id, reason="pi_abort")
        self.assertEqual(result["execution_state"], "ABORTED")
        row = self.registry.get_task(task_id)
        self.assertIsNone(row["wake_state"],
                          "a manual abort must never turn the session back on")
        events = [e for e in self.registry.recent_events(task_id, limit=200)
                  if e["event_type"].startswith("wake_")]
        self.assertEqual(events, [])

    def test_crashed_requests_wake(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, origin=dict(ORIGIN))
        process.finish(1)  # the Pi process dies before settlement
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], "CRASHED")
        self.assertEqual(row["wake_state"], "pending")

    def test_stalled_requests_wake(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, origin=dict(ORIGIN))
        # Pure silence past the soft-stall threshold and the abort grace.
        for _ in range(12):
            self.clock.advance(30)
            manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], "STALLED")
        self.assertEqual(row["wake_state"], "pending")

    def test_recovery_failure_requests_wake(self):
        manager = self.make_manager(FakePiProcess())
        missing = str(self.tmp / "no-such-session.jsonl")
        self.registry.create_task(
            task_id="pi-rec", execution_state="RUNNING", session_file=missing,
            origin=json.dumps(ORIGIN), continuation_enabled=1,
        )
        result = manager.recover_task("pi-rec")
        self.assertFalse(result["recovered"])
        row = self.registry.get_task("pi-rec")
        self.assertEqual(row["execution_state"], "ABORTED")
        self.assertIn("recovery_missing_session", row["last_error"])
        self.assertEqual(row["wake_state"], "pending",
                         "a RECOVERY failure is a terminal outcome the "
                         "orchestrator must act on (unlike a manual abort)")

    def test_recovery_backfills_missing_wake_for_settled_rows(self):
        """If the manager died between the gate resolving and the wake
        request, the boot-time recovery sweep opens it — exactly once."""
        manager = self.make_manager(FakePiProcess())
        self._settle_row(manager, "pi-backfill")
        manager.recover_all()
        self.assertEqual(self.registry.get_task("pi-backfill")["wake_state"], "pending")
        manager.recover_all()  # idempotent
        events = [e for e in self.registry.recent_events("pi-backfill", limit=100)
                  if e["event_type"] == "wake_pending"]
        self.assertEqual(len(events), 1)

    def test_disabled_settled_row_is_marked_disabled_by_recovery(self):
        manager = self.make_manager(FakePiProcess())
        self.registry.create_task(
            task_id="pi-dis", execution_state=EXEC_SETTLED,
            verification_state=VERIFY_PASS, origin=json.dumps(ORIGIN),
            continuation_enabled=0,
        )
        manager.recover_all()
        self.assertEqual(self.registry.get_task("pi-dis")["wake_state"], "disabled")

    @staticmethod
    def _settle_row(manager: PiManager, task_id: str) -> None:
        manager.registry.create_task(
            task_id=task_id, execution_state=EXEC_SETTLED,
            verification_state=VERIFY_PASS, origin=json.dumps(ORIGIN),
            continuation_enabled=1,
        )


# ---------------------------------------------------------------------------
# TerminalWakeWorker: acceptance, retry, uncertain recovery, routing
# ---------------------------------------------------------------------------


class TestTerminalWakeWorker(_OutboxManagerCase):

    def _worker(self, ctx: FakePluginContext, **kw) -> TerminalWakeWorker:
        return TerminalWakeWorker(self.registry, ctx, now_fn=self.clock, **kw)

    def test_acceptance_true_marks_accepted_with_exact_session_key(self):
        ctx = FakePluginContext()
        task_id = self._wake("pi-w1")
        worker = self._worker(ctx)
        self.assertEqual(worker.run_once(now=self.clock()), 1)

        self.assertEqual(len(ctx.injects), 1)
        inj = ctx.injects[0]
        self.assertEqual(inj["session_key"], SESSION_KEY,
                         "the wake must route to the EXACT origin session_key")
        self.assertEqual(inj["role"], "user")

        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "accepted")
        self.assertIsNotNone(row["wake_accepted_at"])
        self.assertEqual(row["wake_attempts"], 1)

        # Accepted is terminal: further passes never touch the wake again.
        self.clock.advance(10_000)
        worker.run_once(now=self.clock())
        self.assertEqual(len(ctx.injects), 1)
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "accepted")

    def test_wake_message_is_small_and_names_states(self):
        ctx = FakePluginContext()
        task_id = self._wake("pi-msg", execution_state="CRASHED",
                             verification_state=VERIFY_NOT_RUN)
        self._worker(ctx).run_once(now=self.clock())
        msg = ctx.injects[0]["content"]
        for needle in (task_id, "execution_state=CRASHED",
                       "verification_state=NOT_RUN",
                       continuation_id_for(task_id)):
            self.assertIn(needle, msg)
        self.assertLess(len(msg), 700,
                        "the wake is a small control message, not a report")
        self.assertNotIn("krok", msg)  # no Telegram notice body smuggled in

    def test_format_message_matches_row_fields(self):
        row = {"execution_state": "STALLED", "verification_state": VERIFY_FAIL}
        msg = format_terminal_wake_message("pi-fmt", row)
        self.assertIn("execution_state=STALLED", msg)
        self.assertIn("verification_state=FAIL", msg)
        self.assertIn("wake:pi-fmt", msg)

    def test_wake_message_is_autonomy_preserving(self):
        """The wake must NOT forbid the orchestrator from re-running work;
        it defers the retry/replacement decision to the orchestrator's own
        analysis of the terminal state, verification result, and evidence."""
        row = {"execution_state": EXEC_SETTLED, "verification_state": VERIFY_PASS}
        msg = format_terminal_wake_message("pi-auto", row)

        # The old overly restrictive sentence is gone.
        self.assertNotIn("do not re-run the task", msg)

        # New semantics: analyze and continue autonomously.
        self.assertIn("continue the parent workflow autonomously", msg)

        # Dedupe succeeded work, but allow retry/replacement when justified.
        self.assertIn("Do not duplicate work that already succeeded", msg)
        self.assertIn("retry or launch a replacement Pi task only when", msg)
        self.assertIn("the terminal state, verification result, or available evidence", msg)
        self.assertIn("justifies it", msg)

        # One durable wake, verifier-before-wake — unchanged guarantees.
        self.assertIn("fires once per task", msg)
        self.assertIn("verifier resolved", msg)
        self.assertIn("wake:pi-auto", msg)

    def test_false_return_retries_on_bounded_backoff(self):
        ctx = FakePluginContext()
        ctx.behavior(False)
        task_id = self._wake("pi-r1")
        worker = self._worker(ctx, retry_backoff_seconds=(10.0, 20.0))
        now0 = self.clock()
        worker.run_once(now=now0)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "pending",
                         "a False return is a failed dispatch: back to pending")
        self.assertEqual(row["wake_attempts"], 1)
        self.assertAlmostEqual(row["wake_requested_at"], now0 + 10.0)
        self.assertIn("returned False", row["wake_last_error"])

        # Not due yet: no second dispatch.
        self.clock.advance(5.0)
        worker.run_once(now=self.clock())
        self.assertEqual(len(ctx.injects), 1)

        # Due again: second attempt, second backoff step.
        self.clock.advance(6.0)
        worker.run_once(now=self.clock())
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_attempts"], 2)
        self.assertAlmostEqual(row["wake_requested_at"], self.clock() + 20.0)
        self.assertEqual(len(ctx.injects), 2)

    def test_exception_is_treated_like_false(self):
        ctx = FakePluginContext()
        ctx.behavior(RuntimeError("gateway scheduling blew up"))
        task_id = self._wake("pi-r2")
        self._worker(ctx).run_once(now=self.clock())
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "pending")
        self.assertIn("inject_message raised", row["wake_last_error"])

    def test_retry_budget_is_bounded_then_exhausted(self):
        ctx = FakePluginContext()
        ctx.behavior(False)
        task_id = self._wake("pi-r3")
        worker = self._worker(ctx, max_attempts=3, retry_backoff_seconds=(1.0,))
        for attempt in range(3):
            self.clock.advance(2.0)
            worker.run_once(now=self.clock())
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "exhausted",
                         "the bounded budget must terminate, not loop")
        self.assertEqual(row["wake_attempts"], 3)
        self.assertEqual(len(ctx.injects), 3)
        self.assertIsNotNone(row["wake_last_error"])

        # Exhausted is terminal: nothing further, ever.
        self.clock.advance(100_000)
        worker.run_once(now=self.clock())
        self.assertEqual(len(ctx.injects), 3)
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "exhausted")

    def test_disabled_task_is_never_dispatched(self):
        ctx = FakePluginContext()
        self.registry.create_task(
            task_id="pi-dis2", execution_state=EXEC_SETTLED,
            verification_state=VERIFY_PASS, origin=json.dumps(ORIGIN),
            continuation_enabled=0,
        )
        self.assertEqual(self.registry.begin_terminal_wake("pi-dis2", self.clock()),
                         "disabled")
        worker = self._worker(ctx)
        self.assertEqual(worker.run_once(now=self.clock()), 0)
        self.assertEqual(ctx.injects, [])

    def test_dispatching_surviving_restart_is_uncertain_and_never_retried(self):
        """Crash safety: a worker that claimed the wake and died before
        recording the outcome leaves 'dispatching'. The gateway MAY have
        accepted that injection, so the fresh process must NOT re-dispatch
        (a duplicate orchestrator turn is worse than a missed wake)."""
        task_id = self._wake("pi-unc")
        # The previous process claimed the wake and then died.
        self.assertTrue(self.registry.claim_terminal_wake(task_id, self.clock()))
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "dispatching")

        # Fresh plugin load: brand-new worker, brand-new ctx.
        ctx = FakePluginContext()
        worker = self._worker(ctx)
        worker.run_once(now=self.clock())
        row = self.registry.get_task(task_id)
        self.assertEqual(row["wake_state"], "uncertain")
        self.assertEqual(ctx.injects, [],
                         "an uncertain wake is never (re-)dispatched")
        self.assertIn("not retried", row["wake_last_error"])
        events = [e for e in self.registry.recent_events(task_id, limit=100)
                  if e["event_type"] == "wake_uncertain"]
        self.assertEqual(len(events), 1)

        # And no future pass ever re-dispatches it.
        self.clock.advance(100_000)
        worker.run_once(now=self.clock())
        self.assertEqual(ctx.injects, [])
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "uncertain")

    def test_live_pending_row_is_untouched_by_the_restart_sweep(self):
        """Only 'dispatching' rows are uncertain; a 'pending' row that
        simply waited across the restart is delivered normally."""
        task_id = self._wake("pi-pend")
        ctx = FakePluginContext()
        self._worker(ctx).run_once(now=self.clock())
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "accepted")
        self.assertEqual(len(ctx.injects), 1)


# ---------------------------------------------------------------------------
# Progress baseline + passivity
# ---------------------------------------------------------------------------


class TestProgressBaselineAndPassivity(_OutboxManagerCase):

    def test_silent_task_never_emits_its_first_progress(self):
        """The baseline edge case: a silent task still running past 90 s
        (and later, past 90+180 s) must not emit a progress notice — the
        gate now compares against the initial task signature."""
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        rt = manager._rt(task_id)

        self.clock.advance(95)
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._progress_kinds(task_id), [],
                         "silence past the 90 s mark is not progress")

        self.clock.advance(180)  # well past first + one interval, still silent
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._progress_kinds(task_id), [])

    def test_real_progress_permits_the_first_notice(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        rt = manager._rt(task_id)

        self.clock.advance(60)
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._progress_kinds(task_id), [],
                         "inside the 90 s window nothing is emitted")

        self.clock.advance(40)  # t=100 > 90, with real progress since boot
        manager._notify_progress(task_id, rt)
        self.assertEqual(self._progress_kinds(task_id), ["progress"],
                         "real progress changes the signature and permits "
                         "the first notice on the normal cadence")

    def test_progress_never_injects_into_any_session(self):
        """Progress stays passive end to end: a progress notice on the outbox
        must never reach ctx.inject_message, and a non-terminal task has no
        wake requested at all."""
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, origin=dict(ORIGIN))
        rt = manager._rt(task_id)

        self.clock.advance(100)
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        manager._notify_progress(task_id, rt)
        self.assertEqual(len(self._progress_kinds(task_id)), 1,
                         "the passive outbox progress rail still works")

        ctx = FakePluginContext()
        TerminalWakeWorker(self.registry, ctx, now_fn=self.clock).run_once(
            now=self.clock())
        self.assertEqual(ctx.injects, [], "no progress ever calls inject_message")
        self.assertIsNone(self.registry.get_task(task_id)["wake_state"])


# ---------------------------------------------------------------------------
# Rails stay independent
# ---------------------------------------------------------------------------


class _NativeStub:
    """Stands in for the native tools.send_message_tool at its import
    boundary (same pattern as test_outbox_host_adapter)."""

    def __init__(self) -> None:
        self.calls = []

    def send_message_tool(self, args, **kw):
        self.calls.append({"args": dict(args), "kw": dict(kw)})
        return json.dumps({"success": True, "platform": "telegram"})


_MISSING = object()


def _install_native_stub(stub: _NativeStub):
    saved = sys.modules.get("tools.send_message_tool", _MISSING)
    fake = ModuleType("tools.send_message_tool")
    fake.send_message_tool = stub.send_message_tool
    sys.modules["tools.send_message_tool"] = fake

    def restore():
        if saved is _MISSING:
            sys.modules.pop("tools.send_message_tool", None)
        else:
            sys.modules["tools.send_message_tool"] = saved

    return restore


class TestRailsIndependence(_OutboxManagerCase):

    def test_telegram_notification_causes_no_agent_turn(self):
        """The existing rail end to end: a terminal Telegram notice is
        delivered by the outbox worker through the (stubbed) native
        send_message_tool and NEVER touches ctx.inject_message. The wake
        worker is the only inject path, and it uses the session rail, not
        the Telegram rail."""
        stub = _NativeStub()
        self.addCleanup(_install_native_stub(stub))

        task_id = self._wake("pi-rails")
        # Same terminal event also enqueues the passive Telegram notice.
        self.outbox.enqueue(task_id, "verifier", "completion body")

        ctx = FakePluginContext()
        outbox_worker = OutboxWorker(self.outbox)  # default deliver = host adapter
        wake_worker = TerminalWakeWorker(self.registry, ctx, now_fn=self.clock)

        # 1) The Telegram rail: delivered natively, zero injections.
        self.assertEqual(outbox_worker.run_once(now=self.clock()), 1)
        self.assertEqual(len(stub.calls), 1)
        self.assertEqual(stub.calls[0]["args"]["target"],
                         "telegram:-1001234567890:77")
        self.assertEqual(ctx.injects, [],
                         "the Telegram completion notice must never cause "
                         "a main-agent turn")

        # 2) The wake rail: one injection into the exact session, and the
        #    Telegram rail is untouched by it.
        wake_worker.run_once(now=self.clock())
        self.assertEqual(len(ctx.injects), 1)
        self.assertEqual(ctx.injects[0]["session_key"], SESSION_KEY)
        self.assertEqual(len(stub.calls), 1,
                         "the wake never rides the Telegram rail")
        self.assertEqual(self.registry.get_task(task_id)["wake_state"], "accepted")


if __name__ == "__main__":
    unittest.main()