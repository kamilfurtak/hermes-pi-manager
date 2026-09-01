"""pi-manager core test suite — scenarios A-H plus concurrency, RPC command
correlation, hard-timeout/abort escalation, malformed-JSONL resilience, and
exact ``agent_settled`` gating.

Deterministic: no real Pi/NInfer/network. Uses ``FakePiProcess`` (in-process
Popen-like fake) and a manually advanced ``FakeClock`` so watchdog thresholds
are exercised without real sleeps. Run:

    python3 -m unittest discover -s infra/hermes/plugins/pi-manager/tests -p 'test_*.py' -v
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLUGIN_DIR))
sys.path.insert(0, str(PLUGIN_DIR / "tests"))

from fakes import FakeClock, FakePiProcess, fake_popen_factory  # noqa: E402
from core import (  # noqa: E402
    EXEC_ABORTED,
    EXEC_CRASHED,
    EXEC_RUNNING,
    EXEC_SETTLED,
    EXEC_STALLED,
    EXEC_STARTING,
    EXEC_TOOL_RUNNING,
    EXEC_UNRESPONSIVE,
    EXEC_WAITING,
    DERIVED_DONE,
    DERIVED_FAILED_VERIFICATION,
    VERIFY_FAIL,
    VERIFY_NOT_RUN,
    VERIFY_PASS,
    VERIFY_PENDING,
    PiManager,
    Thresholds,
    VerifierSpec,
    derived_state,
)
from registry_db import Registry  # noqa: E402


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


TEST_THRESHOLDS = Thresholds(
    heartbeat_seconds=20.0,
    rpc_timeout_seconds=0.5,
    waiting_seconds=10.0,
    soft_stall_seconds=90.0,
    tool_stall_seconds=300.0,
    stall_grace_seconds=15.0,
    emergency_cap_seconds=900.0,
    terminate_grace_seconds=0.3,
    watchdog_interval_seconds=999999.0,  # disable the auto-loop; tests tick manually
)


class PiManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="pi-manager-test-"))
        self.cwd_dir = self.tmp / "cwd"
        self.cwd_dir.mkdir()
        self.db_path = self.tmp / "registry.sqlite3"
        self.registry = Registry(db_path=self.db_path)
        self.clock = FakeClock()

    def tearDown(self):
        self.registry.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_manager(self, process: FakePiProcess) -> PiManager:
        return PiManager(
            registry=self.registry,
            popen_factory=fake_popen_factory(process),
            clock=self.clock,
            default_thresholds=TEST_THRESHOLDS,
        )

    def emit_and_sync(self, manager: PiManager, process: FakePiProcess, task_id: str, event: dict):
        """Emit one event and block until the reader thread has applied it
        before returning — event delivery is asynchronous (a background
        reader thread), so a caller that immediately advances the fake
        clock or asserts state must not race that thread. Synchronizes on
        the registry's real-wall-clock ``updated_at`` marker (independent
        of the FakeClock used for watchdog threshold math), so it works
        even when the same event type repeats across a loop."""
        baseline = self.registry.get_task(task_id)["updated_at"]
        process.emit(event)
        self.assertTrue(
            wait_until(lambda: (self.registry.get_task(task_id) or {}).get("updated_at", 0) > baseline),
            f"event {event.get('type')!r} was never applied",
        )

    def start_and_boot(self, manager: PiManager, process: FakePiProcess, **kw):
        result = manager.start_task(prompt="do the thing", cwd=str(self.cwd_dir), **kw)
        task_id = result["task_id"]
        self.assertTrue(process.wait_until_command("get_state"), "boot never called get_state")
        self.assertTrue(process.wait_until_command("prompt"), "boot never sent prompt")
        self.assertTrue(
            wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_RUNNING),
            "task never reached RUNNING",
        )
        return task_id


# ---------------------------------------------------------------------------
# A. normal tool/event sequence -> agent_end -> agent_settled -> verifier
#    PASS -> DONE
# ---------------------------------------------------------------------------


class TestScenarioA_NormalSettleAndVerifyPass(PiManagerTestCase):
    def test_settle_then_pass_yields_done(self):
        process = FakePiProcess()
        verifier = VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(0)"])
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, verifier=verifier)

        process.emit({"type": "agent_start"})
        process.emit({"type": "tool_execution_start", "tool": "read", "toolCallId": "t1"})
        process.emit({"type": "tool_execution_end", "toolCallId": "t1"})
        process.emit({"type": "agent_end", "willRetry": False})
        process.emit({"type": "agent_settled"})

        self.assertTrue(
            wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED),
            "task never reached SETTLED",
        )
        self.assertTrue(
            wait_until(lambda: manager.status(task_id)["verification_state"] == VERIFY_PASS),
            "verifier never reported PASS",
        )
        status = manager.status(task_id)
        self.assertEqual(status["state"], DERIVED_DONE)
        self.assertIsNotNone(status["settled_at"])


# ---------------------------------------------------------------------------
# B. long tool + progress/heartbeat -> remains TOOL_RUNNING, no false STALLED
# ---------------------------------------------------------------------------


class TestScenarioB_LongToolNoFalseStall(PiManagerTestCase):
    def test_long_tool_with_progress_stays_tool_running(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        process.emit({"type": "agent_start"})
        process.emit({"type": "tool_execution_start", "tool": "bash", "toolCallId": "t1"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_TOOL_RUNNING))

        # Simulate a long-running tool: repeated progress updates well past
        # the generic soft-stall threshold, but under tool_stall_seconds.
        for _ in range(5):
            self.clock.advance(60.0)  # total will exceed soft_stall (90s) across iterations
            self.emit_and_sync(manager, process, task_id,
                                {"type": "tool_execution_update", "toolCallId": "t1"})
            manager.watchdog_tick(task_id)
            status = manager.status(task_id)
            self.assertEqual(status["execution_state"], EXEC_TOOL_RUNNING)
            self.assertNotEqual(status["execution_state"], EXEC_STALLED)

        process.emit({"type": "tool_execution_end", "toolCallId": "t1"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_RUNNING))


# ---------------------------------------------------------------------------
# C. quiet but responsive/provider-delay -> WAITING, not immediate STALLED
# ---------------------------------------------------------------------------


class TestScenarioC_QuietResponsiveIsWaiting(PiManagerTestCase):
    def test_quiet_period_becomes_waiting_not_stalled(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        self.clock.advance(TEST_THRESHOLDS.waiting_seconds + 1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_WAITING)
        self.assertNotEqual(status["execution_state"], EXEC_STALLED)


# ---------------------------------------------------------------------------
# D. no events, no progress, unchanged state -> diagnostic snapshot, grace,
#    then STALLED
# ---------------------------------------------------------------------------


class TestScenarioD_TrueStallAfterGrace(PiManagerTestCase):
    def test_true_stall_after_grace_period(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        # Cross the soft-stall threshold: heartbeat succeeds (process alive
        # and RPC-responsive) but quiet -> diagnostic snapshot + grace start.
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertNotEqual(status["execution_state"], EXEC_STALLED,
                             "must not jump straight to STALLED before grace expires")
        events = self.registry.recent_events(task_id, limit=50)
        self.assertTrue(any(e["event_type"] == "diagnostic_snapshot" for e in events),
                         "no diagnostic snapshot was recorded before STALLED")

        # Grace period expires with still no progress -> STALLED.
        self.clock.advance(TEST_THRESHOLDS.stall_grace_seconds + 1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_STALLED)


# ---------------------------------------------------------------------------
# D2. last_state_hash drives stall semantics: a responsive-but-changing
#     state must avoid STALLED even past the soft-stall threshold, while
#     unchanged heartbeats (scenario D above) still reach STALLED.
# ---------------------------------------------------------------------------


class TestStateHashDrivesLiveness(PiManagerTestCase):
    def test_changing_state_during_heartbeat_avoids_stall(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        # Repeatedly cross the soft-stall threshold with NO events at all —
        # heartbeat is the only source of freshness here — but change the
        # get_state payload before every tick. This proves state-hash-driven
        # liveness specifically (not event-driven liveness, already covered
        # by scenario B/C).
        for i in range(5):
            self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
            process.state = dict(process.state, messageCount=process.state["messageCount"] + 1)
            manager.watchdog_tick(task_id)
            status = manager.status(task_id)
            self.assertNotEqual(status["execution_state"], EXEC_STALLED,
                                 f"iteration {i}: a changing state hash must never stall")

        row = self.registry.get_task(task_id)
        self.assertIsNotNone(row["last_state_hash"], "last_state_hash must be populated")

    def test_unchanged_heartbeats_still_reach_stalled_after_grace(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        first_hash = row["last_state_hash"]
        self.assertIsNotNone(first_hash, "last_state_hash must be populated on the first heartbeat")
        self.assertEqual(row["execution_state"], EXEC_WAITING)
        first_progress_at = row["last_progress_at"]

        # A second identical heartbeat, still inside the grace window: hash
        # unchanged (no execution_state transition either) -> must NOT
        # refresh last_progress_at nor last_state_changed_at.
        self.clock.advance(1.0)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_WAITING, "still inside the grace window")
        self.assertEqual(row["last_state_hash"], first_hash,
                          "state hash must be identical across unchanged heartbeats")
        self.assertEqual(row["last_progress_at"], first_progress_at,
                          "an unchanged heartbeat must not refresh last_progress_at")

        # Grace expires with the state still unchanged -> true STALLED. The
        # unchanged heartbeat must never defeat this.
        self.clock.advance(TEST_THRESHOLDS.stall_grace_seconds)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_STALLED)
        self.assertEqual(row["last_state_hash"], first_hash,
                          "state hash must still be identical once STALLED")
        self.assertEqual(row["last_progress_at"], first_progress_at,
                          "reaching STALLED must not be preceded by a phantom progress refresh")


# ---------------------------------------------------------------------------
# E. process exits before settled -> CRASHED
# ---------------------------------------------------------------------------


class TestScenarioE_CrashBeforeSettle(PiManagerTestCase):
    def test_early_exit_yields_crashed(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_start"})
        wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_RUNNING)

        process.finish(exit_code=1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_CRASHED)
        self.assertEqual(status["exit_code"], 1)
        self.assertNotEqual(status["state"], DERIVED_DONE)


# ---------------------------------------------------------------------------
# F. restart/recovery with matching identity + cursor replay; negative
#    mismatch/new-empty case never resumes
# ---------------------------------------------------------------------------


class TestScenarioF_RecoveryIdentityAndReplay(PiManagerTestCase):
    def _seed_resumable_row(self, task_id: str, session_file: Path, session_id: str,
                             last_entry_id=None):
        session_file.write_text('{"id": "seed"}\n', encoding="utf-8")
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_RUNNING,
            verification_state=VERIFY_NOT_RUN,
            session_id=session_id, session_file=str(session_file),
            expected_session_id=session_id, expected_session_file=str(session_file),
            last_entry_id=last_entry_id,
        )

    def test_matching_identity_recovers_and_replays_cursor(self):
        task_id = "task-recover-ok"
        session_file = self.tmp / "session-ok.jsonl"
        self._seed_resumable_row(task_id, session_file, "sess-match", last_entry_id="entry-5")

        process = FakePiProcess()
        process.state = {
            "sessionId": "sess-match", "sessionFile": str(session_file),
            "isStreaming": False, "isCompacting": False, "messageCount": 4,
            "pendingMessageCount": 0,
        }
        process.entries = {
            "entries": [{"id": "entry-6"}, {"id": "entry-7"}], "leafId": "entry-7",
        }
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)

        self.assertTrue(result["recovered"])
        self.assertEqual(result["replayed_entries"], 2)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["last_entry_id"], "entry-7")
        self.assertEqual(row["execution_state"], EXEC_WAITING)
        self.assertTrue(any(
            e["event_type"] == "recovery_attached" for e in self.registry.recent_events(task_id, 50)
        ))

    def test_identity_mismatch_never_resumes(self):
        task_id = "task-recover-mismatch"
        session_file = self.tmp / "session-mismatch.jsonl"
        self._seed_resumable_row(task_id, session_file, "sess-expected")

        process = FakePiProcess()
        process.state = {
            "sessionId": "sess-DIFFERENT", "sessionFile": str(session_file),
            "isStreaming": False, "isCompacting": False, "messageCount": 0,
            "pendingMessageCount": 0,
        }
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)

        self.assertFalse(result["recovered"])
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_ABORTED)
        self.assertIn("mismatch", row["last_error"])
        # Never sent a prompt during a rejected recovery.
        self.assertFalse(any(c.get("type") == "prompt" for c in process.commands_received))

    def test_missing_session_file_never_resumes(self):
        task_id = "task-recover-missing"
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_RUNNING,
            verification_state=VERIFY_NOT_RUN, session_id="sess-x",
            session_file=str(self.tmp / "does-not-exist.jsonl"),
            expected_session_id="sess-x", expected_session_file=str(self.tmp / "does-not-exist.jsonl"),
        )
        process = FakePiProcess()
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)
        self.assertFalse(result["recovered"])
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_ABORTED)
        self.assertIn("missing_session", row["last_error"])
        self.assertEqual(process.commands_received, [])  # never even connected

    def test_new_empty_session_never_silently_adopted(self):
        task_id = "task-recover-empty"
        session_file = self.tmp / "session-empty-expected.jsonl"
        # No expected_session_id on record (simulates a task that crashed
        # before identity was ever established) but a session_file exists.
        session_file.write_text("{}\n", encoding="utf-8")
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_STARTING,
            verification_state=VERIFY_NOT_RUN, session_file=str(session_file),
        )
        process = FakePiProcess()
        process.state = {
            "sessionId": None, "sessionFile": None, "isStreaming": False,
            "isCompacting": False, "messageCount": 0, "pendingMessageCount": 0,
        }
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)
        self.assertFalse(result["recovered"])
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_ABORTED)

    def test_actual_session_file_missing_when_expected_is_set_never_resumes(self):
        # Regression: an expected_session_file on record but the actual
        # get_state response returns no sessionFile at all must be REJECTED
        # (previously the falsy actual_file short-circuited the comparison
        # and let a mismatched/absent session slip through as identity_ok).
        task_id = "task-recover-actual-file-missing"
        session_file = self.tmp / "session-actual-missing.jsonl"
        self._seed_resumable_row(task_id, session_file, "sess-match")

        process = FakePiProcess()
        process.state = {
            "sessionId": "sess-match", "sessionFile": None, "isStreaming": False,
            "isCompacting": False, "messageCount": 4, "pendingMessageCount": 0,
        }
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)

        self.assertFalse(result["recovered"])
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_ABORTED)
        self.assertIn("mismatch", row["last_error"])
        # The stored expected_session_file (ground truth) must survive a
        # rejected recovery unchanged, not be clobbered by the (missing)
        # actual value before validation ran.
        self.assertEqual(row["expected_session_file"], str(session_file))
        self.assertFalse(any(c.get("type") == "prompt" for c in process.commands_received))

    def test_session_file_identity_compares_canonical_paths(self):
        # A session file reachable via a non-normalized path (redundant
        # "./" segment) must still be recognized as the SAME identity as
        # the canonical expected path — comparison must be canonical/
        # expanded absolute paths, not raw strings.
        task_id = "task-recover-canonical-path"
        session_file = self.tmp / "session-canonical.jsonl"
        self._seed_resumable_row(task_id, session_file, "sess-match")

        noisy_path = str(self.tmp) + os.sep + "." + os.sep + session_file.name
        process = FakePiProcess()
        process.state = {
            "sessionId": "sess-match", "sessionFile": noisy_path, "isStreaming": False,
            "isCompacting": False, "messageCount": 4, "pendingMessageCount": 0,
        }
        manager = self.make_manager(process)
        result = manager.recover_task(task_id)

        self.assertTrue(result["recovered"], result.get("reason"))
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_WAITING)


# ---------------------------------------------------------------------------
# F2. verifier spec restoration across recovery (defect 4b)
# ---------------------------------------------------------------------------


class TestVerifierRestorationOnRecovery(PiManagerTestCase):
    def test_settled_pending_verification_restores_and_runs_verifier(self):
        # Simulates a manager restart AFTER a task settled but BEFORE its
        # verifier finished (or even started): the row is SETTLED with
        # verification_state PENDING and a persisted verifier_spec. Recovery
        # must restore that spec and actually run it — never derive DONE
        # without a real verifier PASS, and never touch execution_state.
        task_id = "task-settled-pending-verify"
        verifier_argv = [sys.executable, "-c", "import sys; sys.exit(0)"]
        now = self.clock()
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_SETTLED,
            verification_state=VERIFY_PENDING,
            verifier_spec=json.dumps({"argv": verifier_argv, "timeout_seconds": 30.0}),
            started_at=now, settled_at=now,
        )
        process = FakePiProcess()  # never spawned for a SETTLED recovery
        manager = self.make_manager(process)

        result = manager.recover_task(task_id)

        self.assertTrue(result["recovered"])
        self.assertEqual(result["execution_state"], EXEC_SETTLED)
        self.assertTrue(result["verifier_restored"])
        self.assertEqual(result["verification_state"], VERIFY_PASS)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_SETTLED, "recovery must never change SETTLED")
        self.assertEqual(row["verification_state"], VERIFY_PASS)
        self.assertEqual(derived_state(row["execution_state"], row["verification_state"]), DERIVED_DONE)
        # Never spawned a live process for an already-settled task.
        self.assertEqual(process.commands_received, [])

    def test_recover_all_includes_settled_pending_verification_rows(self):
        task_id = "task-settled-pending-verify-all"
        verifier_argv = [sys.executable, "-c", "import sys; sys.exit(1)"]
        now = self.clock()
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_SETTLED,
            verification_state=VERIFY_NOT_RUN,
            verifier_spec=json.dumps({"argv": verifier_argv, "timeout_seconds": 30.0}),
            started_at=now, settled_at=now,
        )
        process = FakePiProcess()
        manager = self.make_manager(process)

        results = manager.recover_all()

        self.assertTrue(any(r.get("task_id") == task_id for r in results),
                         "recover_all must include SETTLED rows with pending verification")
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_SETTLED)
        self.assertEqual(row["verification_state"], VERIFY_FAIL)

    def test_resumed_task_verifier_survives_restart_to_real_settlement(self):
        # A task mid-execution (not yet settled) with a persisted verifier
        # must have that verifier restored into the recovered runtime task
        # object, so that when it later settles for real, verification
        # actually runs instead of being silently dropped.
        task_id = "task-resume-with-verifier"
        session_file = self.tmp / "session-resume-verifier.jsonl"
        session_file.write_text('{"id": "seed"}\n', encoding="utf-8")
        verifier_argv = [sys.executable, "-c", "import sys; sys.exit(0)"]
        self.registry.create_task(
            task_id=task_id, cwd=str(self.cwd_dir), execution_state=EXEC_RUNNING,
            verification_state=VERIFY_NOT_RUN,
            session_id="sess-resume", session_file=str(session_file),
            expected_session_id="sess-resume", expected_session_file=str(session_file),
            verifier_spec=json.dumps({"argv": verifier_argv, "timeout_seconds": 30.0}),
        )
        process = FakePiProcess()
        process.state = {
            "sessionId": "sess-resume", "sessionFile": str(session_file), "isStreaming": False,
            "isCompacting": False, "messageCount": 4, "pendingMessageCount": 0,
        }
        manager = self.make_manager(process)

        result = manager.recover_task(task_id)
        self.assertTrue(result["recovered"], result.get("reason"))

        process.emit({"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["verification_state"] == VERIFY_PASS),
            "verifier restored on recovery must actually run after real settlement")


# ---------------------------------------------------------------------------
# F3. real (nested) entry_appended event shape advances the replay cursor
# ---------------------------------------------------------------------------


class TestEntryAppendedNestedShapeAdvancesCursor(PiManagerTestCase):
    def test_nested_entry_id_advances_last_entry_id(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        self.assertIsNone(self.registry.get_task(task_id).get("last_entry_id"))

        # Real Pi RPC shape: {"type": "entry_appended", "entry": {"id": ...}}
        # — the id is nested, never top-level, in production.
        self.emit_and_sync(manager, process, task_id,
                            {"type": "entry_appended", "entry": {"id": "entry-nested-1"}})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["last_entry_id"], "entry-nested-1",
                          "cursor must advance from the nested entry.id, not stay unset")

        self.emit_and_sync(manager, process, task_id,
                            {"type": "entry_appended", "entry": {"id": "entry-nested-2"}})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["last_entry_id"], "entry-nested-2")

    def test_top_level_id_is_only_a_defensive_fallback(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        # No nested "entry" object at all: the top-level id is accepted
        # only as a defensive fallback, never the primary path.
        self.emit_and_sync(manager, process, task_id,
                            {"type": "entry_appended", "id": "entry-fallback-1"})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["last_entry_id"], "entry-fallback-1")

        # When BOTH are present, the nested shape must win (it is the real
        # primary path; the top-level field must not shadow it).
        self.emit_and_sync(manager, process, task_id,
                            {"type": "entry_appended", "id": "entry-STALE",
                             "entry": {"id": "entry-nested-wins"}})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["last_entry_id"], "entry-nested-wins")


# ---------------------------------------------------------------------------
# G. agent_end without settled is not success
# ---------------------------------------------------------------------------


class TestScenarioG_AgentEndAloneIsNotSuccess(PiManagerTestCase):
    def test_agent_end_without_settled_is_not_final(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_start"})
        process.emit({"type": "agent_end", "willRetry": False})

        wait_until(lambda: manager.status(task_id)["last_event_type"] == "agent_end")
        status = manager.status(task_id)
        self.assertNotEqual(status["execution_state"], EXEC_SETTLED)
        self.assertNotIn(status["state"], (DERIVED_DONE, DERIVED_FAILED_VERIFICATION))
        self.assertIsNone(status["settled_at"])

        # willRetry=True must also never settle, and monitoring continues.
        process.emit({"type": "agent_end", "willRetry": True})
        time.sleep(0.05)
        status = manager.status(task_id)
        self.assertNotEqual(status["execution_state"], EXEC_SETTLED)


# ---------------------------------------------------------------------------
# H. settled + verifier FAIL -> execution SETTLED, verification FAIL,
#    final FAILED_VERIFICATION
# ---------------------------------------------------------------------------


class TestScenarioH_SettleThenVerifyFail(PiManagerTestCase):
    def test_settle_then_fail_yields_failed_verification(self):
        process = FakePiProcess()
        verifier = VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(1)"])
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, verifier=verifier)

        process.emit({"type": "agent_start"})
        process.emit({"type": "agent_end", "willRetry": False})
        process.emit({"type": "agent_settled"})

        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["verification_state"] == VERIFY_FAIL))
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_SETTLED)
        self.assertEqual(status["verification_state"], VERIFY_FAIL)
        self.assertEqual(status["state"], DERIVED_FAILED_VERIFICATION)


# ---------------------------------------------------------------------------
# No verifier at all: SETTLED must stay SETTLED/NOT_RUN, never DONE.
# ---------------------------------------------------------------------------


class TestNoVerifierNeverClaimsDone(PiManagerTestCase):
    def test_settled_without_verifier_stays_not_run(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        status = manager.status(task_id)
        self.assertEqual(status["verification_state"], VERIFY_NOT_RUN)
        self.assertEqual(status["state"], EXEC_SETTLED)
        self.assertNotEqual(status["state"], DERIVED_DONE)


# ---------------------------------------------------------------------------
# Concurrent task isolation
# ---------------------------------------------------------------------------


class TestConcurrentTaskIsolation(PiManagerTestCase):
    def test_two_tasks_do_not_cross_contaminate(self):
        proc_a = FakePiProcess()
        proc_a.state = dict(proc_a.state, sessionId="sess-a")
        proc_b = FakePiProcess()
        proc_b.state = dict(proc_b.state, sessionId="sess-b")

        procs = {"a": proc_a, "b": proc_b}
        calls = {"n": 0}

        def factory(argv, cwd):
            calls["n"] += 1
            proc = proc_a if calls["n"] == 1 else proc_b
            proc.sync_session_state_from_argv(argv)
            return proc

        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        task_a = manager.start_task(prompt="task a", cwd=str(self.cwd_dir), task_id="task-a")["task_id"]
        task_b = manager.start_task(prompt="task b", cwd=str(self.cwd_dir), task_id="task-b")["task_id"]

        self.assertTrue(proc_a.wait_until_command("prompt"))
        self.assertTrue(proc_b.wait_until_command("prompt"))

        proc_a.emit({"type": "agent_settled"})
        self.assertTrue(wait_until(lambda: manager.status(task_a)["execution_state"] == EXEC_SETTLED))
        self.assertEqual(manager.status(task_b)["execution_state"], EXEC_RUNNING,
                          "task b must be unaffected by task a settling")

        proc_b.emit({"type": "tool_execution_start", "tool": "bash", "toolCallId": "x"})
        self.assertTrue(wait_until(lambda: manager.status(task_b)["execution_state"] == EXEC_TOOL_RUNNING))
        self.assertEqual(manager.status(task_a)["execution_state"], EXEC_SETTLED,
                          "task a must stay SETTLED, unaffected by task b's tool")


# ---------------------------------------------------------------------------
# RPC command/response correlation
# ---------------------------------------------------------------------------


class TestRpcCommandCorrelation(unittest.TestCase):
    def test_responses_route_to_the_matching_command_even_out_of_order(self):
        from rpc_transport import PiRpcTransport

        process = FakePiProcess(command_handler=lambda p, obj: None)  # manual responses
        events = []
        transport = PiRpcTransport(process, on_event=events.append)

        results = {}
        errors = {}

        def call(name):
            try:
                results[name] = transport.send_command(name, {}, timeout=3.0)
            except Exception as exc:  # noqa: BLE001
                errors[name] = exc

        import threading
        t1 = threading.Thread(target=call, args=("first",))
        t2 = threading.Thread(target=call, args=("second",))
        t1.start()
        t2.start()

        self.assertTrue(process.wait_until_command("first"))
        self.assertTrue(process.wait_until_command("second"))
        first_id = next(c["id"] for c in process.commands_received if c["type"] == "first")
        second_id = next(c["id"] for c in process.commands_received if c["type"] == "second")

        # Respond out of order and with each other's id swapped to prove the
        # correlation is by id, not by arrival/send order.
        process.emit({"type": "response", "command": "second", "id": second_id,
                      "success": True, "data": {"who": "second"}})
        process.emit({"type": "response", "command": "first", "id": first_id,
                      "success": True, "data": {"who": "first"}})
        t1.join(timeout=3)
        t2.join(timeout=3)

        self.assertEqual(results.get("first", {}).get("who"), "first")
        self.assertEqual(results.get("second", {}).get("who"), "second")
        process.finish(0)


# ---------------------------------------------------------------------------
# Real Pi RPC outgoing wire shape: {"type": ..., "id": ...}, never the old
# {"command": ...} shape, and "message" (not "prompt") for prompt/steer text.
# ---------------------------------------------------------------------------


class TestOutgoingWireShape(unittest.TestCase):
    def test_outgoing_commands_use_real_pi_wire_shape(self):
        from rpc_transport import PiRpcTransport

        process = FakePiProcess()
        transport = PiRpcTransport(process, on_event=lambda ev: None)

        transport.get_state(timeout=3.0)
        self.assertTrue(process.wait_until_command("get_state"))
        state_cmd = next(c for c in process.commands_received if c.get("type") == "get_state")
        self.assertNotIn("command", state_cmd,
                          "outgoing get_state must not use the old 'command' key")
        self.assertEqual(state_cmd["type"], "get_state")

        transport.send_prompt("hello there")
        self.assertTrue(process.wait_until_command("prompt"))
        prompt_cmd = next(c for c in process.commands_received if c.get("type") == "prompt")
        self.assertNotIn("command", prompt_cmd,
                          "outgoing prompt must not use the old 'command' key")
        self.assertNotIn("prompt", prompt_cmd,
                          "outgoing prompt must not use the old 'prompt' field name")
        self.assertEqual(prompt_cmd.get("message"), "hello there")

        transport.send_steer("steer this way")
        self.assertTrue(process.wait_until_command("steer"))
        steer_cmd = next(c for c in process.commands_received if c.get("type") == "steer")
        self.assertNotIn("command", steer_cmd)
        self.assertEqual(steer_cmd.get("message"), "steer this way")

        transport.send_abort(timeout=3.0)
        self.assertTrue(process.wait_until_command("abort"))
        abort_cmd = next(c for c in process.commands_received if c.get("type") == "abort")
        self.assertNotIn("command", abort_cmd)

        transport.get_entries(since="cursor-1", timeout=3.0)
        self.assertTrue(process.wait_until_command("get_entries"))
        entries_cmd = next(c for c in process.commands_received if c.get("type") == "get_entries")
        self.assertNotIn("command", entries_cmd)
        self.assertEqual(entries_cmd.get("since"), "cursor-1")

        process.finish(0)


# ---------------------------------------------------------------------------
# Hard timeout -> abort -> SIGTERM -> SIGKILL escalation
# ---------------------------------------------------------------------------


class TestHardTimeoutEscalation(PiManagerTestCase):
    def test_emergency_cap_escalates_to_sigkill(self):
        """The infrastructure backstop still escalates SIGTERM -> SIGKILL.

        Renamed from hard_timeout: this path is no longer a task deadline,
        only a catastrophic cap (default 6 h). Escalation behaviour itself is
        unchanged and must not regress.
        """
        process = FakePiProcess()
        process.terminate_is_effective = False  # simulate a stuck process
        thresholds = Thresholds(
            **{**TEST_THRESHOLDS.__dict__, "emergency_cap_seconds": 100.0,
               "terminate_grace_seconds": 0.2}
        )
        manager = PiManager(registry=self.registry, popen_factory=fake_popen_factory(process),
                             clock=self.clock, default_thresholds=thresholds)
        task_id = self.start_and_boot(manager, process)

        self.clock.advance(thresholds.emergency_cap_seconds + 1)
        manager.watchdog_tick(task_id)

        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_ABORTED)
        self.assertEqual(status["exit_code"], -9, "must escalate to SIGKILL when SIGTERM is ignored")
        self.assertTrue(any(c.get("type") == "abort" for c in process.commands_received),
                         "must attempt RPC abort before signalling the process")


# ---------------------------------------------------------------------------
# Malformed JSONL resilience
# ---------------------------------------------------------------------------


class TestMalformedJsonlResilience(PiManagerTestCase):
    def test_malformed_line_is_skipped_and_recorded_without_crashing(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        process.emit_raw("this is not json { { {")
        process.emit_raw("")
        process.emit({"type": "agent_start"})
        process.emit_raw("also not json")
        process.emit({"type": "agent_settled"})

        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        events = self.registry.recent_events(task_id, limit=100)
        malformed = [e for e in events if e["event_type"] == "malformed_jsonl"]
        self.assertEqual(len(malformed), 2)


# ---------------------------------------------------------------------------
# UNRESPONSIVE: heartbeat timeout, then later recovery
# ---------------------------------------------------------------------------


class TestUnresponsiveThenRecovers(PiManagerTestCase):
    def test_heartbeat_timeout_yields_unresponsive_then_recovers(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_start"})
        wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_RUNNING)

        process.hang_commands.add("get_state")
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_UNRESPONSIVE)

        process.hang_commands.discard("get_state")
        # A real event proves liveness even before the next heartbeat.
        process.emit({"type": "message_update"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] not in (EXEC_UNRESPONSIVE,)))


# ---------------------------------------------------------------------------
# Defect 1: dispose the long-lived Pi RPC process after semantic settlement
# ---------------------------------------------------------------------------


class TestProcessDisposalAfterSettlement(PiManagerTestCase):
    def test_settled_process_is_terminated_and_task_stays_settled(self):
        process = FakePiProcess()
        verifier = VerifierSpec(argv=[sys.executable, "-c", "import sys; sys.exit(0)"])
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, verifier=verifier)

        self.assertIsNone(process.poll(), "process must still be alive before settlement")
        process.emit({"type": "agent_settled"})

        # The installed Pi 0.84.2 RPC process never exits on its own after a
        # command; the manager must actively terminate it once settlement is
        # persisted.
        self.assertTrue(
            wait_until(lambda: process.poll() is not None),
            "settled process was never terminated by the manager",
        )
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_SETTLED,
                          "the manager-initiated termination must never be mistaken for a crash")
        self.assertNotEqual(status["execution_state"], EXEC_CRASHED)
        self.assertEqual(status["exit_code"], -15,
                          "the manager-initiated SIGTERM exit code must be recorded "
                          "without changing the settled state")

        # A normal verifier PASS must still correctly derive DONE afterward,
        # proving disposal does not interfere with the verification axis.
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["verification_state"] == VERIFY_PASS))
        status = manager.status(task_id)
        self.assertEqual(status["state"], DERIVED_DONE)
        self.assertIsNotNone(status["settled_at"])

    def test_disposal_does_not_run_watchdog_crash_detection(self):
        # A watchdog_tick called AFTER settlement (e.g. a straggling tick
        # from the loop before it observes stop_watchdog) must never flip a
        # SETTLED task to CRASHED just because the disposed process has now
        # exited.
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_settled"})
        self.assertTrue(wait_until(lambda: process.poll() is not None))
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))

        manager.watchdog_tick(task_id)  # must be a no-op: execution_state is final
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_SETTLED)


# ---------------------------------------------------------------------------
# Defect 2: default leaf-worker system prompt file (best-effort, override
# wins, absence never fails; renamed from pi-parent.md to pi-worker.md —
# Hermes is the parent, this Pi is the leaf worker)
# ---------------------------------------------------------------------------


class TestDefaultSystemPromptFile(PiManagerTestCase):
    def test_renamed_default_points_at_pi_worker_not_pi_parent(self):
        """The versioned default was renamed pi-parent.md -> pi-worker.md
        (the prompt states the WORKER's policy; Hermes is the parent). The
        module constant must point at the new name and never require the
        old one."""
        import core  # type: ignore
        self.assertEqual(core.DEFAULT_SYSTEM_PROMPT_FILE.name, "pi-worker.md")
        self.assertNotEqual(core.DEFAULT_SYSTEM_PROMPT_FILE.name, "pi-parent.md")

    def test_renamed_default_selected_without_old_path_present(self):
        """Only the renamed file exists (no pi-parent.md anywhere in the
        prompts dir): the default is still selected and passed to argv."""
        process = FakePiProcess()
        captured_argv: list = []

        def factory(argv, cwd):
            captured_argv.append(argv)
            process.sync_session_state_from_argv(argv)
            return process

        prompts_dir = self.tmp / "prompts"
        prompts_dir.mkdir()
        worker_prompt = prompts_dir / "pi-worker.md"
        worker_prompt.write_text("leaf worker policy", encoding="utf-8")
        self.assertFalse((prompts_dir / "pi-parent.md").exists())
        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        with mock.patch("core.DEFAULT_SYSTEM_PROMPT_FILE", worker_prompt):
            self.start_and_boot(manager, process)
        self.assertIn("--append-system-prompt", captured_argv[0])
        idx = captured_argv[0].index("--append-system-prompt")
        self.assertEqual(captured_argv[0][idx + 1], str(worker_prompt))

    def test_default_path_used_when_it_exists(self):
        process = FakePiProcess()
        captured_argv: list = []

        def factory(argv, cwd):
            captured_argv.append(argv)
            process.sync_session_state_from_argv(argv)
            return process

        default_prompt = self.tmp / "pi-worker.md"
        default_prompt.write_text("default worker policy", encoding="utf-8")
        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        with mock.patch("core.DEFAULT_SYSTEM_PROMPT_FILE", default_prompt):
            self.start_and_boot(manager, process)
        self.assertTrue(captured_argv, "spawn was never called")
        self.assertIn("--append-system-prompt", captured_argv[0])
        idx = captured_argv[0].index("--append-system-prompt")
        self.assertEqual(captured_argv[0][idx + 1], str(default_prompt))

    def test_explicit_override_beats_existing_default_in_argv(self):
        process = FakePiProcess()
        captured_argv: list = []

        def factory(argv, cwd):
            captured_argv.append(argv)
            process.sync_session_state_from_argv(argv)
            return process

        default_prompt = self.tmp / "pi-worker-default.md"
        default_prompt.write_text("default", encoding="utf-8")
        override_prompt = self.tmp / "pi-worker-override.md"
        override_prompt.write_text("override", encoding="utf-8")
        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        with mock.patch("core.DEFAULT_SYSTEM_PROMPT_FILE", default_prompt):
            self.start_and_boot(manager, process, system_prompt_file=str(override_prompt))
        idx = captured_argv[0].index("--append-system-prompt")
        self.assertEqual(captured_argv[0][idx + 1], str(override_prompt))

    def test_no_failure_when_default_missing_and_no_override(self):
        process = FakePiProcess()
        captured_argv: list = []

        def factory(argv, cwd):
            captured_argv.append(argv)
            process.sync_session_state_from_argv(argv)
            return process

        missing_default = self.tmp / "does-not-exist" / "pi-worker.md"
        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        with mock.patch("core.DEFAULT_SYSTEM_PROMPT_FILE", missing_default):
            task_id = self.start_and_boot(manager, process)
        self.assertNotIn("--append-system-prompt", captured_argv[0])
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_RUNNING)


# ---------------------------------------------------------------------------
# Defect 4: heal UNRESPONSIVE after a successful, CHANGING heartbeat
# ---------------------------------------------------------------------------


class TestUnresponsiveHealsOnChangingHeartbeat(PiManagerTestCase):
    def test_heartbeat_with_changed_state_heals_unresponsive_to_running(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        # start_and_boot already waits for RUNNING; no extra agent_start
        # event is needed here, and emitting one unsynced would race with
        # the ticks below (its async application could land after them).

        # Drive into UNRESPONSIVE via a hanging heartbeat, exactly like the
        # existing event-based-recovery scenario.
        process.hang_commands.add("get_state")
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_UNRESPONSIVE)

        # Now the heartbeat starts succeeding again AND the state payload
        # changes: this must heal UNRESPONSIVE via the heartbeat path alone
        # (no event is emitted here) -- previously only WAITING/STALLED were
        # healed by a changing heartbeat, leaving UNRESPONSIVE stuck.
        process.hang_commands.discard("get_state")
        process.state = dict(process.state, messageCount=process.state["messageCount"] + 1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_RUNNING,
                          "a changed, successful heartbeat must heal UNRESPONSIVE")

    def test_unresponsive_recovers_to_tool_running_when_tool_was_active(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        process.emit({"type": "agent_start"})
        self.emit_and_sync(manager, process, task_id,
                            {"type": "tool_execution_start", "tool": "bash", "toolCallId": "t1"})
        wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_TOOL_RUNNING)

        process.hang_commands.add("get_state")
        self.clock.advance(TEST_THRESHOLDS.tool_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_UNRESPONSIVE)

        process.hang_commands.discard("get_state")
        process.state = dict(process.state, messageCount=process.state["messageCount"] + 1)
        manager.watchdog_tick(task_id)
        status = manager.status(task_id)
        self.assertEqual(status["execution_state"], EXEC_TOOL_RUNNING,
                          "must recover to the pre-UNRESPONSIVE active state, not just RUNNING")

    def test_event_based_recovery_from_unresponsive_still_works(self):
        # Regression guard: the pre-existing event-based recovery path
        # (TestUnresponsiveThenRecovers) must remain supported alongside the
        # new heartbeat-based path.
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        # start_and_boot already waits for RUNNING; see the note above about
        # not emitting a redundant, unsynced agent_start here.

        process.hang_commands.add("get_state")
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_UNRESPONSIVE)

        process.hang_commands.discard("get_state")
        process.emit({"type": "message_update"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] not in (EXEC_UNRESPONSIVE,)))


# ---------------------------------------------------------------------------
# Fix 1 (packet 3): explicit per-task session file — <registry-parent>/
# sessions/<uuid>.jsonl allocated and persisted BEFORE boot, passed via
# --session; never --no-session, never Pi's implicit default storage.
# ---------------------------------------------------------------------------


class TestExplicitSessionFile(PiManagerTestCase):
    def test_argv_uses_persisted_session_path_and_never_no_session(self):
        process = FakePiProcess()
        captured: list = []

        def factory(argv, cwd):
            captured.append(list(argv))
            process.sync_session_state_from_argv(argv)
            return process

        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        result = manager.start_task(prompt="hi", cwd=str(self.cwd_dir))
        task_id = result["task_id"]

        # Persisted at the moment start_task returned — i.e. BEFORE the boot
        # thread can have spawned anything: the row already carries the
        # allocated path as both session_file and expected_session_file.
        row = self.registry.get_task(task_id)
        self.assertTrue(row["session_file"], "session_file must be persisted before boot")
        self.assertEqual(row["expected_session_file"], row["session_file"])
        self.assertEqual(result["session_file"], row["session_file"])
        allocated = Path(row["session_file"])
        self.assertTrue(allocated.name.endswith(".jsonl"))
        self.assertEqual(allocated.parent.name, "sessions")
        self.assertEqual(allocated.parent.parent, self.db_path.parent,
                          "session files live under <registry-parent>/sessions")
        self.assertTrue(allocated.is_file(), "the allocated session file must be durable")

        # The outgoing argv must carry exactly that path via --session and
        # must never carry --no-session for a managed task.
        self.assertTrue(process.wait_until_command("get_state"))
        self.assertTrue(captured, "spawn never happened")
        argv = captured[0]
        self.assertIn("--session", argv, "managed tasks must use an explicit --session")
        self.assertEqual(argv[argv.index("--session") + 1], str(allocated))
        self.assertNotIn("--no-session", argv, "managed tasks must never use --no-session")

        # The fake reports exactly the --session path it was spawned with
        # (like real Pi), so boot verifies and the persisted identity must
        # remain the allocated file, unchanged. A differing reported
        # sessionFile is now a rejection (TestBootIdentityRejection).
        row = self.registry.get_task(task_id)
        self.assertEqual(row["session_file"], str(allocated))
        self.assertEqual(row["expected_session_file"], str(allocated))

    def test_two_managed_tasks_get_distinct_session_files(self):
        proc_a = FakePiProcess()
        proc_b = FakePiProcess()
        calls = {"n": 0}
        captured: list = []

        def factory(argv, cwd):
            calls["n"] += 1
            captured.append(list(argv))
            proc = proc_a if calls["n"] == 1 else proc_b
            proc.sync_session_state_from_argv(argv)
            return proc

        manager = PiManager(registry=self.registry, popen_factory=factory,
                             clock=self.clock, default_thresholds=TEST_THRESHOLDS)
        task_a = manager.start_task(prompt="a", cwd=str(self.cwd_dir))["task_id"]
        self.assertTrue(proc_a.wait_until_command("get_state"))
        self.assertEqual(len(captured), 1, "only task A may have spawned so far")
        row_a = self.registry.get_task(task_a)
        argv_a = captured[0]
        self.assertEqual(argv_a[argv_a.index("--session") + 1], row_a["session_file"])

        task_b = manager.start_task(prompt="b", cwd=str(self.cwd_dir))["task_id"]
        self.assertTrue(proc_b.wait_until_command("get_state"))
        argv_b = captured[1]
        row_b = self.registry.get_task(task_b)
        self.assertEqual(argv_b[argv_b.index("--session") + 1], row_b["session_file"])
        self.assertNotIn("--no-session", argv_a)
        self.assertNotIn("--no-session", argv_b)
        self.assertNotEqual(row_a["session_file"], row_b["session_file"],
                              "each managed task must get its own unique session file")


# ---------------------------------------------------------------------------
# Boot identity hardening: the initial get_state must PROVE the process
# owns the pre-allocated session BEFORE any identity is persisted or any
# prompt is sent. A response missing a non-empty sessionId, missing
# sessionFile, or reporting a sessionFile whose canonical path differs
# from the allocated one is rejected: bounded startup/session identity
# diagnostic recorded, process terminated, explicit non-success state
# (CRASHED), prompt NEVER sent, persisted (expected) identity untouched.
# ---------------------------------------------------------------------------


class TestBootIdentityRejection(PiManagerTestCase):
    def _boot_rejected(self, process: FakePiProcess):
        manager = self.make_manager(process)
        result = manager.start_task(prompt="MUST NEVER BE SENT", cwd=str(self.cwd_dir))
        task_id = result["task_id"]
        allocated = result["session_file"]
        self.assertTrue(process.wait_until_command("get_state"), "boot never called get_state")
        self.assertTrue(
            wait_until(lambda: manager.status(task_id)["execution_state"] == EXEC_CRASHED),
            "task never reached CRASHED after a rejected boot identity",
        )
        return task_id, allocated

    def _assert_rejection_invariants(self, process: FakePiProcess, task_id: str,
                                      allocated: str, reason_prefix: str):
        row = self.registry.get_task(task_id)
        # The persisted (expected) identity must be the pre-allocated
        # ground truth, untouched by the rejected response.
        self.assertEqual(row["session_file"], allocated)
        self.assertEqual(row["expected_session_file"], allocated)
        self.assertIsNone(row["session_id"], "rejected identity must never be persisted")
        self.assertIsNone(row["expected_session_id"], "rejected identity must never be persisted")
        # Explicit non-success state with a bounded diagnostic.
        self.assertEqual(row["execution_state"], EXEC_CRASHED)
        self.assertIn("session identity rejected", row["last_error"])
        # A bounded startup/session identity diagnostic was recorded, and
        # its reason identifies the specific failure.
        events = self.registry.recent_events(task_id, limit=50)
        rejected = [e for e in events if e["event_type"] == "session_identity_rejected"]
        self.assertTrue(rejected, "a startup/session identity diagnostic must be recorded")
        summary = json.loads(rejected[0]["summary"])
        self.assertTrue(summary["reason"].startswith(reason_prefix),
                        f"diagnostic reason {summary['reason']!r} must start with {reason_prefix!r}")
        # The prompt must never have been sent to an unverified process.
        self.assertFalse(
            any(c.get("type") == "prompt" for c in process.commands_received),
            "prompt must never be sent after a rejected boot identity",
        )
        # The process must have been terminated.
        self.assertIsNotNone(process.poll(), "rejected process must be terminated")

    def test_missing_session_id_rejects_before_prompt(self):
        process = FakePiProcess()
        process.state = dict(process.state, sessionId=None)
        task_id, allocated = self._boot_rejected(process)
        self._assert_rejection_invariants(process, task_id, allocated, "missing_session_id")

    def test_empty_session_id_rejects_before_prompt(self):
        process = FakePiProcess()
        process.state = dict(process.state, sessionId="")
        task_id, allocated = self._boot_rejected(process)
        self._assert_rejection_invariants(process, task_id, allocated, "missing_session_id")

    def test_missing_session_file_rejects_before_prompt(self):
        process = FakePiProcess()
        process.state = dict(process.state, sessionFile=None)
        task_id, allocated = self._boot_rejected(process)
        self._assert_rejection_invariants(process, task_id, allocated, "missing_session_file")

    def test_mismatched_session_file_rejects_before_prompt(self):
        process = FakePiProcess()
        process.state = dict(process.state, sessionFile=str(self.tmp / "some-other-session.jsonl"))
        task_id, allocated = self._boot_rejected(process)
        self._assert_rejection_invariants(process, task_id, allocated, "session_file_mismatch")


# ---------------------------------------------------------------------------
# Fix 2 (packet 3): real periodic get_state heartbeat on its own interval —
# due before the soft-stall threshold; unchanged => last_state_at only;
# changed => progress + grace clear + heal; timeout => UNRESPONSIVE even
# with recent progress.
# ---------------------------------------------------------------------------


class TestPeriodicHeartbeatBeforeSoftStall(PiManagerTestCase):
    def test_changed_heartbeat_before_soft_stall_refreshes_progress(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        t0 = self.clock()
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        # Past the heartbeat interval (20s) but far inside the soft-stall
        # threshold (90s): the watchdog must issue get_state NOW.
        self.clock.advance(TEST_THRESHOLDS.heartbeat_seconds + 1)
        process.state = dict(process.state, messageCount=process.state["messageCount"] + 1)
        manager.watchdog_tick(task_id)

        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_RUNNING,
                          "a successful changed heartbeat before soft stall keeps the task RUNNING")
        self.assertEqual(row["last_progress_at"], t0 + TEST_THRESHOLDS.heartbeat_seconds + 1,
                          "a changed heartbeat must refresh last_progress_at")
        self.assertIsNotNone(row["last_state_changed_at"])
        get_state_cmds = [c for c in process.commands_received if c.get("type") == "get_state"]
        self.assertGreaterEqual(len(get_state_cmds), 2,
                                 "watchdog must issue a get_state before the soft-stall threshold")

        # The refreshed progress must prevent a true stall: a full soft-stall
        # window later (no further change) may be at most WAITING.
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertNotEqual(row["execution_state"], EXEC_STALLED,
                             "progress made by the earlier heartbeat must prevent a true stall")
        self.assertEqual(row["last_progress_at"], t0 + TEST_THRESHOLDS.heartbeat_seconds + 1,
                          "the later unchanged heartbeat must not refresh progress again")

    def test_heartbeat_timeout_marks_unresponsive_even_with_recent_progress(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        # Progress is recent (21s idle) and far short of soft_stall_seconds,
        # but the heartbeat interval has elapsed and get_state hangs: a
        # failing heartbeat must mark UNRESPONSIVE immediately, not wait
        # for the stall thresholds.
        process.hang_commands.add("get_state")
        self.clock.advance(TEST_THRESHOLDS.heartbeat_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_UNRESPONSIVE,
                          "a heartbeat timeout must mark UNRESPONSIVE even with recent progress")

    def test_unchanged_heartbeat_before_soft_stall_only_updates_last_state_at(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        t0 = self.clock()
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})
        progress_at = self.registry.get_task(task_id)["last_progress_at"]

        self.clock.advance(TEST_THRESHOLDS.heartbeat_seconds + 1)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_WAITING,
                          "quiet-but-responsive task degrades to WAITING, nothing worse")
        self.assertEqual(row["last_progress_at"], progress_at,
                          "an unchanged heartbeat must not refresh last_progress_at")
        self.assertEqual(row["last_state_at"], t0 + TEST_THRESHOLDS.heartbeat_seconds + 1,
                          "an unchanged heartbeat still records last_state_at")

        # ...and the unchanged heartbeat must never defeat a later true
        # stall: soft stall + grace with no progress -> STALLED, with the
        # progress marker untouched.
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.clock.advance(TEST_THRESHOLDS.stall_grace_seconds + 1)
        manager.watchdog_tick(task_id)
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_STALLED)
        self.assertEqual(row["last_progress_at"], progress_at,
                          "reaching STALLED must not be preceded by a phantom progress refresh")

    def test_changed_heartbeat_clears_grace_and_heals_waiting_to_running(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})

        # Into WAITING with an unchanged heartbeat + started grace window...
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_WAITING)

        # ...then a CHANGED heartbeat: the grace must clear and the task
        # must heal back to RUNNING.
        self.clock.advance(TEST_THRESHOLDS.heartbeat_seconds + 1)
        process.state = dict(process.state, messageCount=process.state["messageCount"] + 1)
        manager.watchdog_tick(task_id)
        self.assertEqual(manager.status(task_id)["execution_state"], EXEC_RUNNING,
                          "a changed heartbeat must heal WAITING back to RUNNING and clear grace")

        # Proof the grace was cleared, not inherited: a full further
        # soft-stall window with no progress may put the task at most back
        # in WAITING, never straight to STALLED.
        self.clock.advance(TEST_THRESHOLDS.soft_stall_seconds + 1)
        manager.watchdog_tick(task_id)
        self.assertNotEqual(manager.status(task_id)["execution_state"], EXEC_STALLED)


# ---------------------------------------------------------------------------
# Defect 3: recovery runs at plugin registration time, not lazily
# ---------------------------------------------------------------------------


class TestRecoveryRunsAtRegistration(unittest.TestCase):
    def test_register_triggers_manager_init_and_recovery_without_a_tool_call(self):
        plugin_dir = Path(__file__).resolve().parent.parent
        tmp = Path(tempfile.mkdtemp(prefix="pi-manager-register-test-"))
        try:
            # get_manager() resolves the registry path via
            # registry_db.default_db_path(), i.e.
            # $HERMES_HOME/state/pi-manager/registry.sqlite3 — seed and
            # re-read the SAME computed path, not an arbitrary one, or the
            # manager constructed inside register() will see an empty DB.
            from registry_db import default_db_path
            db_path = default_db_path(hermes_home=str(tmp))
            registry = Registry(db_path=db_path)
            # Seed a stale STARTING row with no session file: recover_task
            # must reject it (ABORTED), proving recover_all() actually ran.
            registry.create_task(
                task_id="stale-task", cwd=str(tmp), execution_state=EXEC_STARTING,
                verification_state=VERIFY_NOT_RUN,
            )
            registry.close()

            os.environ["HERMES_HOME"] = str(tmp)
            sys.path.insert(0, str(plugin_dir))
            import importlib
            import tools as tools_module
            importlib.reload(tools_module)
            import __init__ as plugin_init  # noqa: N813 - package-relative alias for testing
            importlib.reload(plugin_init)

            registered = []

            class _FakeCtx:
                def register_tool(self, **kw):
                    registered.append(kw["name"])

            try:
                plugin_init.register(_FakeCtx())
            finally:
                tools_module.reset_manager_for_tests()

            self.assertTrue(registered, "register_all must still register tools")
            registry2 = Registry(db_path=db_path)
            row = registry2.get_task("stale-task")
            registry2.close()
            self.assertEqual(row["execution_state"], EXEC_ABORTED,
                              "recovery must have run during register(), with no tool call")
        finally:
            os.environ.pop("HERMES_HOME", None)
            shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Fix (packet 4): event-derived status metrics — a short event-only task
# must show useful message_count/is_streaming/is_compacting before and
# after settlement, without ever waiting on the next periodic get_state.
# ---------------------------------------------------------------------------


class TestEventDerivedStatusMetrics(PiManagerTestCase):
    def test_short_event_only_task_has_status_metrics_before_and_after_settlement(self):
        process = FakePiProcess()
        # Authoritative boot snapshot: empty, non-streaming session.
        process.state = dict(process.state, messageCount=0, isStreaming=False)
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        row = self.registry.get_task(task_id)
        self.assertEqual(row["message_count"], 0)
        self.assertEqual(row["is_streaming"], 0)

        # Pure event stream, NO watchdog tick and NO further get_state in
        # between — every metric below must come from the events alone.
        self.emit_and_sync(manager, process, task_id, {"type": "agent_start"})
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        self.emit_and_sync(
            manager, process, task_id,
            {"type": "message_update", "text": "TOPSECRET_PAYLOAD_MARKER" * 100})
        self.emit_and_sync(manager, process, task_id, {"type": "message_start"})
        self.emit_and_sync(
            manager, process, task_id,
            {"type": "tool_execution_start", "tool": "read", "toolCallId": "t1"})
        self.emit_and_sync(
            manager, process, task_id, {"type": "tool_execution_update", "toolCallId": "t1"})
        self.emit_and_sync(
            manager, process, task_id, {"type": "tool_execution_end", "toolCallId": "t1"})

        row = self.registry.get_task(task_id)
        self.assertEqual(row["message_count"], 2,
                         "message_count must be incremented from the persisted value by message_start")
        self.assertEqual(row["is_streaming"], 1)
        self.assertEqual(row["execution_state"], EXEC_RUNNING)

        # No heartbeat must have been issued: get_state count is still the
        # single boot-time snapshot.
        get_state_cmds = [c for c in process.commands_received if c.get("type") == "get_state"]
        self.assertEqual(len(get_state_cmds), 1)

        # The event log must be bounded: the message_update summary carries
        # a length, never the full text payload.
        events = self.registry.recent_events(task_id, limit=100)
        mu_events = [e for e in events if e["event_type"] == "message_update"]
        self.assertEqual(len(mu_events), 1)
        self.assertIn("text_len", mu_events[0]["summary"])
        self.assertNotIn("TOPSECRET_PAYLOAD_MARKER", mu_events[0]["summary"])

        # agent_end stops the stream but is still not settlement.
        self.emit_and_sync(
            manager, process, task_id, {"type": "agent_end", "willRetry": False})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["is_streaming"], 0)
        self.assertEqual(row["execution_state"], EXEC_RUNNING,
                         "agent_end must not settle the task")

        # agent_settled: flags cleared, SETTLED preserved, the deliberate
        # disposal exit is a code — never a CRASHED classification.
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        self.assertTrue(wait_until(lambda: process.poll() is not None))
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_SETTLED)
        self.assertNotEqual(row["execution_state"], EXEC_CRASHED)
        self.assertEqual(row["is_streaming"], 0)
        self.assertEqual(row["is_compacting"], 0)
        self.assertEqual(row["message_count"], 2)
        status = manager.status(task_id)
        self.assertEqual(status["state"], EXEC_SETTLED,
                         "no verifier => SETTLED/NOT_RUN, never acceptance")

    def test_compaction_flags_transition(self):
        process = FakePiProcess()
        process.state = dict(process.state, messageCount=0, isStreaming=False)
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process)

        self.emit_and_sync(manager, process, task_id, {"type": "compaction_start"})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["is_compacting"], 1)
        self.assertEqual(row["execution_state"], EXEC_RUNNING,
                         "compaction is a status flag, not an execution-state change")

        self.emit_and_sync(manager, process, task_id, {"type": "compaction_end"})
        row = self.registry.get_task(task_id)
        self.assertEqual(row["is_compacting"], 0)

        # A settle following compaction must leave both flags cleared while
        # keeping execution_state=SETTLED.
        self.emit_and_sync(manager, process, task_id, {"type": "compaction_start"})
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        self.assertTrue(wait_until(
            lambda: manager.status(task_id)["execution_state"] == EXEC_SETTLED))
        row = self.registry.get_task(task_id)
        self.assertEqual(row["execution_state"], EXEC_SETTLED)
        self.assertEqual(row["is_streaming"], 0)
        self.assertEqual(row["is_compacting"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
