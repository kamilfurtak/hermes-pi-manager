"""A gate that cannot run is not a gate that failed.

On 2026-08-28 three consecutive tasks were verified with
`bash -n ops/deploy-cms-strapi.sh`. Each returned 127 and each was recorded
as a plain FAIL, which reads as "the work is wrong" and sends a human to the
diff. No verdict had been reached at all.

The cause turned out to be the opposite of the obvious reading: the script
was supposed to exist, and the first two attempts simply never wrote it. The
third did, and passed. So UNRUNNABLE means "no verdict", and must not claim
the gate is at fault — either side can be.
"""
from __future__ import annotations

import json
import unittest

from test_pi_manager import PiManagerTestCase, FakePiProcess  # type: ignore
from core import VerifierSpec, derived_state  # type: ignore


class TestUnrunnableGate(PiManagerTestCase):
    def _settled(self, manager, process):
        task_id = self.start_and_boot(manager, process)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        return task_id

    def test_missing_command_is_unrunnable_not_failed(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self._settled(manager, process)
        outcome = manager.run_verifier_now(
            task_id, VerifierSpec(argv=["bash", "-n", "no/such/file.sh"],
                                  timeout_seconds=20.0))
        self.assertEqual(outcome, "UNRUNNABLE")
        self.assertEqual(derived_state("SETTLED", outcome), "GATE_UNRUNNABLE")

    def test_a_real_failure_is_still_failed(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self._settled(manager, process)
        # Runs fine, exits non-zero: the gate worked and the answer was no.
        outcome = manager.run_verifier_now(
            task_id, VerifierSpec(argv=["false"], timeout_seconds=20.0))
        self.assertEqual(outcome, "FAIL")

    def test_success_is_unaffected(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self._settled(manager, process)
        self.assertEqual(
            manager.run_verifier_now(
                task_id, VerifierSpec(argv=["true"], timeout_seconds=20.0)),
            "PASS")

    def test_diagnosis_is_recorded(self):
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self._settled(manager, process)
        manager.run_verifier_now(
            task_id, VerifierSpec(argv=["bash", "-n", "no/such/file.sh"],
                                  timeout_seconds=20.0))
        ev = [e for e in self.registry.recent_events(task_id, limit=50)
              if e["event_type"] == "verifier_run"][-1]
        payload = json.loads(ev["summary"])
        self.assertEqual(payload["returncode"], 127)
        self.assertIn("no verdict was reached", payload["diagnosis"])


class TestRepeatedGateFailure(PiManagerTestCase):
    def _run(self, argv):
        # The verifier must be attached at dispatch, as pi_task does: the
        # repeat check compares the stored verifier_spec of earlier tasks, and
        # a spec passed only to run_verifier_now is never persisted.
        spec = VerifierSpec(argv=argv, timeout_seconds=20.0)
        process = FakePiProcess()
        manager = self.make_manager(process)
        task_id = self.start_and_boot(manager, process, verifier=spec)
        self.emit_and_sync(manager, process, task_id, {"type": "agent_settled"})
        from test_pi_manager import wait_until  # type: ignore
        wait_until(lambda: (self.registry.get_task(task_id) or {})
                   .get("verification_state") not in ("PENDING", None))
        return manager, task_id

    def _repeats(self, task_id):
        return [e for e in self.registry.recent_events(task_id, limit=100)
                if e["event_type"] == "verifier_repeat_failure"]

    def test_first_failure_is_not_flagged(self):
        _m, t = self._run(["bash", "-n", "no/such/file.sh"])
        self.assertEqual(self._repeats(t), [])

    def test_second_identical_failure_is_flagged(self):
        _m, first = self._run(["bash", "-n", "no/such/file.sh"])
        _m2, second = self._run(["bash", "-n", "no/such/file.sh"])
        flagged = self._repeats(second)
        self.assertTrue(flagged, "a repeated identical gate failure must be flagged")
        payload = json.loads(flagged[-1]["summary"])
        self.assertEqual(payload["attempt"], 2)
        self.assertIn(first, payload["previous_task_ids"])

    def test_a_different_gate_is_not_flagged(self):
        self._run(["bash", "-n", "no/such/file.sh"])
        _m, other = self._run(["false"])          # different argv, also fails
        self.assertEqual(self._repeats(other), [],
                         "only the same gate counts as a repeat")

    def test_passing_runs_are_never_flagged(self):
        self._run(["bash", "-n", "no/such/file.sh"])
        _m, ok = self._run(["true"])
        self.assertEqual(self._repeats(ok), [])


if __name__ == "__main__":
    unittest.main()
