"""Live LSP feedback pump: steers fresh diagnostics into a running task."""

import json
import os
import tempfile
import time
import unittest

import lsp_feedback


def _wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _write_tool_call(path, tool="write"):
    entry = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [{"type": "toolCall", "name": tool}],
        },
    }
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry) + "\n")


class _Result:
    def __init__(self, findings, errors=None):
        self.findings = findings
        self.errors = errors if errors is not None else len(findings)

    def as_dict(self):
        return {
            "files": 2,
            "errors": self.errors,
            "warnings": 0,
            "findings": self.findings,
            "truncated": False,
        }


class TestLspFeedbackPump(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.session = os.path.join(self.tmp.name, "session.jsonl")
        self.steers = []
        self.records = []
        self.results = []
        original_run = lsp_feedback.lsp_check.run

        def fake_run(cwd, budget_seconds=15.0):
            if not self.results:
                return None
            return self.results.pop(0).as_dict()

        self._original_run = original_run
        lsp_feedback.lsp_check.run = fake_run
        self.addCleanup(setattr, lsp_feedback.lsp_check, "run", original_run)
        self.addCleanup(self.tmp.cleanup)

    def _pump(self, **kw):
        kw.setdefault("quiet_seconds", 0.05)
        kw.setdefault("poll_seconds", 0.02)
        kw.setdefault("max_reports", 4)
        pump = lsp_feedback.LspFeedbackPump(
            task_id="t1", cwd=self.tmp.name, session_file=self.session,
            steer=self.steers.append, record=self.records.append, **kw)
        pump.start()
        self.addCleanup(pump.stop)
        return pump

    def test_write_tool_steers_fresh_findings(self):
        self.results.append(_Result(["a.ts:12: bad name"]))
        self._pump()
        _write_tool_call(self.session)
        self.assertTrue(_wait_until(lambda: bool(self.steers)),
                        "expected a steer with fresh LSP findings")
        self.assertIn("a.ts:12: bad name", self.steers[0])
        self.assertEqual(len(self.records), 1)

    def test_empty_verdict_retries_and_steers_on_second_call(self):
        # tsserver warm-up race: first verdict comes back clean (timed out),
        # the retry catches the real finding.
        self.results.append(_Result([], errors=0))
        self.results.append(_Result(["a.ts:1: bad type"]))
        self._pump(retry_seconds=0.05)
        _write_tool_call(self.session)
        self.assertTrue(_wait_until(lambda: bool(self.steers)),
                        "expected a steer after the empty-verdict retry")
        self.assertIn("a.ts:1: bad type", self.steers[0])

    def test_empty_twice_stays_silent(self):
        self.results.append(_Result([], errors=0))
        self.results.append(_Result([], errors=0))
        self._pump(retry_seconds=0.05)
        _write_tool_call(self.session)
        self.assertFalse(_wait_until(lambda: bool(self.steers)),
                         "no findings means no steer, even after retry")
        self.assertEqual(self.records, [])

    def test_same_findings_are_not_steered_twice(self):
        self.results.append(_Result(["a.ts:12: bad name"]))
        self.results.append(_Result(["a.ts:12: bad name"]))
        self._pump()
        _write_tool_call(self.session)
        self.assertTrue(_wait_until(lambda: len(self.steers) == 1))
        _write_tool_call(self.session)
        time.sleep(0.2)
        self.assertEqual(len(self.steers), 1, "dedupe failed")

    def test_new_findings_after_more_writes_get_a_second_steer(self):
        self.results.append(_Result(["a.ts:12: bad name"]))
        self.results.append(_Result(["b.ts:3: wrong type"]))
        self._pump()
        _write_tool_call(self.session)
        self.assertTrue(_wait_until(lambda: len(self.steers) == 1))
        _write_tool_call(self.session)
        self.assertTrue(_wait_until(lambda: len(self.steers) == 2),
                        "a new finding after another write must steer again")
        self.assertIn("b.ts:3: wrong type", self.steers[1])

    def test_max_reports_caps_the_steer_storm(self):
        self.results.append(_Result(["a.ts:1: e1"]))
        self.results.append(_Result(["a.ts:2: e2"]))
        self.results.append(_Result(["a.ts:3: e3"]))
        self.results.append(_Result(["a.ts:4: e4"]))
        self._pump(max_reports=2)
        for _ in range(3):
            _write_tool_call(self.session)
            # gap > quiet window: each write is its own edit burst
            time.sleep(0.3)
        self.assertTrue(_wait_until(lambda: len(self.steers) == 2, timeout=4.0),
                        "expected the two budgeted steers to fire")
        time.sleep(0.4)  # give a would-be third check a chance to misbehave
        self.assertEqual(len(self.steers), 2, "budget exceeded")

    def test_read_only_tools_do_not_trigger_a_check(self):
        self.results.append(_Result(["a.ts:1: e"]))
        self._pump()
        _write_tool_call(self.session, tool="read")
        time.sleep(0.2)
        self.assertEqual(self.steers, [], "read tools must not trigger LSP")

    def test_no_verdict_stays_silent(self):
        self._pump()
        _write_tool_call(self.session)
        time.sleep(0.2)
        self.assertEqual(self.steers, [], "None verdict must stay silent")

    def test_stop_ends_the_thread(self):
        pump = self._pump()
        thread = pump._thread
        self.assertTrue(_wait_until(lambda: thread is not None and thread.is_alive()))
        pump.stop()
        assert thread is not None
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
