"""The semantic check and the verdict-carrying wake.

Both exist for the same reason: a delegated task should cost the
orchestrator as few turns as possible. The LSP pass runs in-process (zero
turns) and its result rides the ONE wake that already fires, so the common
"settled, gate green, nothing to look at" case needs no follow-up call.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_pi_manager import PiManagerTestCase  # type: ignore  (sets sys.path)

import lsp_check  # type: ignore
from core import (  # type: ignore
    EXEC_SETTLED, EXEC_STALLED, VERIFY_PASS, VERIFY_FAIL, VERIFY_NOT_RUN,
    format_terminal_wake_message, _compact_verifier_verdict,
)


class TestTouchedFileCollection(unittest.TestCase):
    """Pi does not commit, so the working tree IS the deliverable."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pi-lsp-")
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", self.dir], check=False))
        run = lambda *a: subprocess.run(a, cwd=self.dir, check=True,
                                        capture_output=True)
        run("git", "init", "-q")
        run("git", "config", "user.email", "t@t")
        run("git", "config", "user.name", "t")
        Path(self.dir, "base.py").write_text("x = 1\n")
        run("git", "add", "-A")
        run("git", "commit", "-qm", "base")

    def test_untracked_and_modified_sources_are_collected(self):
        Path(self.dir, "new.py").write_text("y = 2\n")           # untracked
        Path(self.dir, "base.py").write_text("x = 1\nz = 3\n")   # modified
        Path(self.dir, "notes.txt").write_text("ignore me\n")    # unserved ext
        found = {os.path.basename(p) for p in lsp_check.collect_touched_files(self.dir)}
        self.assertEqual(found, {"new.py", "base.py"},
                         "only source files Pi touched may be diagnosed")

    def test_deleted_files_are_not_collected(self):
        os.remove(os.path.join(self.dir, "base.py"))
        self.assertEqual(lsp_check.collect_touched_files(self.dir), [],
                         "a deleted file has nothing to diagnose")

    def test_non_git_directory_yields_nothing(self):
        plain = tempfile.mkdtemp(prefix="pi-plain-")
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", plain], check=False))
        Path(plain, "a.py").write_text("x = 1\n")
        self.assertEqual(lsp_check.collect_touched_files(plain), [])

    def test_missing_cwd_is_survivable(self):
        self.assertEqual(lsp_check.collect_touched_files(None), [])
        self.assertEqual(lsp_check.collect_touched_files("/nope/nope"), [])

    def test_collection_is_capped(self):
        for i in range(12):
            Path(self.dir, f"f{i}.py").write_text("x = 1\n")
        self.assertEqual(len(lsp_check.collect_touched_files(self.dir, limit=5)), 5)


class TestLspRunIsBestEffort(unittest.TestCase):
    """No verdict must never be reported as a clean verdict."""

    def test_no_service_returns_none_not_clean(self):
        real = lsp_check._service
        lsp_check._service = lambda: None
        self.addCleanup(lambda: setattr(lsp_check, "_service", real))
        self.assertIsNone(lsp_check.run("/tmp"))
        self.assertIsNone(lsp_check.format_summary(None),
                          "absence of a verdict must not render as text")

    def test_raising_server_is_swallowed(self):
        class Boom:
            def get_diagnostics_sync(self, *a, **kw):
                raise RuntimeError("server died")
        real_svc, real_files = lsp_check._service, lsp_check.collect_touched_files
        lsp_check._service = lambda: Boom()
        lsp_check.collect_touched_files = lambda cwd, **kw: ["/tmp/a.py"]
        self.addCleanup(lambda: (setattr(lsp_check, "_service", real_svc),
                                 setattr(lsp_check, "collect_touched_files", real_files)))
        self.assertIsNone(lsp_check.run("/tmp"), "a dead server yields no verdict")

    def test_quiet_and_dirty_summaries_read_differently(self):
        quiet = lsp_check.format_summary(
            {"files": 3, "errors": 0, "warnings": 0, "findings": []})
        self.assertIn("no LSP errors detected", quiet)
        self.assertNotIn("clean", quiet,
                         "silence from an LSP server is not a clean verdict")
        dirty = lsp_check.format_summary(
            {"files": 2, "errors": 2, "warnings": 1,
             "findings": ["a.py:7: undefined name 'foo'"]})
        self.assertIn("2 error(s)", dirty)
        self.assertIn("undefined name", dirty)


class TestWakeCarriesTheVerdict(unittest.TestCase):
    """The wake used to cost a mandatory pi_digest round-trip."""

    def test_clean_outcome_tells_the_orchestrator_not_to_fetch(self):
        msg = format_terminal_wake_message("pi-ok", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_PASS,
            "verifier_summary": "gate passed (exit 0)",
            "lsp_summary": "no LSP errors detected in 4 touched file(s)",
        })
        self.assertIn("gate passed (exit 0)", msg)
        self.assertIn("no LSP errors detected", msg)
        self.assertIn("complete outcome", msg)
        self.assertIn("continue the parent workflow autonomously", msg)

    def test_failing_gate_is_quoted_so_the_first_fact_is_free(self):
        msg = format_terminal_wake_message("pi-bad", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_FAIL,
            "verifier_summary": "gate failed (exit 1) — 3 tests failed",
            "lsp_summary": "LSP found 2 error(s) in 1 touched file(s) — a.py:7: bad",
        })
        self.assertIn("gate failed (exit 1)", msg)
        self.assertIn("a.py:7", msg)
        self.assertIn("Analyze the result", msg)
        self.assertNotIn("complete outcome", msg)

    def test_lsp_errors_defeat_a_green_gate(self):
        """A passing gate plus type errors is NOT a clean outcome."""
        msg = format_terminal_wake_message("pi-mixed", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_PASS,
            "verifier_summary": "gate passed (exit 0)",
            "lsp_summary": "LSP found 1 error(s) in 1 touched file(s) — a.py:2: nope",
        })
        self.assertNotIn("complete outcome", msg)
        self.assertIn("Analyze the result", msg)

    def test_missing_lsp_verdict_is_stated_not_implied(self):
        msg = format_terminal_wake_message("pi-nolsp", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_PASS,
            "verifier_summary": "gate passed (exit 0)",
            "lsp_summary": None,
        })
        self.assertIn("not available", msg,
                      "silence must not be readable as 'clean'")

    def test_stalled_task_never_reads_as_complete(self):
        msg = format_terminal_wake_message("pi-stall", {
            "execution_state": EXEC_STALLED,
            "verification_state": VERIFY_NOT_RUN,
        })
        self.assertNotIn("complete outcome", msg)

    def test_backward_compatible_with_rows_predating_the_columns(self):
        msg = format_terminal_wake_message("pi-old", {
            "execution_state": EXEC_SETTLED, "verification_state": VERIFY_PASS})
        self.assertIn("pi-old", msg)
        self.assertIn("continue the parent workflow autonomously", msg)


class TestVerifierVerdictCompaction(unittest.TestCase):
    def test_pass_is_one_short_line(self):
        self.assertEqual(_compact_verifier_verdict({"returncode": 0}),
                         "gate passed (exit 0)")

    def test_failure_keeps_the_last_stderr_line(self):
        out = _compact_verifier_verdict(
            {"returncode": 1, "stderr_tail": "noise\nFAIL: 3 tests failed"})
        self.assertIn("exit 1", out)
        self.assertIn("3 tests failed", out)

    def test_unrunnable_prefers_the_diagnosis(self):
        out = _compact_verifier_verdict(
            {"returncode": 127, "diagnosis": "the gate names something missing"})
        self.assertIn("127", out)
        self.assertIn("names something missing", out)

    def test_empty_summary_yields_nothing(self):
        self.assertIsNone(_compact_verifier_verdict({}))


if __name__ == "__main__":
    unittest.main()


class TestRegressionsFromStandaloneReview(unittest.TestCase):
    """Three false-confidence paths found reviewing the standalone repo."""

    def test_empty_diagnostics_from_an_unavailable_server_is_not_clean(self):
        """Upstream returns [] for "no server", "could not spawn" and
        "timed out" as readily as for a genuinely clean file. Counting that
        as checked would report "LSP clean" for a file nothing ever opened —
        worse than reporting nothing at all."""
        class Svc:
            def enabled_for(self, path): return True
            def get_diagnostics_sync(self, path, **kw): return []
            def get_status(self): return {"clients": []}      # nothing running
        real = lsp_check._service, lsp_check.collect_touched_files
        lsp_check._service = lambda: Svc()
        lsp_check.collect_touched_files = lambda cwd, **kw: ["/tmp/a.py"]
        self.addCleanup(lambda: (setattr(lsp_check, "_service", real[0]),
                                 setattr(lsp_check, "collect_touched_files", real[1])))
        self.assertIsNone(lsp_check.run("/tmp"),
                          "no live server means no verdict, not a clean one")

    def test_empty_diagnostics_from_a_live_server_is_clean(self):
        class Svc:
            def enabled_for(self, path): return True
            def get_diagnostics_sync(self, path, **kw): return []
            def get_status(self): return {"clients": [{"server_id": "pyright",
                                                       "running": True}]}
        real = lsp_check._service, lsp_check.collect_touched_files, lsp_check._server_id_for
        lsp_check._service = lambda: Svc()
        lsp_check.collect_touched_files = lambda cwd, **kw: ["/tmp/a.py"]
        lsp_check._server_id_for = lambda p: "pyright"
        self.addCleanup(lambda: (setattr(lsp_check, "_service", real[0]),
                                 setattr(lsp_check, "collect_touched_files", real[1]),
                                 setattr(lsp_check, "_server_id_for", real[2])))
        res = lsp_check.run("/tmp")
        self.assertIsNotNone(res)
        summary = lsp_check.format_summary(res)
        self.assertIn("no LSP errors detected", summary)
        self.assertNotIn("clean", summary,
                         "a live server that returned [] proves nothing: "
                         "upstream uses [] for 'no fresh diagnostics' too")

    def test_files_the_service_declines_are_reported_not_hidden(self):
        """'clean across 1 file' when two were touched reads as a verdict on
        both. The count of unchecked files must survive into the summary."""
        summary = lsp_check.format_summary(
            {"files": 1, "errors": 0, "warnings": 0, "unavailable": 3,
             "findings": []})
        self.assertIn("could not be checked", summary)

    def test_settled_without_a_gate_is_never_a_complete_outcome(self):
        """SETTLED + NOT_RUN means nothing checked the work. Announcing it as
        complete is the exact distinction _notify_done exists to preserve."""
        msg = format_terminal_wake_message("pi-ungated", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_NOT_RUN,
            "lsp_summary": None,
        })
        self.assertNotIn("complete outcome", msg)
        self.assertIn("Analyze the result", msg)

    def test_worker_policy_ships_with_the_plugin(self):
        """A fresh install has no ~/.pi; without a packaged policy Pi would
        run with no worker contract at all, silently."""
        import core  # type: ignore
        self.assertTrue(core.PACKAGED_SYSTEM_PROMPT_FILE.is_file(),
                        "pi-worker.md must be versioned with the plugin")
        self.assertEqual(core.DEFAULT_SYSTEM_PROMPT_FILE,
                         core.PACKAGED_SYSTEM_PROMPT_FILE)

    def test_a_live_but_slow_server_is_not_reported_as_proof(self):
        """Upstream's own path: the server is alive, produced no fresh
        diagnostics within the wait budget, returns [] and is deliberately
        NOT marked broken ("slow is not dead"). enabled_for() and
        get_status() both still say yes, so nothing can distinguish this
        from a genuinely clean file — the summary must not claim it is one.
        Common on tsserver over large projects, i.e. exactly the codebases
        where a missed type error costs the most."""
        class SlowButAlive:
            def enabled_for(self, path): return True
            def get_diagnostics_sync(self, path, **kw): return []   # no fresh data
            def get_status(self): return {"clients": [{"server_id": "typescript",
                                                       "running": True}]}
        real = (lsp_check._service, lsp_check.collect_touched_files,
                lsp_check._server_id_for)
        lsp_check._service = lambda: SlowButAlive()
        lsp_check.collect_touched_files = lambda cwd, **kw: ["/tmp/big.ts"]
        lsp_check._server_id_for = lambda p: "typescript"
        self.addCleanup(lambda: (setattr(lsp_check, "_service", real[0]),
                                 setattr(lsp_check, "collect_touched_files", real[1]),
                                 setattr(lsp_check, "_server_id_for", real[2])))
        summary = lsp_check.format_summary(lsp_check.run("/tmp"))
        self.assertIsNotNone(summary)
        self.assertNotIn("clean", summary)
        self.assertIn("no LSP errors detected", summary)

    def test_reported_errors_remain_a_blocking_signal(self):
        """Weakening the empty case must not weaken the useful one: an error
        the server DID report is trustworthy and must still defeat a green
        gate in the wake."""
        msg = format_terminal_wake_message("pi-x", {
            "execution_state": EXEC_SETTLED,
            "verification_state": VERIFY_PASS,
            "verifier_summary": "gate passed (exit 0)",
            "lsp_summary": "LSP found 1 error(s) in 1 touched file(s) — a.ts:3: nope",
        })
        self.assertNotIn("complete outcome", msg)
        self.assertIn("Analyze the result", msg)
