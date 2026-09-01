"""Focused unit tests for the user-facing notification text.

These are the exact strings delivered to Kamil in Telegram, so they are
tested directly, off the full manager machinery: Polish, concise, factual
(task id, step count, elapsed time, verdict, exit code, last error) and free
of raw orchestrator meta, internal tool-call instructions, and internal
paths.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLUGIN_DIR))
sys.path.insert(0, str(PLUGIN_DIR / "tests"))

from core import (  # noqa: E402
    VERIFY_FAIL,
    VERIFY_NOT_RUN,
    VERIFY_PASS,
    VERIFY_UNRUNNABLE,
    _krok_word,
    format_failed_message,
    format_progress_message,
    format_stalled_message,
    format_notification_message,
)

TASK = "pi-16d3ff7c3f14"


class TestKrokPlural(unittest.TestCase):
    def test_polish_plurals(self):
        cases = {
            0: "kroków", 1: "krok", 2: "kroki", 4: "kroki",
            5: "kroków", 10: "kroków", 11: "kroków", 12: "kroków",
            13: "kroków", 14: "kroków", 21: "krok", 22: "kroki",
            55: "kroków", 121: "krok", 122: "kroki", 112: "kroków",
        }
        for count, word in cases.items():
            self.assertEqual(_krok_word(count), word, f"count={count}")


class TestProgressFormatting(unittest.TestCase):
    def test_shape_with_activity(self):
        msg = format_progress_message(TASK, {
            "execution_state": "RUNNING",
            "message_count": 55,
            "active_tool": "bash",
        })
        self.assertEqual(
            msg,
            "Pi pracuje nad zadaniem `pi-16d3ff7c3f14` — wykonano 55 "
            "kroków. Ostatnia aktywność: bash. Task nadal działa.",
        )

    def test_activity_clause_omitted_when_unavailable(self):
        msg = format_progress_message(TASK, {"message_count": 3})
        self.assertEqual(
            msg,
            "Pi pracuje nad zadaniem `pi-16d3ff7c3f14` — wykonano 3 kroki. "
            "Task nadal działa.",
        )
        self.assertNotIn("Ostatnia aktywność", msg)

    def test_zero_count_and_singular(self):
        msg = format_progress_message(TASK, {"message_count": 1})
        self.assertIn("wykonano 1 krok.", msg)

    def test_no_raw_orchestrator_meta(self):
        msg = format_progress_message(TASK, {
            "execution_state": "RUNNING", "message_count": 55,
        })
        self.assertNotIn("still working", msg)
        self.assertNotIn("RUNNING", msg)
        self.assertNotIn("I will only message", msg)
        self.assertNotIn("pi_status", msg)

    def test_long_tool_name_is_bounded(self):
        msg = format_progress_message(TASK, {
            "message_count": 2,
            "active_tool": "bash" * 500,  # 2000 chars
        })
        self.assertLessEqual(len(msg), 400)


class TestCompletionFormatting(unittest.TestCase):
    def _row(self, **over):
        row = {
            "execution_state": "SETTLED",
            "message_count": 42,
            "started_at": 100.0,
            "settled_at": 250.0,
        }
        row.update(over)
        return row

    def test_pass_verdict_and_elapsed_time(self):
        msg = format_notification_message("verifier", TASK,
                                          self._row(verification_state=VERIFY_PASS))
        self.assertTrue(msg.startswith(
            f"Pi zakończył zadanie `{TASK}`. Weryfikacja: PASS."))
        self.assertIn("Wykonano 42 kroki.", msg)
        self.assertIn("Czas: 150 s.", msg)

    def test_fail_verdict_requires_review(self):
        msg = format_notification_message("verifier", TASK,
                                          self._row(verification_state=VERIFY_FAIL))
        self.assertIn("Weryfikacja: FAIL", msg)
        self.assertIn("wymagają przeglądu", msg)
        self.assertNotIn("Weryfikacja: PASS", msg)

    def test_unrunnable_verdict_explicitly_no_verdict(self):
        msg = format_notification_message("verifier", TASK,
                                          self._row(verification_state=VERIFY_UNRUNNABLE))
        self.assertIn("Weryfikacja: UNRUNNABLE", msg)
        self.assertIn("nie wydano werdyktu", msg)
        self.assertIn("to nie jest FAIL", msg)

    def test_ungated_says_nothing_checked(self):
        msg = format_notification_message("settled", TASK,
                                          self._row(verification_state=VERIFY_NOT_RUN))
        self.assertTrue(msg.startswith(f"Pi zakończył zadanie `{TASK}`."))
        self.assertIn("Weryfikacja: nie uruchomiono (brak weryfikatora)", msg)
        self.assertIn("wynik nie został sprawdzony", msg)

    def test_missing_timestamps_omit_time_clause(self):
        msg = format_notification_message("settled", TASK,
                                          self._row(started_at=None, settled_at=None))
        self.assertNotIn("Czas:", msg)

    def test_missing_message_count_omits_step_clause(self):
        msg = format_notification_message("settled", TASK,
                                          self._row(message_count=0))
        self.assertNotIn("Wykonano", msg)

    def test_last_error_is_preserved_and_bounded(self):
        msg = format_notification_message("settled", TASK,
                                          self._row(last_error="x" * 10_000))
        self.assertIn("Ostatni błąd:", msg)
        self.assertLessEqual(len(msg), 500)

    def test_no_internal_paths_or_boilerplate(self):
        row = self._row(verification_state=VERIFY_PASS)
        row["cwd"] = "/home/someone/workspace/repos/secret-project"
        msg = format_notification_message("verifier", TASK, row)
        self.assertNotIn("/home/someone", msg)
        self.assertNotIn("pi_digest", msg)
        self.assertNotIn("pi_status", msg)
        self.assertNotIn("Settlement is not acceptance", msg)
        self.assertNotIn("SETTLED", msg)


class TestFailedFormatting(unittest.TestCase):
    def test_with_exit_code_and_detail(self):
        msg = format_failed_message(TASK, {
            "execution_state": "CRASHED",
            "exit_code": 7,
            "last_error": "connection reset",
        })
        self.assertEqual(
            msg,
            "Zadanie `pi-16d3ff7c3f14` zakończyło się z błędem: "
            "stan CRASHED, kod wyjścia 7. connection reset "
            "Wyniki wymagają przeglądu.",
        )

    def test_without_exit_code_or_detail(self):
        msg = format_failed_message(TASK, {"execution_state": "ABORTED"})
        self.assertIn("ABORTED", msg)
        self.assertNotIn("kod wyjścia", msg)
        self.assertIn("Wyniki wymagają przeglądu", msg)

    def test_no_tool_call_instructions(self):
        msg = format_failed_message(TASK, {"execution_state": "CRASHED"})
        self.assertNotIn("pi_status", msg)
        self.assertNotIn("pi_resume", msg)
        self.assertNotIn("pi_abort", msg)


class TestStalledFormatting(unittest.TestCase):
    def test_with_active_tool(self):
        msg = format_stalled_message(TASK, {"active_tool": "bash"})
        self.assertIn("STALLED", msg)
        self.assertIn("przy aktywnym narzędziu bash", msg)

    def test_without_active_tool(self):
        msg = format_stalled_message(TASK, {"last_progress_at": 123.0})
        self.assertIn("STALLED", msg)
        self.assertNotIn("przy aktywnym narzędziu", msg)

    def test_no_tool_call_instructions(self):
        msg = format_stalled_message(TASK, {})
        self.assertNotIn("pi_status", msg)
        self.assertNotIn("pi_steer", msg)


if __name__ == "__main__":
    unittest.main()