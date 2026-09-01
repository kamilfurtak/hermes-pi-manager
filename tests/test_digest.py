"""A supervisor must be able to ask what a delegate did without reading it.

status() reports the state machine and says nothing about the work, so the
only way to see what a delegate actually did was to read its session
transcript. Those run from 93 KB to 1.38 MB on this machine; reading one back
costs the orchestrator far more context than the delegation ever saved, and on
2026-08-28 08:55 the agent tried exactly that.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PLUGIN_DIR))
sys.path.insert(0, str(PLUGIN_DIR / "tests"))

from test_pi_manager import PiManagerTestCase  # type: ignore
from core import PiManager, DIGEST_FINAL_CHARS, DIGEST_PROMPT_CHARS  # type: ignore


def _msg(role: str, *parts: dict) -> str:
    return json.dumps({"type": "message", "id": "x", "parentId": None,
                       "timestamp": 0, "message": {"role": role,
                                                   "content": list(parts)}})


def _text(s: str) -> dict:
    return {"type": "text", "text": s}


def _call(name: str, args: str = "{}") -> dict:
    return {"type": "toolCall", "id": "c", "name": name, "arguments": args}


class DigestTestCase(PiManagerTestCase):
    def setUp(self):
        super().setUp()
        self.manager = PiManager(registry=self.registry, clock=self.clock)

    def make_task(self, *lines: str, task_id: str = "t1") -> str:
        session = self.tmp / f"{task_id}.jsonl"
        session.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.registry.create_task(task_id, session_file=str(session),
                                  cwd=str(self.cwd_dir))
        self.registry.update_task(task_id, execution_state="SETTLED",
                                  verification_state="PASS")
        return task_id


class TestDigestReportsTheWork(DigestTestCase):
    def test_the_delegates_closing_words_are_the_summary(self):
        tid = self.make_task(
            _msg("user", _text("do the thing")),
            _msg("assistant", _text("starting"), _call("bash")),
            _msg("toolResult", _text("output")),
            _msg("assistant", _text("Done: changed 2 files.")),
        )
        d = self.manager.digest(tid)
        self.assertEqual(d["summary"], "Done: changed 2 files.")
        self.assertEqual(d["prompt"], "do the thing")
        self.assertEqual(d["state"], "DONE")

    def test_tool_use_is_counted(self):
        tid = self.make_task(
            _msg("user", _text("go")),
            _msg("assistant", _call("bash"), _call("bash"), _call("edit")),
            _msg("assistant", _text("finished")),
        )
        d = self.manager.digest(tid)
        self.assertEqual(d["tool_counts"], {"bash": 2, "edit": 1})
        self.assertEqual(d["tool_calls_total"], 3)
        self.assertEqual(d["assistant_turns"], 2)

    def test_thinking_is_never_returned(self):
        """Scratch reasoning is the bulkiest thing in a transcript and is not
        what a supervisor asked for."""
        secret = "internal deliberation that must not travel"
        tid = self.make_task(
            _msg("user", _text("go")),
            _msg("assistant", {"type": "thinking", "text": secret},
                 _text("all done")),
        )
        d = self.manager.digest(tid)
        self.assertEqual(d["summary"], "all done")
        self.assertNotIn(secret, json.dumps(d))

    def test_an_interrupted_delegate_still_reports_its_last_words(self):
        # An aborted delegate never writes a closing summary; its last
        # utterance plus the counts is what a supervisor gets, and that is
        # honest rather than empty.
        tid = self.make_task(
            _msg("user", _text("go")),
            _msg("assistant", _text("Checking the bundle:"), _call("bash")),
        )
        self.registry.update_task(tid, execution_state="ABORTED")
        d = self.manager.digest(tid)
        self.assertEqual(d["summary"], "Checking the bundle:")
        self.assertEqual(d["execution_state"], "ABORTED")
        self.assertEqual(d["tool_calls_total"], 1)


class TestDigestStaysSmall(DigestTestCase):
    def test_every_field_is_bounded(self):
        tid = self.make_task(
            _msg("user", _text("p" * 5000)),
            *[_msg("assistant", _text("s" * 5000), _call("bash"))
              for _ in range(300)],
        )
        d = self.manager.digest(tid)
        self.assertLessEqual(len(d["prompt"]), DIGEST_PROMPT_CHARS + 60)
        self.assertLessEqual(len(d["summary"]), DIGEST_FINAL_CHARS + 60)
        self.assertEqual(d["tool_calls_total"], 300)

    def test_a_huge_transcript_still_digests_small(self):
        tid = self.make_task(
            _msg("user", _text("go")),
            *[_msg("assistant", _text("x" * 2000), _call("bash"))
              for _ in range(500)],
        )
        transcript = (self.tmp / "t1.jsonl").stat().st_size
        rendered = len(json.dumps(self.manager.digest(tid)))
        self.assertGreater(transcript, 500_000, "precondition: a big session")
        self.assertLess(rendered, 4000,
                        "the digest must never scale with the transcript")


class TestDigestFailsSoftly(DigestTestCase):
    def test_a_missing_session_file_is_reported_not_raised(self):
        self.registry.create_task("t9", session_file=str(self.tmp / "gone.jsonl"))
        d = self.manager.digest("t9")
        self.assertIn("digest_error", d)
        self.assertEqual(d["task_id"], "t9")

    def test_a_task_with_no_session_file_is_reported_not_raised(self):
        self.registry.create_task("t8")
        self.assertIn("digest_error", self.manager.digest("t8"))

    def test_malformed_lines_are_skipped(self):
        # A half-written trailing line is normal for a live transcript.
        tid = self.make_task(
            _msg("user", _text("go")),
            "{not json at all",
            _msg("assistant", _text("done anyway")),
            '{"type": "message", "message": {"role": "assistant", "content"',
        )
        self.assertEqual(self.manager.digest(tid)["summary"], "done anyway")

    def test_unknown_task_raises_keyerror(self):
        with self.assertRaises(KeyError):
            self.manager.digest("never-existed")


if __name__ == "__main__":
    unittest.main()
