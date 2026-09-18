"""Immediate (per-write) LSP diagnostics for a live Pi task, via steer.

Pi ships no LSP and the settle-time check in ``core`` reports only after the
task ends. This pump tails the task's session file, watches for write-class
tool calls, and after a quiet window runs ``lsp_check.run(cwd)`` in the host
process. When NEW errors appear, it pushes a compact summary into the task
through the transport's steer channel, so the worker can fix them while the
work is still fresh.

All cost is host-side: in-process LSP service, zero agent turns, zero tokens.
The steer text is capped and the pump is budget-limited per task, so a noisy
LSP cannot turn into a steer storm. Every check failure degrades to "no
verdict" and never raises into the task lifecycle.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from . import lsp_check  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    import lsp_check  # type: ignore

logger = logging.getLogger(__name__)

# Tools that modify files. Read-only tools are deliberately excluded: a read
# never changes the code, so diagnosing after it would only repeat itself.
WRITE_TOOL_NAMES = {
    "edit", "write", "write_file", "patch", "apply_patch", "replace_in_file",
    "create", "update",
}

DEFAULT_QUIET_SECONDS = 4.0
DEFAULT_MAX_REPORTS = 4
DEFAULT_CHECK_BUDGET = 15.0
DEFAULT_POLL_SECONDS = 1.0
GIVE_UP_EMPTY_SECONDS = 12.0
MAX_STEER_CHARS = 600
MAX_FINDINGS_PER_STEER = 5


class LspFeedbackPump:
    """Tail one task's session file; steer fresh LSP errors back into it.

    ``steer(text)`` is called from the pump thread; it must be safe to call
    concurrently and may no-op (e.g. transport not up yet). ``record(summary)``
    persists an audit event. Both may raise; the pump swallows everything.
    """

    def __init__(
        self,
        task_id: str,
        cwd: str,
        session_file: str,
        steer: Callable[[str], None],
        record: Callable[[Dict[str, Any]], None],
        *,
        quiet_seconds: float = DEFAULT_QUIET_SECONDS,
        max_reports: int = DEFAULT_MAX_REPORTS,
        check_budget: float = DEFAULT_CHECK_BUDGET,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        empty_give_up_seconds: float = GIVE_UP_EMPTY_SECONDS,
        clock: Any = None,
    ) -> None:
        self._task_id = task_id
        self._cwd = cwd
        self._session_file = session_file
        self._steer = steer
        self._record = record
        self._quiet_seconds = quiet_seconds
        self._max_reports = max_reports
        self._check_budget = check_budget
        self._poll_seconds = poll_seconds
        self._empty_give_up_seconds = empty_give_up_seconds
        self._clock = clock or time.monotonic
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._offset = 0
        self._last_write_at: Optional[float] = None
        self._empty_since: Optional[float] = None
        self._reported: Set[str] = set()
        self._reports = 0

    # -- lifecycle -----------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop,
            name=f"pi-lsp-feedback-{self._task_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "pi-manager: lsp feedback pump active (task=%s, session=%s)",
            self._task_id, self._session_file)

    def stop(self) -> None:
        self._stop.set()

    # -- internals -----------------------------------------------------

    def _loop(self) -> None:
        while not self._stop.wait(self._poll_seconds):
            try:
                self._scan_tail()
                if self._should_check():
                    self._check_now()
            except Exception:  # noqa: BLE001 - feedback must never break a task
                logger.debug("pi-manager: lsp feedback loop error", exc_info=True)

    def _scan_tail(self) -> None:
        path = self._session_file
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                handle.seek(self._offset)
                # readline() + tell() instead of `for line in handle`:
                # tell() inside for-iteration raises "telling position
                # disabled by next() call" (TextIOWrapper readahead).
                while True:
                    line = handle.readline()
                    if not line:
                        break
                    self._offset = handle.tell()
                    self._digest_line(line)
        except OSError:
            return

    def _digest_line(self, line: str) -> None:
        try:
            entry = json.loads(line)
        except ValueError:
            return  # half-written trailing line is normal
        if entry.get("type") != "message":
            return
        message = entry.get("message") or {}
        if message.get("role") != "assistant":
            return
        parts = message.get("content")
        if not isinstance(parts, list):
            return
        for part in parts:
            if not isinstance(part, dict) or part.get("type") != "toolCall":
                continue
            if str(part.get("name") or "") in WRITE_TOOL_NAMES:
                self._last_write_at = self._clock()
                self._empty_since = None  # new burst: restart the give-up clock

    def _should_check(self) -> bool:
        if self._last_write_at is None:
            return False
        if self._reports >= self._max_reports:
            return False
        return (self._clock() - self._last_write_at) >= self._quiet_seconds

    def _check_now(self) -> None:
        result = lsp_check.run(self._cwd, budget_seconds=self._check_budget)
        if not result:
            self._last_write_at = None  # no verdict; stay silent
            self._empty_since = None
            return
        findings: List[str] = result.get("findings") or []
        if not findings:
            # Newly created files race tsserver warm-up: a fresh file can
            # come back "clean" simply because diagnostics timed out. Keep
            # the write armed so the next poll retries, until the give-up
            # deadline passes. Still fully host-side, zero agent turns.
            now = self._clock()
            empty_since = self._empty_since
            if empty_since is None:
                empty_since = now
                self._empty_since = empty_since
            if now - empty_since >= self._empty_give_up_seconds:
                self._last_write_at = None
                self._empty_since = None
            return
        self._last_write_at = None
        self._empty_since = None
        fresh = [f for f in findings if f not in self._reported]
        if not fresh:
            return
        self._reported.update(findings)
        self._reports += 1
        text = self._format(fresh, result)
        logger.info(
            "pi-manager: lsp feedback steering %d error(s) into task=%s",
            len(fresh), self._task_id)
        try:
            self._steer(text)
        except Exception:  # noqa: BLE001
            logger.debug("pi-manager: lsp feedback steer failed", exc_info=True)
        try:
            self._record({
                "task_id": self._task_id,
                "report_index": self._reports,
                "errors": int(result.get("errors") or 0),
                "findings_shown": fresh[:MAX_FINDINGS_PER_STEER],
            })
        except Exception:  # noqa: BLE001
            logger.debug("pi-manager: lsp feedback record failed", exc_info=True)

    def _format(self, fresh: List[str], result: Dict[str, Any]) -> str:
        shown = "; ".join(fresh[:MAX_FINDINGS_PER_STEER])
        errors = int(result.get("errors") or 0)
        head = (
            f"LSP (live, host-side): {errors} error(s) in "
            f"{result.get('files', 0)} touched file(s)."
        )
        tail = (
            " Fix these before settling; you do not see LSP yourself, so also "
            "self-typecheck with `npx tsc --noEmit -p <touched>/tsconfig.spec.json`."
        )
        return (head + " " + shown + tail)[:MAX_STEER_CHARS]


__all__ = ["LspFeedbackPump", "WRITE_TOOL_NAMES"]
