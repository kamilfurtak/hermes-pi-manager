"""Plugin-owned terminal continuation wake worker.

Delivers the ONE durable terminal wake per task (see ``core.py`` module
docstring and the ``registry_db`` terminal-wake methods). The worker holds
a fresh ``PluginContext`` — taken from ``register(ctx)`` on each plugin
load, kept ONLY as worker integration state, never in the PiManager core or
on any task — and delivers by calling the public API::

    ctx.inject_message(message, session_key=origin.session_key)

exactly as ``hermes_cli.plugins.PluginContext.inject_message`` declares
(``content, role='user', *, session_key=None -> bool``). A ``True`` return
means the live gateway accepted the request for asynchronous dispatch and
the wake row becomes ``accepted`` (terminal). ``False`` or a raised
exception means the dispatch did not happen (no live gateway, missing
session key, no ``plugins.entries.<plugin>.allow_gateway_injection`` grant,
scheduling failure): the row goes back to ``pending`` on the bounded
backoff, and after the budget is spent it becomes ``exhausted``.

Crash safety: the only state that is NOT automatically retried is a
``dispatching`` row found at a fresh plugin (re)load — the previous process
claimed it and died before recording the outcome, so the gateway may have
ALREADY accepted the injection. Such a row is marked ``uncertain`` (audit
only) and never dispatched again: a duplicate orchestrator turn is the
failure this exists to prevent.

This is the ONLY ``inject_message`` path in the plugin. Progress notices
never inject, and the Telegram notification outbox keeps its existing
delivery exactly (``host_adapter`` -> native ``send_message_tool`` ->
gateway, zero agent turns). No completion queue, no event bus, no custom
Telegram API — the explicit user-approved terminal wake is the only
exception to the plugin's blanket "no injection" rule.

Stdlib + registry only; the PluginContext is passed in, never imported, so
the module is unit-testable with a fake context and no Hermes runtime.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from .core import continuation_id_for, format_terminal_wake_message  # type: ignore
    from .outbox import parse_origin  # type: ignore
    from .registry_db import Registry  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    from core import continuation_id_for, format_terminal_wake_message  # type: ignore
    from outbox import parse_origin  # type: ignore
    from registry_db import Registry  # type: ignore

logger = logging.getLogger(__name__)

# -- wake states (mirror the registry_db state machine) --------------------

WAKE_PENDING = "pending"
WAKE_DISPATCHING = "dispatching"
WAKE_ACCEPTED = "accepted"
WAKE_DISABLED = "disabled"
WAKE_UNCERTAIN = "uncertain"
WAKE_EXHAUSTED = "exhausted"

# -- policy ------------------------------------------------------------------

# How often the worker looks for due pending wakes. Wakes are rare (one per
# task, at terminal state) so a 15 s cadence is comfortably responsive.
DEFAULT_WAKE_INTERVAL_SECONDS = 15.0
# Bounded dispatch attempts per task (claims, so including the first try).
DEFAULT_MAX_ATTEMPTS = 5
# Backoff applied after attempts 1..4; the 5th failure exhausts the budget.
DEFAULT_RETRY_BACKOFF_SECONDS = (30.0, 120.0, 600.0, 1800.0)
# How many due wakes one tick may claim (wakes are rare; this is a sanity cap).
DEFAULT_MAX_PER_TICK = 8


class TerminalWakeWorker:
    """Drains the per-task terminal wake state machine through
    ``PluginContext.inject_message``.

    ``start()`` returns immediately (daemon thread); ``stop()`` signals the
    loop and joins with a bounded timeout — same lifecycle shape as
    ``OutboxWorker``. A fresh plugin load replaces the worker (the old one
    is stopped BEFORE the new one starts, so a live in-flight dispatch can
    never be marked ``uncertain`` by its own replacement). Tests drive
    ``run_once`` directly with an explicit ``now``.
    """

    def __init__(
        self,
        registry: Registry,
        ctx: Any,
        now_fn: Callable[[], float] = time.time,
        interval_seconds: float = DEFAULT_WAKE_INTERVAL_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_backoff_seconds: Optional[tuple] = None,
        max_per_tick: int = DEFAULT_MAX_PER_TICK,
        worker_id: Optional[str] = None,
    ) -> None:
        self.registry = registry
        # Fresh PluginContext of the CURRENT registration — worker
        # integration state only. Never referenced from the PiManager core,
        # never persisted, replaced (and the old worker stopped) on reload.
        self._ctx = ctx
        self._now_fn = now_fn
        self.interval = float(interval_seconds)
        self.max_attempts = int(max_attempts)
        self.retry_backoff = tuple(retry_backoff_seconds or DEFAULT_RETRY_BACKOFF_SECONDS)
        self.max_per_tick = int(max_per_tick)
        self.worker_id = worker_id or f"pi-wake-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        thread = threading.Thread(
            target=self._loop, name=f"pi-wake-{self.worker_id}", daemon=True,
        )
        self._thread = thread
        thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the loop and join with a bounded timeout. A dispatch in
        flight past the timeout is a daemon thread finishing on its own; the
        row's state machine (dispatching -> uncertain on next load) keeps it
        from being dispatched twice."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        self._thread = None

    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:  # a broken tick must never kill the worker
                logger.warning("pi-wake worker tick failed: %s", exc)
            self._stop.wait(self.interval)

    # -- dispatch -------------------------------------------------------------

    def run_once(self, now: Optional[float] = None) -> int:
        """One wake pass: recover stale ``dispatching`` rows (restart
        safety — they become ``uncertain`` and are never retried), claim
        due ``pending`` wakes, dispatch each. Returns the number of rows
        claimed. Public so tests drive it deterministically."""
        now = now if now is not None else self._now_fn()
        try:
            stale = self.registry.settle_stale_wake_dispatching(now)
            for task_id in stale:
                self._event(task_id, "wake_uncertain", {
                    "note": "dispatching at plugin load; not retried to avoid "
                            "a duplicate orchestrator turn",
                })
        except Exception as exc:
            logger.warning("pi-wake: stale-dispatching recovery failed: %s", exc)
        claimed = 0
        for row in self.registry.list_wake_pending(now, limit=self.max_per_tick):
            if not self.registry.claim_terminal_wake(row["task_id"], now):
                continue  # lost the CAS race; the other claimant owns it now
            claimed += 1
            self._dispatch_one(row, now)
        return claimed

    def _dispatch_one(self, row: Dict[str, Any], now: float) -> None:
        task_id = row["task_id"]
        fresh = self.registry.get_task(task_id) or row
        origin = parse_origin(fresh.get("origin"))
        # Exact routing: the persisted dispatch-turn snapshot's session_key,
        # verbatim. The wake lands in the ORIGINAL orchestrator session —
        # never a target-built chat message.
        session_key = origin.get("session_key") or None
        message = format_terminal_wake_message(task_id, fresh)
        error: Optional[str] = None
        try:
            accepted = bool(self._ctx.inject_message(message, session_key=session_key))
        except Exception as exc:
            accepted = False
            error = f"inject_message raised: {exc}"
        if accepted:
            # True = the live gateway accepted the request for asynchronous
            # dispatch. Terminal: record it and never touch this wake again.
            self.registry.mark_wake_accepted(task_id, now)
            self._event(task_id, "wake_accepted", {
                "continuation_id": continuation_id_for(task_id),
                "execution_state": fresh.get("execution_state"),
                "verification_state": fresh.get("verification_state"),
            })
            return
        if error is None:
            error = ("inject_message returned False: gateway did not accept "
                     "(no live gateway, missing session_key, or the "
                     "allow_gateway_injection grant is not set)")
        # The claim already incremented wake_attempts; this is attempt N.
        attempts = int(fresh.get("wake_attempts") or 0)
        if attempts < self.max_attempts:
            delay = self.retry_backoff[min(attempts - 1, len(self.retry_backoff) - 1)]
            self.registry.mark_wake_retry(task_id, now + delay, error, now)
            self._event(task_id, "wake_retry", {
                "attempts": attempts, "next_retry_at": now + delay, "error": error,
            })
        else:
            self.registry.mark_wake_exhausted(task_id, error, now)
            self._event(task_id, "wake_exhausted", {
                "attempts": attempts, "error": error,
            })

    def _event(self, task_id: str, event_type: str, summary: Dict[str, Any]) -> None:
        try:
            self.registry.append_event(task_id, "wake", event_type, None, None, summary)
        except Exception:  # the audit note must never disturb the dispatch
            pass