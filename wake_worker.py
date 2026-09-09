"""One durable terminal continuation per Pi task, after verification.

CLI and gateway retain PluginContext.inject_message. CLI delivery is bound to
origin.host_runtime_id; gateway delivery requires a live gateway injector.
Desktop/TUI uses desktop_host only in the backend holding the matching session.
It waits behind the running turn and human FIFO; missing/busy owners spend no
retry budget. There is no Bot Chat redirect or fallback to an unrelated CLI.

An accepted wake is never replayed. Gateway acceptance only proves scheduling;
Desktop also records wake_turn_finished from the native terminal callback.
A dispatch whose process died, or whose Desktop admission raised after a possible
start, becomes uncertain: inspect evidence manually instead of duplicating a turn.
Live dispatchers are preserved across other workers' recovery passes.

Progress notices use the separate passive outbox and never start a model turn.
The worker owns only its current PluginContext, not a second session or queue.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from . import desktop_host, cli_host
    from .core import (  # type: ignore
        HOST_RUNTIME_ID, continuation_id_for, format_terminal_wake_message,
    )
    from .outbox import parse_origin  # type: ignore
    from .registry_db import Registry  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    import desktop_host
    import cli_host
    from core import (  # type: ignore
        HOST_RUNTIME_ID, continuation_id_for, format_terminal_wake_message,
    )
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
# How long a LOCAL wake (no session_key: deliverable only by the process
# that dispatched it) may sit unclaimed before any other worker retires it.
# The owning process ticks every DEFAULT_WAKE_INTERVAL_SECONDS, so a live
# owner always claims its own wake orders of magnitude sooner than this;
# reaching the deadline means the owner died with the wake undelivered.
# Without it such a row would stay 'pending' forever — never claimed by its
# dead owner, never claimable by anyone else.
DEFAULT_LOCAL_WAKE_ORPHAN_SECONDS = 1800.0


class TerminalWakeWorker:
    """Drains terminal wakes through the matching native host adapter.

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
        local_wake_orphan_seconds: float = DEFAULT_LOCAL_WAKE_ORPHAN_SECONDS,
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
        self.local_wake_orphan_seconds = float(local_wake_orphan_seconds)
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
        row's state machine (dispatching while alive; uncertain after death) keeps it
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
                    "note": "dispatch owner died or is legacy/unknown; not retried to avoid "
                            "a duplicate orchestrator turn",
                })
        except Exception as exc:
            logger.warning("pi-wake: stale-dispatching recovery failed: %s", exc)
        claimed = 0
        # Apply the per-tick cap to eligible rows, not the first rows in the
        # shared DB. Closed Desktop chats must not starve a live Telegram/CLI.
        for row in self.registry.list_wake_pending(now, limit=None):
            if claimed >= self.max_per_tick:
                break
            if not self._deliverable_here(row, now):
                continue  # another process owns this one; leave the budget alone
            if not self.registry.claim_terminal_wake(row["task_id"], now):
                continue  # lost the CAS race; the other claimant owns it now
            claimed += 1
            self._dispatch_one(row, now)
        return claimed

    def _deliverable_here(self, row: Dict[str, Any], now: float) -> bool:
        """Whether THIS process is the one that can serve *row*'s wake.

        A gateway-routed wake (origin.session_key) is deliverable by any
        process holding the live gateway, so it stays first-come. A LOCAL
        wake has no session_key: ``inject_message`` will take its
        ``_cli_ref`` branch, which exists only inside the interactive CLI
        process that dispatched the task. Letting a foreign worker claim it
        would spend an attempt from the shared bounded budget on a delivery
        it cannot perform, and five such claims would exhaust the wake
        before the owning CLI ever got a tick.

        Returns False without touching the row when it belongs elsewhere —
        except for an orphan past its deadline, which is retired here
        because its owner is gone and nobody else may claim it.
        """
        origin = parse_origin(row.get("origin"))
        if desktop_host.is_desktop(origin):
            # Ordinary Desktop sessions are not gateway/CLI injection targets.
            # Busy sessions and foreign processes leave the retry budget alone.
            return desktop_host.available(origin, idle=True)
        if origin.get("session_key"):
            # Gateway wake. Any process holding a LIVE gateway may serve it
            # (the session store is durable, so a restarted gateway serves an
            # older wake fine) — but a process without one must keep its hands
            # off. Two distinct failures come from letting it through:
            #
            #   - an ACP/desktop backend or the web UI has neither a gateway
            #     nor a CLI reference, so its inject_message returns False and
            #     spends an attempt from the shared bounded budget;
            #   - a CLI process is WORSE than useless: inject_message checks
            #     _cli_ref FIRST and returns True without ever looking at
            #     session_key, so the wake is reported accepted (terminal, no
            #     retry) after landing in a terminal that never asked for it.
            #     Observed 2026-09-02: a desktop task's wake was answered in
            #     an unrelated CLI while the desktop session sat silent.
            #
            # When the host cannot be probed, fall through to the historical
            # first-come behaviour: unknown must never be less capable than
            # the code this replaces.
            return self._gateway_injector_live() is not False
        owner = origin.get("host_runtime_id")
        if owner == HOST_RUNTIME_ID:
            return cli_host.can_wake(origin) if cli_host.is_cli(origin) else True
        task_id = row["task_id"]
        requested_at = row.get("wake_requested_at")
        age = (now - float(requested_at)) if requested_at is not None else 0.0
        if age > self.local_wake_orphan_seconds:
            # Retire it through the normal CAS so exactly one worker wins the
            # race and the row passes through 'dispatching', which is the
            # only state mark_wake_exhausted accepts. No injection happens on
            # this path — the claim buys the right to close the row, not to
            # deliver it.
            if self.registry.claim_terminal_wake(task_id, now):
                error = (
                    "local wake orphaned: it is deliverable only inside the "
                    f"process that dispatched it (host_runtime_id={owner!r}), "
                    f"and that process has not claimed it in {age:.0f}s"
                )
                self.registry.mark_wake_exhausted(task_id, error, now)
                self._event(task_id, "wake_orphaned", {
                    "host_runtime_id": owner, "age_seconds": round(age, 1),
                })
        return False

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
        if desktop_host.is_desktop(origin):
            try:
                outcome = desktop_host.deliver_wake(
                    origin, message, continuation_id_for(task_id), self._ctx,
                    lambda receipt: self._event(task_id, "wake_turn_finished", {
                        "status": str(receipt.get("status") or "unknown"),
                        "error": str(receipt.get("error") or "")[:500],
                        "surface": "desktop", "continuation_id": continuation_id_for(task_id),
                    }))
            except desktop_host.UncertainDelivery as exc:
                self.registry.mark_wake_uncertain(task_id, str(exc), now)
                self._event(task_id, "wake_uncertain", {"error": str(exc), "surface": "desktop"})
                return
            if outcome in ("busy", "unavailable"):
                self.registry.defer_terminal_wake(task_id, now + self.interval)
                return
            accepted = outcome == "accepted"
            error = None if accepted else "Desktop wake denied: allow_gateway_injection grant is required"
        elif cli_host.is_cli(origin):
            try:
                outcome = cli_host.deliver_wake(origin, message)
            except Exception as exc:
                self.registry.mark_wake_uncertain(task_id, str(exc), now)
                self._event(task_id, 'wake_uncertain', {'surface': 'cli', 'error': str(exc)})
                return
            if outcome in ('busy', 'unavailable'):
                self.registry.defer_terminal_wake(task_id, now + self.interval)
                return
            accepted = outcome == 'accepted'
        else:
            try:
                accepted = bool(self._ctx.inject_message(message, session_key=session_key))
            except Exception as exc:
                accepted = False
                error = f"inject_message raised: {exc}"
        if accepted:
            # Accepted means the matching host admitted/scheduled the wake,
            # not that its model turn finished. Never replay this wake.
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

    def _gateway_injector_live(self) -> Optional[bool]:
        """Whether THIS process holds a live gateway injector.

        ``PluginManager.has_gateway_message_injector`` is a public property
        and is exactly the flag ``inject_message`` itself consults before
        taking its gateway branch; the manager is reached through the
        context's private handle because the plugin API exposes no surface
        accessor. Read-only and fail-soft on purpose: any breakage in that
        traversal returns None ("cannot tell"), which the caller treats as
        the historical permissive behaviour rather than as a refusal, so a
        host that renames or hides the attribute loses the optimisation
        instead of losing its wakes.
        """
        manager = getattr(self._ctx, "_manager", None)
        if manager is None:
            return None
        try:
            return bool(manager.has_gateway_message_injector)
        except Exception:
            return None

    def _event(self, task_id: str, event_type: str, summary: Dict[str, Any]) -> None:
        # Every wake event names the process that produced it. The registry
        # is shared by all Hermes processes and each runs its own worker, so
        # "which host attempted this wake" is the first question any wake
        # postmortem asks — and without this stamp it is unanswerable: the
        # interactive CLI suppresses the plugin logger's warnings, so a
        # failed dispatch leaves no trace in any log file.
        summary = dict(summary, host_runtime_id=HOST_RUNTIME_ID)
        try:
            self.registry.append_event(task_id, "wake", event_type, None, None, summary)
        except Exception:  # the audit note must never disturb the dispatch
            pass
