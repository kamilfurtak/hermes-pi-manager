"""pi-manager tool schemas and handlers.

Registers seven tools through the Hermes plugin public API
(``ctx.register_tool(name, toolset, schema, handler, is_async=...)``,
verified against ``~/.hermes/hermes-agent/hermes_cli/plugins.py`` and the
``plugins/spotify`` bundled example):

- ``pi_task``   — start one Pi RPC task; returns immediately (STARTING).
  Safe to call twice back to back for two parallel tasks (ceiling: 2).
- ``pi_status`` — read current task status from the registry.
- ``pi_abort``  — kill switch (RPC abort -> SIGTERM -> SIGKILL).
- ``pi_steer``  — send a Pi ``steer`` command to a live task.
- ``pi_resume`` — explicit re-run of the recovery algorithm for one task.
- ``pi_digest`` — what a delegate DID, in ~2 KB, instead of its transcript.

A ``pi_verify`` tool is also exposed: verification is a separate step from
execution per the spec ("execution_state vs verification_state are
distinct"), and the CLI/tool contract needs an explicit way to trigger it
once a task has reached SETTLED, since not every task carries an
auto-verifier at start time.

There is deliberately NO blocking wait tool. ``pi_wait`` existed until
2026-08-28 and was removed: waiting is strictly worse than the completion
notice on both axes that matter. The notice fires from ``_run_verifier``'s
``finally``, i.e. AFTER the gate resolves, so it carries the real PASS/FAIL —
a wait returns at ``agent_settled``, when ``verification_state`` is still
PENDING and the answer the caller actually wants does not exist yet. And a
wait freezes its whole conversation for the duration (the default was 15
minutes) to learn less. Callers end their turn. Two things come back to
them: the passive Telegram completion notice (outbox -> host adapter), and,
for tasks dispatched from a gateway/Telegram session, a single terminal
wake of the SAME session via ``TerminalWakeWorker``
(``wake_worker.py`` -> ``ctx.inject_message``) asking the orchestrator to
analyze the result and continue the parent workflow.
"""

from __future__ import annotations

import inspect
import json
import os
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from .core import PiManager, Thresholds, VerifierSpec, EXEC_SETTLED  # type: ignore
    from .outbox import NotificationOutbox, OutboxWorker  # type: ignore
    from .registry_db import Registry, default_db_path  # type: ignore
    from .rpc_transport import find_pi_binary  # type: ignore
    from .wake_worker import TerminalWakeWorker  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    from core import PiManager, Thresholds, VerifierSpec, EXEC_SETTLED
    from outbox import NotificationOutbox, OutboxWorker
    from registry_db import Registry, default_db_path
    from rpc_transport import find_pi_binary
    from wake_worker import TerminalWakeWorker

logger = logging.getLogger(__name__)

TOOLSET = "pi-manager"

_manager: Optional[PiManager] = None
_manager_lock = threading.Lock()



# ---------------------------------------------------------------------------
# Notifications: the plugin's own durable outbox.
#
# The first attempt at completion notices injected them straight into the
# caller's session. That worked exactly once — for a single conversation
# with nothing else running — and then failed with "not routed" as soon as
# Hermes had two sessions open. The second attempt pushed them onto a
# private internal Hermes queue, disguised as a framework-internal
# delegation event. That rail is not a public plugin API: its shape is an
# internal contract that can change, its delivery is owned by whichever
# process drains it, and it carries a fiction (this plugin is not one of
# those framework-internal delegations). This stage replaces it with what
# the plugin already owns:
# a SQLite outbox row per notification, and a worker that delivers each one
# by calling the plugin-local host adapter (host_adapter.py) directly —
# the adapter imports and directly calls the Hermes-native
# tools.send_message_tool.send_message_tool. Core intentionally never
# registers send_message in the ToolRegistry (host-only invariant), so the
# plugin registers NO send_message entry at all; delivery reaches the
# native rail by importing and calling its implementation, not through
# the registry.
#
# The routing origin is captured on the dispatching turn (contextvars do not
# survive into the settle thread) and persisted WITH the task in the
# registry, so settlement, crash, restart and recovery all deliver to the
# conversation that asked for the work — with no in-memory dict standing
# between them.
# ---------------------------------------------------------------------------

# Terminal continuation wake worker (separate from the Telegram outbox).
# This worker is the ONE place in the plugin that holds a PluginContext and
# the ONE path that calls ctx.inject_message: the user-approved terminal
# wake of the original orchestrator session. The fresh ctx of each
# registration is passed in here and kept only inside the worker — never in
# the PiManager core, never on a task. On reload the old worker is stopped
# BEFORE the new one starts (the opposite of the outbox order on purpose):
# a fresh worker must never mark a LIVE in-flight dispatch 'uncertain'.

_wake_worker: Optional[TerminalWakeWorker] = None
_wake_worker_lock = threading.Lock()


def _replace_wake_worker(ctx: Any) -> None:
    global _wake_worker
    with _wake_worker_lock:
        old = _wake_worker
        _wake_worker = None
    if old is not None:
        try:
            old.stop()
        except Exception as exc:
            logger.warning("pi-manager: stopping previous wake worker failed: %s", exc)
    worker = TerminalWakeWorker(get_manager().registry, ctx)
    with _wake_worker_lock:
        _wake_worker = worker
    worker.start()


def _stop_wake_worker() -> None:
    global _wake_worker
    with _wake_worker_lock:
        worker, _wake_worker = _wake_worker, None
    if worker is not None:
        try:
            worker.stop()
        except Exception as exc:
            logger.warning("pi-manager: wake worker stop failed: %s", exc)


def reset_wake_worker_for_tests() -> None:
    """Test-only hook: stop and forget the wake worker."""
    _stop_wake_worker()


def _capture_routing() -> Dict[str, Any]:
    """Snapshot the dispatching turn's routing origin via the public
    gateway.session_context API.

    Best effort: empty values are omitted, and a CLI process legitimately
    yields almost nothing (its notifications then fail permanently with a
    readable reason instead of being dropped silently). Only serializable
    strings are stored — never a context or session object.
    """
    out: Dict[str, Any] = {}
    try:
        from gateway.session_context import get_session_env  # type: ignore
        for key, env in (
            ("platform", "HERMES_SESSION_PLATFORM"),
            ("chat_id", "HERMES_SESSION_CHAT_ID"),
            ("thread_id", "HERMES_SESSION_THREAD_ID"),
            ("message_id", "HERMES_SESSION_MESSAGE_ID"),
            ("session_key", "HERMES_SESSION_KEY"),
            ("scope_id", "HERMES_SESSION_SCOPE_ID"),
            ("user_id", "HERMES_SESSION_USER_ID"),
            ("user_name", "HERMES_SESSION_USER_NAME"),
            ("session_id", "HERMES_SESSION_ID"),
            ("ui_session_id", "HERMES_UI_SESSION_ID"),
        ):
            value = get_session_env(env, "")
            if value:
                out[key] = str(value)
    except Exception:
        pass
    if not out.get("platform"):
        # HERMES_PLATFORM is a process-level environment variable (not a
        # session contextvar); consult it as a second source only.
        try:
            value = os.environ.get("HERMES_PLATFORM", "")
            if value:
                out["platform"] = str(value)
        except Exception:
            pass
    return out


def get_manager() -> PiManager:
    """Lazily create the process-wide PiManager, bound to the effective
    Hermes home, with its durable notification outbox attached. Also runs
    recovery-on-init exactly once."""
    global _manager
    with _manager_lock:
        if _manager is None:
            registry = Registry(db_path=default_db_path())
            outbox = NotificationOutbox(registry)
            _manager = PiManager(registry=registry, outbox=outbox)
            try:
                _manager.recover_all()
            except Exception:
                pass
        return _manager


def reset_manager_for_tests() -> None:
    """Test-only hook: force a fresh manager on next get_manager() call."""
    global _manager
    with _manager_lock:
        _manager = None


# -- notification worker -----------------------------------------------------
#
# One worker per registration. It holds no task state of its own and holds
# no PluginContext: its only delivery dependency is the plugin-local host
# adapter (default of OutboxWorker.deliver), which imports and directly
# calls the native send_message_tool. A re-registration (Hermes restart /
# plugin reload) simply starts a new worker and stops the old one.
# Delivery never goes through a registry dispatch and never builds
# LLM/agent turns.

_worker: Optional[OutboxWorker] = None
_worker_lock = threading.Lock()


def _replace_worker() -> None:
    global _worker
    with _worker_lock:
        old = _worker
        worker = OutboxWorker(get_manager().outbox)
        _worker = worker
        worker.start()
    if old is not None:
        try:
            old.stop()
        except Exception as exc:
            logger.warning("pi-manager: stopping previous outbox worker failed: %s", exc)


def _stop_worker() -> None:
    global _worker
    with _worker_lock:
        worker, _worker = _worker, None
    if worker is not None:
        try:
            worker.stop()
        except Exception as exc:
            logger.warning("pi-manager: outbox worker stop failed: %s", exc)


def reset_worker_for_tests() -> None:
    """Test-only hook: stop and forget the worker."""
    _stop_worker()


def _result(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _error(message: str) -> str:
    return _result({"error": message})


# ---------------------------------------------------------------------------
# pi_task
# ---------------------------------------------------------------------------

PI_TASK_SCHEMA = {
    "name": "pi_task",
    "description": (
        "Start one bounded Pi RPC task in the background and return its "
        "task metadata immediately (does not wait for completion). The "
        "task reaches semantic completion only on the real Pi "
        "'agent_settled' event. "
        "END YOUR TURN WITHOUT POLLING: nothing is delivered mid-task "
        "(progress notices stay passive). When the task reaches a terminal "
        "state, AFTER the verifier has resolved (real PASS/FAIL/UNRUNNABLE, "
        "or explicitly 'no verifier ran'), two things happen. (1) A passive "
        "completion notice is delivered to this conversation via the durable "
        "outbox. (2) For tasks dispatched from a gateway/Telegram session — "
        "the default for such tasks — THIS session is resumed ONCE with a "
        "small wake message (task_id, execution_state, verification_state, "
        "continuation_id) asking you to analyze the result (pi_digest "
        "reports what the delegate did for about 2 KB instead of its "
        "93 KB-1.4 MB transcript) and continue the parent workflow. No wake "
        "is sent on manual abort. "
        "Each pi_status poll costs a full model call carrying your whole "
        "context (~152k tokens measured), so a poll loop buys nothing; use "
        "pi_status only for a one-off progress check. "
        "CONCURRENCY: tasks are independent — each gets its own process, "
        "session file and watchdog, and the manager imposes no "
        "serialization. Call pi_task again straight away to run a second "
        "task in parallel (a writer and an independent reviewer, two "
        "unrelated stages); do NOT wait for the first to settle unless the "
        "second genuinely depends on its output. Two concurrent tasks is the "
        "useful ceiling: the NInfer server runs --max-concurrency 2 and the "
        "gateway admits 2 in flight, so a third will queue behind them "
        "rather than run faster."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "prompt": {"type": "string", "description": "The prompt to send to Pi."},
            "cwd": {"type": "string", "description": "Working directory for the Pi process."},
            "task_id": {"type": "string", "description": "Optional explicit task id."},
            "verifier_argv": {
                "type": "array", "items": {"type": "string"},
                "description": "Verifier command (argv array, no shell) run automatically "
                               "once the task settles. PASS IT. agent_settled only means the "
                               "agent stopped; it is not evidence the work is correct, and "
                               "without this the task ends as verification_state=NOT_RUN and "
                               "you are left guessing from a transcript. Choose the gate the "
                               "change actually threatens (the failing lint job for a lint fix, "
                               "the test suite for a code change), not one that passes "
                               "regardless. Omit only for a genuinely read-only audit.",
            },
            "verifier_timeout_seconds": {"type": "number"},
            "emergency_cap_seconds": {
                "type": ["number", "null"],
                "description": "Optional operator safety backstop in seconds. "
                               "DISABLED BY DEFAULT (null) — nothing in this plugin "
                               "kills a task on elapsed time alone; a long task with "
                               "real progress must not be killed. NOT a task deadline "
                               "— a healthy task ends at agent_settled, an unhealthy "
                               "one at STALLED/CRASHED/UNRESPONSIVE, each through "
                               "evidence. Set it only as an explicit infrastructure "
                               "ceiling.",
            },
        },
        "required": ["prompt", "cwd"],
    },
}


def handle_pi_task(args: Dict[str, Any], **_kw) -> str:
    prompt = args.get("prompt")
    cwd = args.get("cwd")
    if not prompt or not cwd:
        return _error("prompt and cwd are required")
    cwd_path = Path(str(cwd)).expanduser()
    if not cwd_path.is_dir():
        return _error(f"cwd is not an existing directory: {cwd_path}")
    verifier = None
    verifier_argv = args.get("verifier_argv")
    if verifier_argv:
        verifier = VerifierSpec(
            argv=list(verifier_argv),
            timeout_seconds=float(args.get("verifier_timeout_seconds") or 120.0),
        )
    thresholds = Thresholds()
    if "emergency_cap_seconds" in args:
        cap = args["emergency_cap_seconds"]
        thresholds.emergency_cap_seconds = None if cap is None else float(cap)
    manager = get_manager()
    # Capture routing NOW, on the dispatching turn: contextvars do not reach
    # the settle thread, and the notifications need to name the conversation
    # that asked for the work. The snapshot is persisted with the task, so
    # it survives settlement, crash and manager restart.
    origin = _capture_routing()
    result = manager.start_task(
        prompt=str(prompt), cwd=str(cwd_path), task_id=args.get("task_id"),
        thresholds=thresholds, verifier=verifier, origin=origin or None,
    )
    return _result(result)


# ---------------------------------------------------------------------------
# pi_status
# ---------------------------------------------------------------------------

PI_STATUS_SCHEMA = {
    "name": "pi_status",
    "description": "Return the current status of a pi_task, including execution_state, "
                    "verification_state, and progress diagnostics.",
    "parameters": {
        "type": "object",
        "properties": {"task_id": {"type": "string"}},
        "required": ["task_id"],
    },
}


def handle_pi_status(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    if not task_id:
        return _error("task_id is required")
    manager = get_manager()
    try:
        return _result(manager.status(str(task_id)))
    except KeyError:
        return _error(f"unknown task_id: {task_id}")


# ---------------------------------------------------------------------------
# pi_digest
# ---------------------------------------------------------------------------

PI_DIGEST_SCHEMA = {
    "name": "pi_digest",
    "description": "What a pi_task actually DID, in about 2 KB: the delegate's own "
                    "closing summary, the prompt it was given, and a count of the "
                    "tools it used. Use this instead of reading the task's session "
                    "file — those run from 93 KB to nearly 1 MB, so reading one "
                    "costs far more context than the delegation saved. pi_status "
                    "answers 'is it done?'; pi_digest answers 'what happened?'.",
    "parameters": {
        "type": "object",
        "properties": {"task_id": {"type": "string"}},
        "required": ["task_id"],
    },
}


def handle_pi_digest(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    if not task_id:
        return _error("task_id is required")
    manager = get_manager()
    try:
        return _result(manager.digest(str(task_id)))
    except KeyError:
        return _error(f"unknown task_id: {task_id}")


# ---------------------------------------------------------------------------
# pi_abort
# ---------------------------------------------------------------------------

PI_ABORT_SCHEMA = {
    "name": "pi_abort",
    "description": "Abort a running pi_task: RPC abort, then SIGTERM, then SIGKILL after a "
                    "grace period. Always records ABORTED, never success.",
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "reason": {"type": "string"},
        },
        "required": ["task_id"],
    },
}


def handle_pi_abort(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    if not task_id:
        return _error("task_id is required")
    manager = get_manager()
    try:
        return _result(manager.abort_task(str(task_id), reason=str(args.get("reason") or "manual_abort")))
    except KeyError:
        return _error(f"unknown task_id: {task_id}")


# ---------------------------------------------------------------------------
# pi_steer
# ---------------------------------------------------------------------------

PI_STEER_SCHEMA = {
    "name": "pi_steer",
    "description": "Send a Pi 'steer' message to a live (non-final) pi_task.",
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "message": {"type": "string"},
        },
        "required": ["task_id", "message"],
    },
}


def handle_pi_steer(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    message = args.get("message")
    if not task_id or not message:
        return _error("task_id and message are required")
    manager = get_manager()
    try:
        return _result(manager.steer_task(str(task_id), str(message)))
    except KeyError:
        return _error(f"unknown task_id: {task_id}")
    except RuntimeError as exc:
        return _error(str(exc))


# ---------------------------------------------------------------------------
# pi_resume
# ---------------------------------------------------------------------------

PI_RESUME_SCHEMA = {
    "name": "pi_resume",
    "description": "Explicitly re-run the recovery algorithm for one task: reopen its exact "
                    "session file in RPC mode, verify identity via get_state, replay entries "
                    "via get_entries(since=cursor), and reattach the watchdog. Never resumes "
                    "on identity mismatch or a missing/empty session.",
    "parameters": {
        "type": "object",
        "properties": {"task_id": {"type": "string"}},
        "required": ["task_id"],
    },
}


def handle_pi_resume(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    if not task_id:
        return _error("task_id is required")
    manager = get_manager()
    try:
        return _result(manager.recover_task(str(task_id)))
    except KeyError:
        return _error(f"unknown task_id: {task_id}")


# ---------------------------------------------------------------------------
# pi_verify (separate verification step; execution vs verification are
# distinct axes per the spec)
# ---------------------------------------------------------------------------

PI_VERIFY_SCHEMA = {
    "name": "pi_verify",
    "description": "Run a verifier command (argv, no shell) against a SETTLED task and derive "
                    "verification_state PASS/FAIL. A task without a verifier stays "
                    "SETTLED/NOT_RUN and is never reported as DONE.",
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "verifier_argv": {"type": "array", "items": {"type": "string"}},
            "verifier_timeout_seconds": {"type": "number"},
        },
        "required": ["task_id", "verifier_argv"],
    },
}


def handle_pi_verify(args: Dict[str, Any], **_kw) -> str:
    task_id = args.get("task_id")
    verifier_argv = args.get("verifier_argv")
    if not task_id or not verifier_argv:
        return _error("task_id and verifier_argv are required")
    manager = get_manager()
    row = manager.registry.get_task(str(task_id))
    if row is None:
        return _error(f"unknown task_id: {task_id}")
    if row.get("execution_state") != EXEC_SETTLED:
        return _error(
            f"task is not SETTLED yet (execution_state={row.get('execution_state')}); "
            "verification only runs after real settlement"
        )
    verifier = VerifierSpec(
        argv=list(verifier_argv),
        timeout_seconds=float(args.get("verifier_timeout_seconds") or 120.0),
    )
    outcome = manager.run_verifier_now(str(task_id), verifier)
    return _result(manager.status(str(task_id)) | {"verifier_outcome": outcome})


_TOOLS = (
    ("pi_task", PI_TASK_SCHEMA, handle_pi_task),
    ("pi_status", PI_STATUS_SCHEMA, handle_pi_status),
    ("pi_abort", PI_ABORT_SCHEMA, handle_pi_abort),
    ("pi_steer", PI_STEER_SCHEMA, handle_pi_steer),
    ("pi_resume", PI_RESUME_SCHEMA, handle_pi_resume),
    ("pi_verify", PI_VERIFY_SCHEMA, handle_pi_verify),
    ("pi_digest", PI_DIGEST_SCHEMA, handle_pi_digest),
)


_pi_available: Optional[bool] = None


def _gateway_injection_supported(ctx: Any) -> bool:
    """Does this host's ``inject_message`` accept a ``session_key``?

    Terminal continuation routes the wake into the ORIGINAL orchestrator
    session by passing the persisted key verbatim
    (``ctx.inject_message(msg, session_key=origin.session_key)``). Hosts
    older than Hermes v0.21.0 expose ``inject_message(content, role)`` only:
    the call still succeeds, but the wake lands in whatever conversation the
    CLI happens to hold — or nowhere — and the worker burns all
    ``max_attempts`` retries before parking the task in ``wake_exhausted``.

    That failure is silent in exactly the wrong way: every individual call
    "worked", so nothing surfaces until a delegated task's result never
    arrives. Checking the signature once at registration turns it into one
    explicit line in the log.
    """
    inject = getattr(ctx, "inject_message", None)
    if not callable(inject):
        return False
    try:
        return "session_key" in inspect.signature(inject).parameters
    except (TypeError, ValueError):
        # Un-introspectable callable (C extension, exotic wrapper): assume the
        # host is capable rather than disabling continuation on a guess.
        return True


def _pi_binary_available() -> bool:
    """Is the Pi CLI installed on this host?

    Registered as ``check_fn`` on every tool: when it returns False the model
    never sees them. Without ``pi`` on PATH each of the seven tools can only
    fail, and seven dead entries in the tool list cost prompt budget on every
    turn while inviting the agent to try something that cannot work. Hiding
    them is the documented way to ship a plugin whose dependency may be
    absent.

    Cached: PATH lookups run on every tool-definition build, and a binary
    does not appear mid-session. Set ``PI_MANAGER_ASSUME_PI=1`` to force the
    tools visible (tests, or a host where the binary is resolved indirectly).
    """
    global _pi_available
    if _pi_available is None:
        if os.environ.get("PI_MANAGER_ASSUME_PI") == "1":
            _pi_available = True
        else:
            try:
                _pi_available = bool(find_pi_binary())
            except Exception:  # noqa: BLE001 - an unresolvable binary is "absent"
                _pi_available = False
    return _pi_available


def _stop_all_workers() -> None:
    _stop_worker()
    _stop_wake_worker()


def register_all(ctx) -> None:
    for name, schema, handler in _TOOLS:
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=handler,
            check_fn=_pi_binary_available,
        )
    # NOTE: the plugin deliberately registers NO "send_message" entry —
    # core keeps that name host-only (registry.get_entry("send_message")
    # must stay None). The outbox worker delivers by calling the
    # plugin-local host adapter directly, which imports and directly calls
    # the native tools.send_message_tool.send_message_tool: zero
    # LLM/agent turns, no registry dispatch, no model-visible surface.
    # Start (or replace) the notification worker and the terminal wake
    # worker. Both start() calls return immediately — plugin registration
    # never blocks on delivery — and any previous workers are stopped.
    # Terminal continuation needs session_key-aware injection (Hermes
    # >= v0.21.0). Checked BEFORE the try/except below on purpose: that
    # handler downgrades any worker failure to a warning, which would hide
    # this one behind the very silence it is meant to prevent. The rest of
    # the plugin does not need injection — the outbox delivers through the
    # host adapter — so an old host loses terminal wakes only, and Telegram
    # notifications keep working.
    wake_ok = _gateway_injection_supported(ctx)
    if not wake_ok:
        logger.error(
            "pi-manager: terminal continuation DISABLED — this host's "
            "inject_message() takes no session_key, which Hermes >= v0.21.0 "
            "provides. Delegated task results will still be delivered by the "
            "notification outbox, but no wake will be injected into the "
            "originating session. Upgrade Hermes to restore continuation."
        )
    try:
        _replace_worker()
        if wake_ok:
            _replace_wake_worker(ctx)
        on_unload = getattr(ctx, "on_unload", None)
        if callable(on_unload):
            on_unload(_stop_all_workers)
    except Exception as exc:
        # A worker failure must never break plugin loading: notifications
        # and wakes stay queued durably and are picked up by the next
        # registration.
        logger.warning("pi-manager: notification worker start failed: %s", exc)
