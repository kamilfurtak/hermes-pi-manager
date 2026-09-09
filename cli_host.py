"""Passive CLI notices through the owning prompt_toolkit application.

Compatibility boundary only: no core patch or native delegation registry writes.
The manager reference is bound at registration; its CLI becomes available later.
"""
from __future__ import annotations

import asyncio
import unicodedata
from typing import Any

_manager: Any = None
_runtime_id = ""
_monitor = None


def bind(ctx: Any, runtime_id: str) -> None:
    global _manager, _runtime_id
    _manager = getattr(ctx, "_manager", None)
    _runtime_id = runtime_id


def capture(origin: dict) -> dict:
    # Never reinterpret Desktop or messaging origins as a local terminal.
    if origin.get("platform") or origin.get("session_key") or origin.get("ui_session_id") or origin.get("source") in ("tui", "desktop"):
        return {}
    cli = getattr(_manager, "_cli_ref", None)
    sid = getattr(cli, "session_id", None)
    if not isinstance(sid, str) or not sid or not callable(getattr(cli, "_console_print", None)):
        return {}
    # Separate field: session_key selects gateway delivery for terminal wakes.
    return {"cli_session_id": sid}


def is_cli(origin: dict) -> bool:
    return bool(origin.get("cli_session_id")) and not (
        origin.get("platform") or origin.get("session_key") or origin.get("ui_session_id") or
        origin.get("source") in ("tui", "desktop"))


def owns(cli, origin: dict) -> bool:
    if not is_cli(origin) or not _runtime_id or origin.get("host_runtime_id") != _runtime_id:
        return False
    if cli is not getattr(_manager, "_cli_ref", None):
        return False
    sid = origin["cli_session_id"]
    current = getattr(cli, "session_id", None)
    if sid == current:
        return True
    try:
        db = getattr(cli, "_session_db", None)
        return bool(db and db.resolve_resume_session_id(sid) == current)
    except Exception:
        return False


def _target(origin: dict, *, idle=True):
    cli = getattr(_manager, "_cli_ref", None)
    if not owns(cli, origin):
        return None
    app = getattr(cli, "_app", None)
    loop = getattr(app, "loop", None)
    if (not callable(getattr(cli, "_console_print", None)) or
            not getattr(app, "is_running", False) or loop is None or not loop.is_running() or
            (idle and getattr(cli, "_agent_running", False))):
        return None
    return cli, app, loop


def ensure_monitor(manager) -> None:
    cli = getattr(_manager, "_cli_ref", None)
    app = getattr(cli, "_app", None)
    loop = getattr(app, 'loop', None)
    native = getattr(cli, "_subagent_monitor", None)
    if (app is None or not app.is_running or loop is None or not loop.is_running() or native is None or
            not all(callable(getattr(native, key, None)) for key in ('refresh', 'control', 'dock_text'))):
        return  # Older host: ordinary passive notices remain available.

    def attach():
        global _monitor
        if cli is not getattr(_manager, '_cli_ref', None):
            return
        if _monitor is not None and _monitor.cli is cli and not _monitor.closed:
            return
        try:
            try:
                from .cli_monitor import Monitor
            except ImportError:
                from cli_monitor import Monitor
            if _monitor is not None:
                _monitor.close()
            _monitor = Monitor(cli, manager, lambda origin: owns(cli, origin))
            _monitor.attach()
        except Exception:
            if _monitor is not None:
                try:
                    _monitor.close()
                except Exception:
                    pass
            _monitor = None
    try:
        loop.call_soon_threadsafe(attach)
    except RuntimeError:
        pass  # CLI teardown cannot change the already-started task's result.


def stop_monitor():
    global _monitor
    monitor, _monitor = _monitor, None
    if monitor is not None:
        loop = getattr(monitor.cli._app, 'loop', None)
        if loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(monitor.close)
            except RuntimeError:
                pass


def available(origin: dict) -> bool:
    return _target(origin) is not None


def emit_status(origin: dict, message: str, notification_id: str, *, kind="") -> dict:
    target = _target(origin)
    if target is None:
        raise RuntimeError("CLI session owner is not available")
    cli, app, loop = target
    task_id = notification_id[5:].rsplit(':', 1)[0] if notification_id.startswith('prog:') else None
    if (kind == 'progress' and _monitor is not None and not _monitor.closed and _monitor.cli is cli):
        registry = getattr(_monitor, 'registry', None)
        row = registry.get_task(task_id) if registry is not None and task_id else None
        presented = task_id in _monitor.rows or (row and row.get('execution_state') in ('SETTLED', 'ABORTED', 'CRASHED'))
        # The native dock/inspector already refreshes this activity every second.
        # A terminal result also supersedes progress deferred while the parent
        # was busy. Keep the receipt without printing stale work after completion.
        if presented:
            app.invalidate()
            return {"success": True, "presentation": "native-dock"}
    # Treat tool/worker text as plain text, including terminal control sequences.
    text = "".join(c for c in str(message)[:4000]
                   if c in "\n\t" or not unicodedata.category(c).startswith("C"))

    async def render():
        from prompt_toolkit.application import run_in_terminal
        from prompt_toolkit.application.current import set_app

        def display():
            # Recheck on the UI loop, after any /new, compression or busy turn.
            fresh = _target(origin)
            if fresh is None or fresh[0] is not cli or fresh[1] is not app:
                raise RuntimeError("CLI session changed before notification display")
            seen = getattr(cli, "_pi_manager_status_ids", None)
            if seen is None:
                seen = cli._pi_manager_status_ids = set()
            if notification_id in seen:
                return {"success": True, "duplicate": True}
            cli._console_print("\n" + text, markup=False, highlight=False)
            seen.add(notification_id)
            if len(seen) > 2048:
                seen.clear()
                seen.add(notification_id)
            return {"success": True}

        with set_app(app):
            return await run_in_terminal(display)

    future = asyncio.run_coroutine_threadsafe(render(), loop)
    try:
        return future.result(timeout=5)
    except Exception:
        future.cancel()
        raise


class _QueuedWake(str):
    def __new__(cls, message, origin):
        obj = super().__new__(cls, message)
        obj.origin = dict(origin)
        return obj


def can_wake(origin):
    target = _target(origin)
    if target is None:
        return False
    cli = target[0]
    native = getattr(cli, '_subagent_monitor', None)
    return (not getattr(cli, '_command_running', False) and
            not getattr(native, 'opening', False) and
            not getattr(native, 'app', None) and
            not any(getattr(cli, key, None) for key in (
                '_approval_state', '_clarify_state', '_sudo_state', '_secret_state',
                '_slash_confirm_state', '_model_picker_state', '_command_palette_state')) and
            getattr(cli, '_pending_input', None) is not None and cli._pending_input.empty())


def deliver_wake(origin, message):
    """Enqueue only on the normal FIFO, with a scope guard at consumption.

    Never use inject_message here: it chooses the interrupt queue if a human
    turn starts between our idle check and its call.
    """
    from concurrent.futures import Future
    target = _target(origin)
    if target is None:
        return 'unavailable'
    cli, app, loop = target
    future = Future()

    def enqueue():
        if not future.set_running_or_notify_cancel():
            return
        try:
            if not can_wake(origin):
                future.set_result('busy')
                return
            consume = getattr(cli, '_tui_process_one_input', None)
            if not callable(consume):
                future.set_result('unavailable')
                return
            if not getattr(cli, '_pi_wake_guard_installed', False):
                def guarded(value):
                    if isinstance(value, _QueuedWake):
                        if not owns(cli, value.origin):
                            return
                        value = str(value)
                    return consume(value)
                cli._tui_process_one_input = guarded
                cli._pi_wake_guard_installed = True
            cli._pending_input.put(_QueuedWake(message, origin))
            future.set_result('accepted')
        except Exception as exc:
            future.set_exception(exc)
    loop.call_soon_threadsafe(enqueue)
    try:
        return future.result(timeout=5)
    except Exception:
        future.cancel()
        # The callback might already have queued the wake. Never blind-retry.
        raise
