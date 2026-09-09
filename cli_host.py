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
_quiet_start = None


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


def ensure_monitor(manager) -> bool:
    cli = getattr(_manager, "_cli_ref", None)
    app = getattr(cli, "_app", None)
    loop = getattr(app, 'loop', None)
    native = getattr(cli, "_subagent_monitor", None)
    if (app is None or not app.is_running or loop is None or not loop.is_running() or native is None or
            not all(callable(getattr(native, key, None)) for key in ('refresh', 'control', 'dock_text'))):
        return False  # Older host: ordinary passive notices remain available.

    def attach():
        global _monitor
        if cli is not getattr(_manager, '_cli_ref', None):
            return False
        if _monitor is not None and _monitor.cli is cli and not _monitor.closed:
            _monitor.refresh()
            return True
        try:
            try:
                from .cli_monitor import Monitor
            except ImportError:
                from cli_monitor import Monitor
            if _monitor is not None:
                _monitor.close()
            _monitor = Monitor(cli, manager, lambda origin: owns(cli, origin))
            _monitor.attach()
            return True
        except Exception:
            if _monitor is not None:
                try:
                    _monitor.close()
                except Exception:
                    pass
            _monitor = None
            return False
    try:
        try:
            if asyncio.get_running_loop() is loop:
                return attach()
        except RuntimeError:
            pass
        # A tool caller may omit its startup message only after the native
        # view actually attached, not merely after scheduling an attempt.
        from concurrent.futures import Future
        ready = Future()
        loop.call_soon_threadsafe(lambda: ready.set_result(attach()))
        return ready.result(timeout=2)
    except Exception:
        return False  # CLI teardown cannot change the already-started task's result.


def stop_monitor():
    global _monitor, _quiet_start
    quiet, _quiet_start = _quiet_start, None
    if quiet is not None:
        def close_quiet():
            try:
                quiet.close()
            except Exception:
                pass
        loop = getattr(getattr(quiet.cli, '_app', None), 'loop', None)
        if loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(close_quiet)
            except RuntimeError:
                close_quiet()
        else:
            close_quiet()
    monitor, _monitor = _monitor, None
    if monitor is not None:
        loop = getattr(monitor.cli._app, 'loop', None)
        if loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(monitor.close)
            except RuntimeError:
                pass


def startup_view(origin: dict) -> dict:
    """Arm quiet presentation only for the current CLI turn and native dock."""
    global _quiet_start
    view = {
        'presentation': 'native-subagent-monitor',
        'instruction': 'The native Subagents panel shows startup and progress. '
                       'If only waiting for Pi, end this turn with one short acknowledgement; '
                       'do not return an empty response. Do not poll pi_status or generate progress turns. '
                       'The terminal result will resume this conversation automatically.',
    }
    target = _target(origin, idle=False)
    if target is None or _monitor is None or _monitor.closed or _monitor.cli is not target[0]:
        return view
    cli, _, loop = target
    agent = getattr(cli, 'agent', None)

    def release(quiet):
        global _quiet_start
        if _quiet_start is quiet:
            _quiet_start = None

    def attach():
        global _quiet_start
        try:
            try:
                from .cli_startup import ACKNOWLEDGEMENT, QuietStart
            except ImportError:
                from cli_startup import ACKNOWLEDGEMENT, QuietStart
            if (getattr(cli, 'agent', None) is not agent
                    or not owns(cli, origin) or not QuietStart.supported(cli)):
                return None
            if _quiet_start is None or not _quiet_start.current():
                if _quiet_start is not None:
                    _quiet_start.close()
                _quiet_start = QuietStart(cli, lambda: owns(cli, origin), on_close=release)
                _quiet_start.attach()
            return ACKNOWLEDGEMENT
        except Exception:
            return None

    try:
        from concurrent.futures import Future
        ready = Future()
        try:
            same_loop = asyncio.get_running_loop() is loop
        except RuntimeError:
            same_loop = False
        if same_loop:
            reply = attach()
        else:
            def queued_attach():
                # A timed-out tool must not attach a turn-specific filter later,
                # after the caller has already fallen back to an ordinary reply.
                if ready.set_running_or_notify_cancel():
                    ready.set_result(attach())
            loop.call_soon_threadsafe(queued_attach)
            try:
                reply = ready.result(timeout=2)
            except Exception:
                ready.cancel()
                raise
        if reply:
            view.update(quiet_start=True, reply=reply)
            view['instruction'] = (
                'The native Subagents panel already confirms startup and shows progress. '
                'If only waiting for Pi, finish this turn with exactly the supplied reply field '
                'as plain text, without additional text. Do not return an empty response: Hermes '
                'would retry it. The CLI presents that acknowledgement in the Subagents panel '
                'without a separate startup/status message. If the user explicitly requested '
                'a task_id or startup report, answer that request normally instead of using the '
                'supplied reply. Continue any independent work normally. Do not poll pi_status '
                'or generate progress turns. The terminal result will resume this conversation automatically.'
            )
    except Exception:
        pass  # A view failure cannot change the already-started task's result.
    return view


def available(origin: dict) -> bool:
    target = _target(origin)
    return target is not None and not _terminal_busy(target[0], target[1])


def _terminal_busy(cli, app):
    """Do not queue a timed notification behind a full-screen inspector.

    Cancelling prompt_toolkit's in_terminal() while it awaits the previous
    owner can leave an unresolved future in the shared terminal queue. The
    agent keeps answering but all subsequent scrollback prints then stall.
    """
    native = getattr(cli, '_subagent_monitor', None)
    pending = getattr(app, '_running_in_terminal_f', None)
    return bool(getattr(native, 'opening', False) or getattr(native, 'app', None)
                or getattr(app, '_running_in_terminal', False)
                or (pending is not None and not pending.done()))


def emit_status(origin: dict, message: str, notification_id: str, *, kind="") -> dict:
    target = _target(origin)
    if target is None:
        raise RuntimeError("CLI session owner is not available")
    cli, app, loop = target
    if _terminal_busy(cli, app):
        raise RuntimeError('CLI terminal is owned by another view')
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
            # Recheck before joining the terminal queue on its owning loop.
            # Shield the queue operation if another owner wins the remaining
            # scheduling race: a delivery timeout must never cancel its chain.
            if _terminal_busy(cli, app):
                raise RuntimeError('CLI terminal became busy before notification display')
            return await asyncio.shield(run_in_terminal(display))

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
    return (not _terminal_busy(cli, target[1]) and not getattr(cli, '_command_running', False) and
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
