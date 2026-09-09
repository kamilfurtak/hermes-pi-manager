"""Passive CLI notices through the owning prompt_toolkit application.

Compatibility boundary only: no core patch, input injection or delegation queue.
The manager reference is bound at registration; its CLI becomes available later.
"""
from __future__ import annotations

import asyncio
import unicodedata
from typing import Any

_manager: Any = None
_runtime_id = ""


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


def _target(origin: dict):
    if not is_cli(origin) or not _runtime_id or origin.get("host_runtime_id") != _runtime_id:
        return None
    cli = getattr(_manager, "_cli_ref", None)
    sid = origin["cli_session_id"]
    current = getattr(cli, "session_id", None)
    if sid != current:
        try:
            db = getattr(cli, "_session_db", None)
            if db is None or db.resolve_resume_session_id(sid) != current:
                return None
        except Exception:
            return None
    app = getattr(cli, "_app", None)
    loop = getattr(app, "loop", None)
    if (not callable(getattr(cli, "_console_print", None)) or
            not getattr(app, "is_running", False) or loop is None or not loop.is_running() or
            getattr(cli, "_agent_running", False)):
        return None
    return cli, app, loop


def available(origin: dict) -> bool:
    return _target(origin) is not None


def emit_status(origin: dict, message: str, notification_id: str) -> dict:
    target = _target(origin)
    if target is None:
        raise RuntimeError("CLI session owner is not available")
    cli, app, loop = target
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
