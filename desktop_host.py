"""Compatibility boundary for ordinary Desktop/TUI sessions (no core patch).

Only the already-running native backend may deliver to a session it owns.
Progress uses notification.show, with no model turn. Completion uses the host's
normal prompt admission and terminal callback after its human FIFO is idle.
No Bot Chat redirection, second backend, fabricated delegation or private queue.
"""
from __future__ import annotations

import inspect
import os
import sys
from typing import Any, Dict, Optional, Tuple


class UncertainDelivery(RuntimeError):
    """The native admission call raised; do not blindly replay the wake."""


def is_desktop(origin: Dict[str, Any]) -> bool:
    return bool(origin.get("ui_session_id")) or origin.get("source") in ("tui", "desktop")


def _host():
    # Importing server here could create the appearance of a host in a foreign
    # gateway/CLI. Only use the real module already loaded by serve/TUI.
    host = sys.modules.get("tui_gateway.server")
    required = ("_sessions", "_sessions_lock", "_run_prompt_submit", "write_json",
                "_event_frame", "_session_db", "_session_home", "_hermes_home",
                "set_hermes_home_override", "reset_hermes_home_override")
    if host is None or any(not hasattr(host, key) for key in required):
        return None
    try:
        if "terminal_callback" not in inspect.signature(host._run_prompt_submit).parameters:
            return None
    except (TypeError, ValueError):
        return None
    return host


def _target(origin: Dict[str, Any], *, require_lease: bool = True) -> Optional[Tuple[Any, str, dict]]:
    host = _host()
    key = origin.get("session_key") or origin.get("session_id")
    if host is None or not is_desktop(origin) or not isinstance(key, str) or not key:
        return None
    try:
        expected_home = os.path.realpath(origin.get("hermes_home") or host._hermes_home)
        with host._sessions_lock:
            snapshot = list(host._sessions.items())
        matches, current = [], []
        for sid, session in snapshot:
            if session.get("_finalized") or session.get("_closing"):
                continue
            if os.path.realpath(host._session_home(session)) != expected_home:
                continue
            with host._session_db(session) as db:
                if db is None:
                    continue
                tip = db.resolve_resume_session_id(key) or key
            # A reused UI tab is not sufficient evidence: its durable
            # session identity must still match the original or its tip.
            if session.get("session_key") not in {key, tip}:
                continue
            lease = session.get("active_session_lease")
            if require_lease and (lease is None or getattr(lease, "released", True)):
                continue
            if session.get("agent") is None or "history_lock" not in session:
                continue
            entry = (host, sid, session)
            matches.append(entry)
            if session.get("session_key") == tip:
                current.append(entry)
        # Prefer the compression tip; never guess between live owners.
        candidates = current or matches
        return candidates[0] if len(candidates) == 1 else None
    except Exception:
        return None


def _idle(session: dict) -> bool:
    return not any(session.get(key) for key in (
        "running", "queued_prompt", "queued_prompts", "_auto_continue_scheduled",
        "_closing", "_finalized"))


def available(origin: Dict[str, Any], *, idle: bool = False) -> bool:
    target = _target(origin, require_lease=not idle)
    if target is None:
        return False
    with target[2]["history_lock"]:
        return not idle or _idle(target[2])


def emit_status(origin: Dict[str, Any], message: str, notification_id: str,
                *, task_id: str = "") -> dict:
    target = _target(origin)
    if target is None:
        raise RuntimeError("Desktop session owner is not available in this process")
    host, sid, session = target
    with session["history_lock"]:
        seen = session.setdefault("_pi_manager_status_ids", set())
        if notification_id in seen:
            return {"success": True, "duplicate": True}
        # Desktop's status.update/process handler only refreshes its native
        # process registry; it does NOT display the text. Use its existing
        # notice renderer, replacing one toast per task. This is an in-app
        # notice, not an OS notification or a transcript/model message.
        frame = host._event_frame("notification.show", sid, {
            "key": "pi-manager:" + (task_id or notification_id),
            "kind": "ttl", "ttl_ms": 20000, "level": "info", "text": message,
        })
        if host.write_json(frame) is not True:
            raise RuntimeError("Desktop transport did not accept the notification frame")
        seen.add(notification_id)
        # Local dedup is best effort; durable outbox semantics stay at-least-once.
        if len(seen) > 2048:
            seen.clear()
            seen.add(notification_id)
    return {"success": True}


def deliver_wake(origin: Dict[str, Any], message: str, continuation_id: str,
                 ctx: Any, terminal_callback) -> str:
    # A just-resumed conversation claims its lease at native admission, not
    # at resume. Requiring an existing lease here would force a human turn.
    target = _target(origin, require_lease=False)
    if target is None:
        return "unavailable"
    host, sid, session = target
    allowed = getattr(ctx, "_gateway_injection_allowed", None)
    token = host.set_hermes_home_override(str(host._session_home(session)))
    try:
        # The worker runs outside the originating turn's contextvars. Check
        # the target profile's grant, not the backend launch profile's grant.
        if not callable(allowed) or allowed() is not True:
            return "denied"
    finally:
        host.reset_hermes_home_override(token)
    with session["history_lock"]:
        if not _idle(session):
            return "busy"
        session["running"] = True
    try:
        accepted = host._run_prompt_submit(
            "__pi_wake__" + continuation_id, sid, session, message,
            image_paths=[], terminal_callback=terminal_callback)
    except Exception as exc:
        # Admission may already have started a turn. Preserve uncertainty;
        # neither release someone else's turn nor retry into another host.
        raise UncertainDelivery("native Desktop admission raised: %s" % exc) from exc
    if accepted is not True:
        with session["history_lock"]:
            session["running"] = False
        return "unavailable"
    return "accepted"
