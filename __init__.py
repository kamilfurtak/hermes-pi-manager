"""pi-manager plugin — hardened Hermes -> Pi RPC task monitoring.

WHAT THIS IS
------------
A standalone Hermes plugin (opt-in via ``plugins.enabled``, never enabled in
production config by this change) that replaces the previous synchronous
``subprocess.run(pi --mode json --print --no-session ...)`` execution path
with a reusable ``PiManager`` core:

  - one Popen per task, started with ``pi --mode rpc`` (JSONL over
    stdin/stdout);
  - a durable SQLite registry + append-only event log under
    ``$HERMES_HOME/state/pi-manager/registry.sqlite3``;
  - a layered watchdog (periodic get_state heartbeat on its own ~30 s
    interval — a liveness/consistency check, not the event source; the
    persisted ``last_progress_at`` / current-tool fields drive a
    progress-based stall policy with NO hard wall-clock deadline by default:
    silence past the quiet threshold triggers an RPC abort plus a 120 s
    grace period, settlement during the grace preserves the true result,
    only an agent that ignores the abort is finalized STALLED; UNRESPONSIVE
    when a heartbeat times out, even before the soft-stall threshold);
  - session recovery after a manager/plugin restart (exact session file +
    identity check + get_entries cursor replay — never a silent new empty
    session);
  - semantic completion gated on the real Pi ``agent_settled`` event only,
    followed by a SEPARATE verifier step (execution_state vs
    verification_state are distinct axes);
  - a durable, plugin-owned notification outbox (``notifications`` table,
    same registry/WAL conventions): routing origin captured at dispatch and
    persisted with the task; bounded rate-limited progress notices; terminal
    settled/failed/stalled/verifier notices that carry the real verdict;
    delivery by a worker that calls the plugin-local host adapter
    (``host_adapter.py``) directly, which imports and directly calls the
    native ``tools.send_message_tool.send_message_tool`` — with bounded
    retry and lease-expiry requeue (at-least-once, best-effort local dedupe).
    Core intentionally never registers ``send_message`` in the tool registry
    (host-only invariant, pinned by core's
    ``test_send_message_remains_host_only``), so this plugin registers NO
    ``send_message`` entry at all (``registry.get_entry("send_message")``
    must be None) and reaches the native rail by calling its
    implementation — the rail remains passive, zero agent turns.
  - a separate, durable TERMINAL CONTINUATION WAKE for the original
    orchestrator session (user-approved 2026-08): a task dispatched from a
    gateway/Telegram session (a valid ``origin.session_key``) resumes its
    session exactly ONCE at a terminal final state, after the verifier has
    resolved (never on manual abort; never for progress), via the
    plugin-owned ``TerminalWakeWorker`` (``wake_worker.py``) calling
    ``PluginContext.inject_message(message, session_key=origin.session_key)``
    — the only ``inject_message`` path in this plugin. The wake state
    machine (one logical wake per task) lives on the task row, not a second
    outbox table, and is crash-safe: a dispatch that survives a process
    death becomes 'uncertain' and is never retried.

See ``core.py`` for the state machine, ``rpc_transport.py`` for the JSONL
transport, ``registry_db.py`` for the SQLite schema, and ``tools.py`` for
the five registered tools (``pi_task``, ``pi_status``, ``pi_abort``,
``pi_steer``, ``pi_resume``, plus ``pi_verify`` for the separate
verification step).

This plugin does not patch Hermes core, does not run a daemon or a second
worker system, and stays disabled in production config — it is loadable by
a temporary Hermes loader test via a scratch ``HERMES_HOME`` only.
"""

from __future__ import annotations

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from .tools import register_all, get_manager  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    from tools import register_all, get_manager


def register(ctx) -> None:
    register_all(ctx)
    # Trigger manager initialization + recover_all() once, at plugin
    # registration time, rather than lazily on the first tool call: if
    # Hermes restarts and no pi_* tool is invoked yet, stale non-final/
    # settled-pending tasks would otherwise never be recovered. Any
    # recovery error is caught here and must never break plugin loading —
    # get_manager() already bounds its own recover_all() call the same way.
    try:
        get_manager()
    except Exception:
        pass
