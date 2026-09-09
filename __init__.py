"""Hermes -> Pi RPC task manager, opt-in through plugins.enabled.

One durable registry owns execution, watchdog, verification and passive notices.
After verification, one continuation returns to the originating CLI, gateway or
ordinary Desktop session. Progress never starts an orchestrator turn.

core.py owns task semantics; registry_db.py owns durable state; rpc_transport.py
owns Pi; outbox.py and wake_worker.py own delivery. Native-host compatibility is
isolated in host_adapter.py, desktop_host.py and cli_host.py; Hermes core stays unchanged.
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
