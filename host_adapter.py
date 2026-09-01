"""Plugin-local host adapter: outbox delivery -> the native
``tools.send_message_tool.send_message_tool``.

The outbox worker delivers every notification by calling
:func:`deliver_send_message` directly. The adapter imports the Hermes-native
``tools.send_message_tool.send_message_tool`` implementation and calls it —
never through ``ToolRegistry.dispatch`` and never through
``ctx.dispatch_tool``. That is deliberate: Hermes core intentionally never
registers ``send_message`` in the tool registry (it is host/gateway
vocabulary, and core pins the invariant in
``tests/tools/test_send_message_plugin_extensibility.py::
test_send_message_remains_host_only``). The plugin therefore registers NO
``send_message`` entry at all — the acceptance invariant is
``registry.get_entry("send_message") is None`` — and reaches the same native
rail by importing and calling its implementation.

Consequences of this shape:

- the native tool's own behavior is preserved end to end (target
  resolution, redaction, platform APIs, its own bounded retries);
- zero LLM/agent turns: nothing on this path consults a model, and no
  model's tool surface is touched;
- the import is lazy (delivery time, inside the real Hermes process) so
  this module stays importable and unit-testable standalone, where the
  ``tools.send_message_tool`` module can be stubbed via ``sys.modules``.

An adapter failure (host module unavailable, the tool raising) propagates
as an exception; the worker treats that as a transient, retry-budget-bounded
delivery failure and persists it on the row.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# The action this rail uses — the only one the outbox ever needs.
SEND_ACTION = "send"


def deliver_send_message(args: Dict[str, Any], **kw: Any) -> Any:
    """Deliver one notification through the native ``send_message_tool``.

    Imports the host implementation at call time and invokes it directly
    with the exact args dict the worker built::

        {"action": "send", "target": "platform:chat_id[:thread_id]",
         "message": "<bounded body>"}

    Returns the tool's result verbatim (a JSON string on both success and
    failure — the worker parses it via ``outbox.extract_tool_error``).
    Raises on anything unexpected; the outbox worker bounds that with the
    retry schedule.
    """
    action = args.get("action") or SEND_ACTION
    if action != SEND_ACTION:
        # Defensive: the outbox worker only ever builds action="send".
        # Fail loudly rather than silently misdirecting a notification.
        raise ValueError(
            f"pi-manager host adapter only supports action='send' (got {action!r})"
        )
    # Lazy import: inside the real Hermes process this resolves the core
    # tools package; in standalone unit tests the module is stubbed in
    # sys.modules. Never cached at import time on purpose — a process that
    # registers the plugin before the host tool is importable must not be
    # stuck with a dead reference.
    from tools.send_message_tool import send_message_tool  # type: ignore
    return send_message_tool(dict(args), **kw)