"""Focused tests: the outbox worker delivers through the plugin-local host
adapter, which imports and directly calls the native
``tools.send_message_tool.send_message_tool``.

Architecture invariants pinned here:

- the native tool is stubbed at its import boundary (``sys.modules``);
  everything above it runs for real — the worker's claim/dedupe/retry
  logic, the adapter, and the exact call shape
  (``{"action": "send", "target": ..., "message": ...}``);
- the worker's DEFAULT deliver is the host adapter itself (no injected
  fake in the delivery tests), so the production delivery shape — no
  PluginContext, no ``ctx.dispatch_tool``, no ToolRegistry entry — is
  what runs;
- ``OutboxWorker`` has no ``ctx`` parameter at all: delivery cannot reach
  the registry dispatch path.

The registry-side acceptance invariant (``registry.get_entry(
"send_message") is None`` after the REAL loader runs) is pinned in
``test_loader_integration.py``; it needs the hermes-agent tree.
"""

from __future__ import annotations

import json
import sys
import unittest
from types import ModuleType

from test_pi_manager import PiManagerTestCase  # type: ignore
import host_adapter  # type: ignore
from outbox import (  # type: ignore
    RETRY_BACKOFF_SECONDS,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SENT,
    NotificationOutbox,
    OutboxWorker,
)
from registry_db import Registry  # type: ignore

ORIGIN_A = {
    "platform": "telegram",
    "chat_id": "-1001234567890",
    "thread_id": "77",
}
TARGET_A = "telegram:-1001234567890:77"

_MISSING = object()


class _NativeStub:
    """Stands in for the native ``tools.send_message_tool`` module at its
    import boundary. Records every call and returns scripted results."""

    def __init__(self):
        self.calls = []
        self._behavior = None  # None -> success

    def behavior(self, result=None, exc=None):
        self._behavior = (result, exc)

    def send_message_tool(self, args, **kw):
        self.calls.append({"args": dict(args), "kw": dict(kw)})
        result, exc = self._behavior or (None, None)
        if exc is not None:
            raise exc
        if result is not None:
            return result
        return json.dumps({"success": True, "platform": "telegram"})


def _install_native_stub(stub: _NativeStub):
    """Install ``stub.send_message_tool`` as ``tools.send_message_tool``
    in sys.modules; returns the restore callable (wired via addCleanup)."""
    saved = sys.modules.get("tools.send_message_tool", _MISSING)
    fake = ModuleType("tools.send_message_tool")
    fake.send_message_tool = stub.send_message_tool
    sys.modules["tools.send_message_tool"] = fake

    def restore():
        if saved is _MISSING:
            sys.modules.pop("tools.send_message_tool", None)
        else:
            sys.modules["tools.send_message_tool"] = saved

    return restore


# ---------------------------------------------------------------------------
# Adapter contract (direct function level)
# ---------------------------------------------------------------------------


class TestHostAdapterContract(unittest.TestCase):
    def test_adapter_imports_and_calls_the_native_tool_directly(self):
        stub = _NativeStub()
        self.addCleanup(_install_native_stub(stub))
        args = {"action": "send", "target": TARGET_A, "message": "body"}
        result = host_adapter.deliver_send_message(args)
        self.assertEqual(result, json.dumps({"success": True, "platform": "telegram"}),
                         "the native result must pass through verbatim")
        self.assertEqual(len(stub.calls), 1)
        # The native tool receives a COPY of the worker's exact args dict,
        # with the same kwargs it was given.
        self.assertEqual(stub.calls[0]["args"], args)
        self.assertEqual(stub.calls[0]["kw"], {})
        self.assertIsNot(stub.calls[0]["args"], args,
                         "the adapter must pass a copy, not the caller's dict")

    def test_adapter_forwards_kwargs(self):
        stub = _NativeStub()
        self.addCleanup(_install_native_stub(stub))
        host_adapter.deliver_send_message(
            {"action": "send", "target": TARGET_A, "message": "b"},
            session_key="k1",
        )
        self.assertEqual(stub.calls[0]["kw"], {"session_key": "k1"})

    def test_adapter_rejects_non_send_action_loudly(self):
        stub = _NativeStub()
        self.addCleanup(_install_native_stub(stub))
        with self.assertRaises(ValueError):
            host_adapter.deliver_send_message({"action": "list"})
        self.assertEqual(stub.calls, [], "the native tool must never be called")


# ---------------------------------------------------------------------------
# Worker delivery through the DEFAULT host adapter
# ---------------------------------------------------------------------------


class TestWorkerDeliveryThroughHostAdapter(PiManagerTestCase):
    def setUp(self):
        super().setUp()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)
        self.stub = _NativeStub()
        self.addCleanup(_install_native_stub(self.stub))

    def _seed(self, task_id="pi-ha", message="completion body"):
        self.registry.create_task(task_id=task_id, execution_state="SETTLED",
                                  origin=json.dumps(ORIGIN_A))
        return self.outbox.enqueue(task_id, "settled", message)

    def test_default_deliver_is_the_host_adapter(self):
        # The production shape: OutboxWorker(outbox) — no ctx, no injected
        # deliver. The worker must hold the plugin-local host adapter.
        worker = OutboxWorker(self.outbox)
        self.assertIs(worker._deliver, host_adapter.deliver_send_message)

    def test_worker_has_no_ctx_parameter_or_state(self):
        import inspect
        params = inspect.signature(OutboxWorker.__init__).parameters
        self.assertNotIn("ctx", params,
                         "the worker must not take a PluginContext")
        worker = OutboxWorker(self.outbox)
        self.assertFalse(hasattr(worker, "_ctx"),
                         "the worker must not retain any ctx state")

    def test_end_to_end_delivery_marks_sent_with_exact_native_args(self):
        nid = self._seed()
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        self.assertEqual(worker.run_once(now=1000.0), 1)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_SENT)
        self.assertIsNotNone(row["sent_at"])
        self.assertEqual(len(self.stub.calls), 1,
                         "exactly one native send per delivered row")
        self.assertEqual(self.stub.calls[0]["args"], {
            "action": "send",
            "target": TARGET_A,
            "message": "completion body",
        })

    def test_native_error_json_is_classified_and_retried(self):
        nid = self._seed()
        self.stub.behavior(result=json.dumps({"error": "429 Too Many Requests"}))
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_PENDING,
                         "a 429 is transient: the row must requeue")
        self.assertEqual(row["attempts"], 1)
        self.assertAlmostEqual(row["next_retry_at"],
                               1000.0 + RETRY_BACKOFF_SECONDS[0])
        self.assertIn("429", row["last_error"])
        # Now the native tool succeeds on the next due pass.
        self.stub.behavior()
        self.clock.advance(RETRY_BACKOFF_SECONDS[0] + 1.0)
        worker.run_once(now=self.clock())
        self.assertEqual(
            self.registry.get_notification(nid)["status"], STATUS_SENT)
        self.assertEqual(len(self.stub.calls), 2)

    def test_native_permanent_error_fails_the_row(self):
        nid = self._seed()
        self.stub.behavior(result=json.dumps({"error": "Unknown platform: foobar"}))
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_FAILED,
                         "a permanent host error must stop retrying")
        self.assertIn("Unknown platform", row["last_error"])

    def test_native_tool_raising_is_transient_and_bounded(self):
        nid = self._seed()
        self.stub.behavior(exc=RuntimeError("gateway down"))
        worker = OutboxWorker(self.outbox, now_fn=self.clock)
        worker.run_once(now=1000.0)
        row = self.registry.get_notification(nid)
        self.assertEqual(row["status"], STATUS_PENDING)
        self.assertIn("host adapter raised", row["last_error"])
        # Recovery: next due pass delivers.
        self.stub.behavior()
        self.clock.advance(RETRY_BACKOFF_SECONDS[0] + 1.0)
        worker.run_once(now=self.clock())
        self.assertEqual(
            self.registry.get_notification(nid)["status"], STATUS_SENT)


if __name__ == "__main__":
    unittest.main()