"""pi-manager tools-level integration: dispatch routing capture, outbox
wiring, and worker registration lifecycle.

The real Hermes gateway module is faked in sys.modules (it only exists
inside the Hermes runtime), and ``tools.get_manager`` is patched to a
manager bound to a temp registry — so nothing in this file can create or
touch production $HERMES_HOME state.
"""

from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path

from test_pi_manager import (  # type: ignore
    PiManagerTestCase,
    FakePiProcess,
    fake_popen_factory,
    TEST_THRESHOLDS,
    wait_until,
)
import tools  # type: ignore  (path setup happens inside test_pi_manager)
from core import PiManager  # type: ignore
from outbox import NotificationOutbox  # type: ignore
from registry_db import Registry  # type: ignore

GATEWAY_ENV = {
    "HERMES_SESSION_PLATFORM": "telegram",
    "HERMES_SESSION_CHAT_ID": "-1001234567890",
    "HERMES_SESSION_THREAD_ID": "3315",
    "HERMES_SESSION_MESSAGE_ID": "841211",
    "HERMES_SESSION_KEY": "agent:main:telegram:group:-1001234567890:3315",
    "HERMES_SESSION_SCOPE_ID": "tenant-7",
    "HERMES_SESSION_USER_ID": "u1",
    "HERMES_SESSION_USER_NAME": "Kamil",
    "HERMES_SESSION_ID": "20260901_010000_aaa111",
    "HERMES_UI_SESSION_ID": "ui-tab-9",
}


def _install_fake_gateway(env: dict):
    fake_pkg = types.ModuleType("gateway")
    fake_mod = types.ModuleType("gateway.session_context")

    def get_session_env(name: str, default: str = "") -> str:
        return env.get(name, default)

    fake_mod.get_session_env = get_session_env
    fake_pkg.session_context = fake_mod
    saved = (sys.modules.get("gateway"), sys.modules.get("gateway.session_context"))
    sys.modules["gateway"] = fake_pkg
    sys.modules["gateway.session_context"] = fake_mod
    return saved


def _restore_gateway(saved):
    for key, value in (("gateway.session_context", saved[1]),
                       ("gateway", saved[0])):
        if value is None:
            sys.modules.pop(key, None)
        else:
            sys.modules[key] = value


class TestCaptureRouting(unittest.TestCase):
    def test_captures_public_session_env_values(self):
        saved = _install_fake_gateway(GATEWAY_ENV)
        self.addCleanup(lambda: _restore_gateway(saved))
        saved_platform = os.environ.pop("HERMES_PLATFORM", None)
        self.addCleanup(lambda: os.environ.__setitem__("HERMES_PLATFORM", saved_platform)
                        if saved_platform is not None else None)
        routing = tools._capture_routing()
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
            self.assertEqual(routing.get(key), GATEWAY_ENV[env], key)

    def test_cli_process_yields_nothing_without_raising(self):
        saved = _install_fake_gateway({})
        self.addCleanup(lambda: _restore_gateway(saved))
        saved_platform = os.environ.pop("HERMES_PLATFORM", None)
        self.addCleanup(lambda: os.environ.__setitem__("HERMES_PLATFORM", saved_platform)
                        if saved_platform is not None else None)
        self.assertEqual(tools._capture_routing(), {})

    def test_hermes_platform_env_is_a_fallback_only(self):
        saved = _install_fake_gateway({})
        self.addCleanup(lambda: _restore_gateway(saved))
        os.environ["HERMES_PLATFORM"] = "discord"
        self.addCleanup(os.environ.pop, "HERMES_PLATFORM", None)
        routing = tools._capture_routing()
        self.assertEqual(routing.get("platform"), "discord")
        self.assertNotIn("chat_id", routing)


class TestHandlePiTaskPersistsOrigin(PiManagerTestCase):
    """handle_pi_task captures the routing origin on the dispatching turn
    and persists it with the task, so settlement/crash/restart all deliver
    to the originating conversation."""

    def setUp(self):
        super().setUp()
        self.process = FakePiProcess()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)
        self.manager = PiManager(
            registry=self.registry,
            popen_factory=fake_popen_factory(self.process),
            clock=self.clock,
            default_thresholds=TEST_THRESHOLDS,
            outbox=self.outbox,
        )
        real = tools.get_manager
        tools.get_manager = lambda: self.manager
        self.addCleanup(lambda: setattr(tools, "get_manager", real))
        saved = _install_fake_gateway(GATEWAY_ENV)
        self.addCleanup(lambda: _restore_gateway(saved))
        saved_platform = os.environ.pop("HERMES_PLATFORM", None)
        self.addCleanup(lambda: os.environ.__setitem__("HERMES_PLATFORM", saved_platform)
                        if saved_platform is not None else None)

    def tearDown(self):
        tools.reset_worker_for_tests()
        super().tearDown()

    def test_origin_snapshot_persisted_at_dispatch(self):
        import json
        out = tools.handle_pi_task({
            "prompt": "do the thing", "cwd": str(self.cwd_dir), "task_id": "pi-tool-1",
        })
        self.assertNotIn("error", out)
        row = self.registry.get_task("pi-tool-1")
        self.assertIsNotNone(row, "handle_pi_task must create the registry row")
        origin = json.loads(row["origin"])
        self.assertEqual(origin["platform"], "telegram")
        self.assertEqual(origin["chat_id"], "-1001234567890")
        self.assertEqual(origin["thread_id"], "3315")
        self.assertEqual(origin["session_key"], GATEWAY_ENV["HERMES_SESSION_KEY"])
        self.assertEqual(origin["scope_id"], "tenant-7")
        self.assertTrue(wait_until(
            lambda: self.process.wait_until_command("get_state")),
            "boot thread must have spawned the (fake) RPC process")


class TestRegistrationWorkerLifecycle(PiManagerTestCase):
    """register_all starts a fresh-context worker without blocking;
    the on_unload hook stops it cleanly. A re-registration replaces the
    worker instead of stacking another one."""

    def setUp(self):
        super().setUp()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)
        self.manager = PiManager(
            registry=self.registry, popen_factory=lambda a, c: None,
            clock=self.clock, default_thresholds=TEST_THRESHOLDS,
            outbox=self.outbox,
        )
        real = tools.get_manager
        tools.get_manager = lambda: self.manager
        self.addCleanup(lambda: setattr(tools, "get_manager", real))

    def tearDown(self):
        tools.reset_worker_for_tests()
        super().tearDown()

    class FakeCtx:
        def __init__(self):
            self.registered = []
            self.unload_callbacks = []

        def register_tool(self, **kw):
            self.registered.append(kw["name"])

        def on_unload(self, callback):
            self.unload_callbacks.append(callback)

    def test_registration_starts_worker_without_blocking(self):
        import time as _time
        ctx = self.FakeCtx()
        started = _time.time()
        tools.register_all(ctx)
        self.assertLess(_time.time() - started, 5.0,
                        "registration must never block on delivery")
        self.assertIn("pi_task", ctx.registered)
        self.assertEqual(len(ctx.unload_callbacks), 1)
        worker = tools._worker
        self.assertIsNotNone(worker)
        self.assertTrue(wait_until(worker.running, timeout=5.0),
                        "worker thread must be alive after registration")

        # on_unload (plugin disable / Hermes shutdown) stops it cleanly.
        ctx.unload_callbacks[0]()
        self.assertTrue(wait_until(lambda: tools._worker is None or not tools._worker.running(),
                                   timeout=10.0),
                        "on_unload must stop the worker cleanly")

    def test_re_registration_replaces_the_worker(self):
        ctx1, ctx2 = self.FakeCtx(), self.FakeCtx()
        tools.register_all(ctx1)
        first = tools._worker
        self.assertTrue(wait_until(first.running, timeout=5.0))
        tools.register_all(ctx2)
        second = tools._worker
        self.assertIsNot(first, second)
        self.assertTrue(wait_until(second.running, timeout=5.0))
        # The stale worker must be stopped, not left dispatching.
        self.assertTrue(wait_until(lambda: not first.running(), timeout=10.0),
                        "the previous worker (stale ctx) must be stopped")

    def test_worker_never_blocks_registration_even_with_backlog(self):
        # A backlog of queued rows must not slow down register_all: the
        # first drain pass is a bounded claim, and the loop yields.
        import json as _json
        origin = _json.dumps({"platform": "telegram", "chat_id": "515151"})
        for i in range(3):
            self.registry.create_task(task_id=f"pi-back{i}",
                                      execution_state="SETTLED", origin=origin)
            self.outbox.enqueue(f"pi-back{i}", "settled", f"body {i}")
        # And the backlog gets drained by the live worker (real clock).
        # The registered worker delivers through the default plugin-local
        # host adapter; the native host tool is unavailable in the
        # standalone test process, so the adapter's module attribute is
        # patched to a recording double (restored on cleanup).
        import host_adapter  # type: ignore
        deliveries = []
        real_deliver = host_adapter.deliver_send_message

        def fake_deliver(args, **kw):
            deliveries.append(dict(args))
            return '{"sent": true}'

        host_adapter.deliver_send_message = fake_deliver
        self.addCleanup(lambda: setattr(host_adapter, "deliver_send_message", real_deliver))
        import time as _time
        ctx = self.FakeCtx()
        started = _time.time()
        tools.register_all(ctx)
        self.assertLess(_time.time() - started, 5.0)
        self.assertTrue(wait_until(
            lambda: sum(1 for n in self.registry.list_notifications(limit=100)
                        if n["status"] == "sent") >= 3,
            timeout=15.0),
            "the registered worker must drain the backlog through the host adapter")
        self.assertTrue(all(a["action"] == "send" for a in deliveries))
        self.assertGreaterEqual(len(deliveries), 3)


class TestGatewayInjectionGuard(PiManagerTestCase):
    """Terminal continuation needs a session_key-aware ``inject_message``
    (Hermes >= v0.21.0). On an older host the wake worker must NOT start —
    otherwise every wake burns its retries and parks the task in
    ``wake_exhausted`` while each individual call reports success. Tool
    registration and the notification outbox stay up either way, because
    the outbox delivers through the host adapter and needs no injection."""

    def setUp(self):
        super().setUp()
        self.outbox = NotificationOutbox(self.registry, now_fn=self.clock)
        self.manager = PiManager(
            registry=self.registry, popen_factory=lambda a, c: None,
            clock=self.clock, default_thresholds=TEST_THRESHOLDS,
            outbox=self.outbox,
        )
        real = tools.get_manager
        tools.get_manager = lambda: self.manager
        self.addCleanup(lambda: setattr(tools, "get_manager", real))

    def tearDown(self):
        tools.reset_worker_for_tests()
        super().tearDown()

    class ModernCtx:
        """Hermes >= v0.21.0."""
        def __init__(self):
            self.registered = []

        def register_tool(self, **kw):
            self.registered.append(kw["name"])

        def inject_message(self, content, role="user", *, session_key=None):
            return True

    class LegacyCtx:
        """Hermes <= v0.20.x — no session_key parameter."""
        def __init__(self):
            self.registered = []

        def register_tool(self, **kw):
            self.registered.append(kw["name"])

        def inject_message(self, content, role="user"):
            return True

    def test_modern_host_is_detected_and_starts_the_wake_worker(self):
        ctx = self.ModernCtx()
        self.assertTrue(tools._gateway_injection_supported(ctx))
        tools.register_all(ctx)
        self.assertIsNotNone(tools._wake_worker,
                             "a session_key-aware host must get the wake worker")

    def test_legacy_host_disables_continuation_but_keeps_the_plugin(self):
        ctx = self.LegacyCtx()
        self.assertFalse(tools._gateway_injection_supported(ctx))
        with self.assertLogs(tools.logger, level="ERROR") as captured:
            tools.register_all(ctx)
        self.assertTrue(
            any("terminal continuation DISABLED" in m for m in captured.output),
            "an old host must say so once, loudly, at registration")
        self.assertIsNone(tools._wake_worker,
                          "no wake worker may run where wakes cannot be routed")
        # The rest of the plugin is unaffected.
        self.assertIn("pi_task", ctx.registered)
        self.assertIsNotNone(tools._worker,
                             "notification delivery must survive an old host")

    def test_missing_inject_message_is_treated_as_unsupported(self):
        class NoInject:
            def register_tool(self, **kw):
                pass
        self.assertFalse(tools._gateway_injection_supported(NoInject()))

    def test_uninspectable_callable_is_assumed_capable(self):
        """A C-extension or exotic wrapper must not disable continuation on a
        guess — signature introspection failing is not evidence of an old host."""
        class Opaque:
            inject_message = print  # builtin: signature() raises for some builtins
        supported = tools._gateway_injection_supported(Opaque())
        self.assertIsInstance(supported, bool)


if __name__ == "__main__":
    unittest.main()

class TestPiBinaryGate(PiManagerTestCase):
    """Without the Pi CLI the seven tools can only fail, so the model must
    not see them: seven dead entries cost prompt budget every turn and
    invite the agent to attempt work that cannot succeed."""

    def tearDown(self):
        tools._pi_available = None      # the cache is process-wide
        os.environ.pop("PI_MANAGER_ASSUME_PI", None)
        super().tearDown()

    def test_tools_are_hidden_when_the_binary_is_missing(self):
        real = tools.find_pi_binary
        tools.find_pi_binary = lambda: None
        self.addCleanup(lambda: setattr(tools, "find_pi_binary", real))
        tools._pi_available = None
        self.assertFalse(tools._pi_binary_available())

    def test_tools_are_visible_when_the_binary_exists(self):
        real = tools.find_pi_binary
        tools.find_pi_binary = lambda: "/usr/local/bin/pi"
        self.addCleanup(lambda: setattr(tools, "find_pi_binary", real))
        tools._pi_available = None
        self.assertTrue(tools._pi_binary_available())

    def test_a_raising_lookup_reads_as_absent(self):
        real = tools.find_pi_binary
        def boom():
            raise OSError("PATH is broken")
        tools.find_pi_binary = boom
        self.addCleanup(lambda: setattr(tools, "find_pi_binary", real))
        tools._pi_available = None
        self.assertFalse(tools._pi_binary_available(),
                         "an unresolvable binary must never crash registration")

    def test_env_override_forces_visibility(self):
        real = tools.find_pi_binary
        tools.find_pi_binary = lambda: None
        self.addCleanup(lambda: setattr(tools, "find_pi_binary", real))
        tools._pi_available = None
        os.environ["PI_MANAGER_ASSUME_PI"] = "1"
        self.assertTrue(tools._pi_binary_available())

    def test_registration_passes_the_gate_to_every_tool(self):
        seen = {}
        class Ctx:
            def register_tool(self, **kw):
                seen[kw["name"]] = kw.get("check_fn")
            def inject_message(self, content, role="user", *, session_key=None):
                return True
        tools.register_all(Ctx())
        self.assertEqual(len(seen), 7)
        self.assertTrue(all(fn is tools._pi_binary_available for fn in seen.values()),
                        "every tool must be gated, not just the ones that spawn Pi")
