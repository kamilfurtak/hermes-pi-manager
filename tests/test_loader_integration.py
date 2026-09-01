"""pi-manager loader integration test — REAL Hermes plugin loader.

Verifies (without touching production config or launching any real Pi/NInfer
process):
  - the plugin is discovered from a temp HERMES_HOME plugins dir,
  - the manifest parses (kind: standalone),
  - register(ctx) registers all six tools via the real tools.registry,
  - a real registry.dispatch() call for pi_status on an unknown task_id
    round-trips through the actual Hermes dispatch path and returns a
    bounded JSON error (proving end-to-end wiring with zero network/process
    launch),
  - the plugin stays entirely out of production config (only the temp
    HERMES_HOME/config.yaml enables it),
  - send_message is NOT registered in the ToolRegistry at all: core keeps
    that name host-only, and the plugin's acceptance invariant is
    registry.get_entry("send_message") is None after the real loader runs,
  - the outbox worker delivers through the plugin-local host adapter,
    which imports and directly calls the real native
    tools.send_message_tool.send_message_tool: a full outbox row
    (enqueue -> claim -> host adapter -> native tool -> mark_sent)
    completes successfully with zero LLM turns and zero registry entries
    for send_message.

Run:  python3 tests/test_loader_integration.py
Requires the hermes-agent source tree (PYTHONPATH) — discovered
automatically from ~/.hermes/hermes-agent, mirroring the pattern used by
plugins/auto-router/tests/test_loader_integration.py.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(os.path.expanduser("~/.hermes/hermes-agent"))
PLUGIN_DIR = Path(__file__).parent.parent


def _hermes_python() -> str:
    candidates = [
        os.environ.get("HERMES_PYTHON"),
        str(REPO / "venv" / "bin" / "python3"),
        str(REPO / ".venv" / "bin" / "python3"),
        "/opt/homebrew/bin/python3.11",
        "/opt/homebrew/bin/python3.14",
        sys.executable,
    ]
    for cand in candidates:
        if not cand or not Path(cand).exists():
            continue
        try:
            r = subprocess.run([cand, "--version"], capture_output=True, text=True, timeout=10)
            if r.returncode == 0 and "Python 3.1" in r.stdout + r.stderr:
                return cand
        except Exception:
            continue
    raise unittest.SkipTest("no Python >=3.11 interpreter found for hermes-agent")


class TestLoaderIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not REPO.exists():
            raise unittest.SkipTest(f"hermes-agent source not found at {REPO}")
        cls.tmp_home = Path(tempfile.mkdtemp(prefix="hermes-pm-it-"))
        cls.plugins_dst = cls.tmp_home / "plugins" / "pi-manager"
        shutil.copytree(
            PLUGIN_DIR, cls.plugins_dst,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "tests"),
        )
        # Minimal config enabling ONLY pi-manager in the temp home — never
        # touches production ~/.hermes/config.yaml, and confirms this
        # plugin's own manifest/registration works without any other
        # standalone plugin present.
        (cls.tmp_home / "config.yaml").write_text(
            "plugins:\n  enabled:\n    - pi-manager\n",
            encoding="utf-8",
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_home, ignore_errors=True)

    def _load_through_loader(self):
        code = f"""
import os, sys, json
os.environ["HERMES_HOME"] = {str(self.tmp_home)!r}
sys.path.insert(0, {str(REPO)!r})
from hermes_cli.plugins import get_plugin_manager
mgr = get_plugin_manager()
mgr.discover_and_load(force=True)
plugs = sorted(mgr._plugins.keys())
print("PLUGINS:", plugs)
from tools.registry import registry
names = registry.get_all_tool_names()
pi_tools = sorted(n for n in names if n.startswith("pi_"))
print("PI_TOOLS:", pi_tools)
# Real end-to-end dispatch through the actual Hermes tool-registry path —
# no real Pi/NInfer process is launched by pi_status on an unknown task.
out = registry.dispatch("pi_status", {{"task_id": "does-not-exist"}})
print("DISPATCH_OUT:", json.dumps(out))
"""
        child_env = dict(os.environ)
        r = subprocess.run(
            [_hermes_python(), "-c", code],
            capture_output=True, text=True, timeout=120, cwd=str(REPO), env=child_env,
        )
        return r

    def test_loader_discovers_pi_manager(self):
        r = self._load_through_loader()
        self.assertEqual(r.returncode, 0, f"loader subprocess failed:\n{r.stderr}\n{r.stdout}")
        self.assertIn("PLUGINS:", r.stdout)
        plugins_line = r.stdout.split("PLUGINS:")[1].splitlines()[0]
        self.assertIn("pi-manager", plugins_line)

    def test_all_six_tools_registered(self):
        r = self._load_through_loader()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("PI_TOOLS:", r.stdout)
        tools_line = r.stdout.split("PI_TOOLS:")[1].splitlines()[0]
        for name in ("pi_task", "pi_status", "pi_abort", "pi_steer", "pi_resume", "pi_verify"):
            self.assertIn(name, tools_line, f"{name} was not registered by the real loader")

    def test_dispatch_round_trip_no_process_launch(self):
        r = self._load_through_loader()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("DISPATCH_OUT:", r.stdout)
        out_line = r.stdout.split("DISPATCH_OUT:")[1].splitlines()[0]
        self.assertIn("error", out_line)
        self.assertIn("does-not-exist", out_line)

    # ------------------------------------------------------------------
    # Host-adapter delivery: acceptance invariants against the REAL
    # registry + REAL native send_message_tool.
    #
    # Core intentionally never registers send_message in the tool
    # registry (host-only). The plugin must NOT register one either:
    # the acceptance invariant is registry.get_entry("send_message")
    # is None. Delivery reaches the native rail by importing and
    # directly calling tools.send_message_tool.send_message_tool via
    # the plugin-local host adapter — no registry dispatch, no LLM
    # turn, no model-visible surface.
    # ------------------------------------------------------------------

    def _run_code(self, code: str):
        """Run a code string in a fresh hermes-agent subprocess with the
        temp HERMES_HOME (same pattern as _load_through_loader)."""
        child_env = dict(os.environ)
        return subprocess.run(
            [_hermes_python(), "-c", code],
            capture_output=True, text=True, timeout=120, cwd=str(REPO), env=child_env,
        )

    def test_send_message_is_not_registered_in_the_tool_registry(self):
        code = f"""
import os, sys, json
os.environ["HERMES_HOME"] = {str(self.tmp_home)!r}
sys.path.insert(0, {str(REPO)!r})
from hermes_cli.plugins import get_plugin_manager
mgr = get_plugin_manager()
mgr.discover_and_load(force=True)
from tools.registry import registry
# Acceptance invariant: the plugin registers NO send_message entry —
# core keeps that name host-only and the outbox worker reaches the
# native rail by importing and calling send_message_tool directly.
entry = registry.get_entry("send_message")
print("SEND_MESSAGE_ENTRY:", "None" if entry is None else "present")
# Control: the plugin's own tools must remain registered and visible.
visible = registry.get_definitions({{"pi_status"}}, quiet=True)
print("CONTROL:", json.dumps({{"pi_status_visible": len(visible) == 1,
                               "pi_tool_count": len([n for n in
                                   registry.get_all_tool_names() if n.startswith('pi_')])}}))
"""
        r = self._run_code(code)
        self.assertEqual(r.returncode, 0, f"subprocess failed:\n{r.stderr}\n{r.stdout}")
        self.assertIn("SEND_MESSAGE_ENTRY: None", r.stdout,
                      f"acceptance invariant violated — send_message must not "
                      f"be registered in the ToolRegistry:\n{r.stdout}")
        import json as _json
        control = _json.loads(r.stdout.split("CONTROL:")[1].splitlines()[0])
        self.assertTrue(control["pi_status_visible"],
                        "pi_status must remain registered and visible")
        self.assertEqual(control["pi_tool_count"], 7,
                         "the seven pi_* tools must be registered")

    def test_successful_outbox_delivery_through_real_host_adapter_rail(self):
        """Full rail with the real registry and the real native tool:
        outbox row -> OutboxWorker (default deliver = plugin-local host
        adapter) -> real tools.send_message_tool.send_message_tool
        (only the leaf platform API call is stubbed) -> mark_sent. Zero
        LLM turns and zero send_message ToolRegistry entries anywhere."""
        code = f"""
import os, sys, json, time
os.environ["HERMES_HOME"] = {str(self.tmp_home)!r}
sys.path.insert(0, {str(REPO)!r})
from hermes_cli.plugins import get_plugin_manager
mgr = get_plugin_manager()
mgr.discover_and_load(force=True)
from tools.registry import registry
assert registry.get_entry("send_message") is None, \
    "send_message must not be registered in the ToolRegistry"
# Stub ONLY the leaf platform send: target resolution, redaction and the
# native send_message_tool itself all run for real; the outbox worker's
# host adapter imports and directly calls it.
import tools.send_message_tool as sst
sent = []
def fake_handle_send(args):
    sent.append(dict(args))
    return json.dumps({{"success": True, "platform": "telegram"}})
sst._handle_send = fake_handle_send
sys.path.insert(0, {str(self.tmp_home / "plugins" / "pi-manager")!r})
import registry_db
import outbox
reg = registry_db.Registry(db_path=registry_db.default_db_path())
reg.create_task("t-it-1", origin=json.dumps(
    {{"platform": "telegram", "chat_id": "12345", "thread_id": "77"}}))
nb = outbox.NotificationOutbox(reg)
nid = nb.enqueue("t-it-1", "settled", "pi task settled: PASS")
assert nid, "enqueue returned None (row not created)"
# The production shape: no ctx, no injected deliver — the default
# plugin-local host adapter, which resolves the real native tool.
w = outbox.OutboxWorker(nb)
row = None
deadline = time.time() + 10
while time.time() < deadline:
    row = reg.get_notification(nid)
    if row["status"] in ("sent", "failed"):
        break
    w.run_once()  # the plugin's own worker thread may claim the row first
    time.sleep(0.05)
row = reg.get_notification(nid)
assert registry.get_entry("send_message") is None, \
    "send_message must still not be registered after delivery"
print("DELIVERY:", json.dumps({{"notification_id": nid,
                                "status": row["status"],
                                "sent_at_set": row["sent_at"] is not None,
                                "attempts": row["attempts"],
                                "last_error": row["last_error"],
                                "host_calls": len(sent),
                                "host_args": sent[0] if sent else None}}))
"""
        r = self._run_code(code)
        self.assertEqual(r.returncode, 0, f"subprocess failed:\n{r.stderr}\n{r.stdout}")
        data = json.loads(r.stdout.split("DELIVERY:")[1].splitlines()[0])
        # The acceptance success case: the row is delivered and marked sent
        # through the real host adapter rail — no registry entry, no
        # dispatch, no LLM turn.
        self.assertEqual(data["status"], "sent", f"delivery failed: {data}")
        self.assertTrue(data["sent_at_set"])
        self.assertGreaterEqual(data["host_calls"], 1)
        self.assertEqual(data["host_args"], {
            "action": "send",
            "target": "telegram:12345:77",
            "message": "pi task settled: PASS",
        })
        self.assertNotIn("Unknown tool", str(data))


if __name__ == "__main__":
    unittest.main(verbosity=2)
