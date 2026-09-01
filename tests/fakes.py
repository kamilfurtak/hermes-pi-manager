"""Deterministic fake Pi RPC process for the pi-manager test suite.

Implements the ``PopenLike`` surface (pid, stdin, stdout, stderr, poll,
terminate, kill, wait) purely in-process with queues and threading, so the
whole PiManager state machine can be driven without a real ``pi`` subprocess
or network/provider call.

Usage pattern in tests:

    proc = FakePiProcess()
    manager = PiManager(registry, popen_factory=lambda argv, cwd: proc, clock=fake_clock)
    result = manager.start_task(prompt="hi", cwd=tmp_dir)
    proc.wait_for_command("get_state")       # let the boot thread progress
    proc.emit({"type": "agent_start"})
    ...
    proc.emit({"type": "agent_settled"})
"""

from __future__ import annotations

import itertools
import json
import queue
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

_pid_counter = itertools.count(1000)


class _FakeStdin:
    def __init__(self, on_line: Callable[[str], None]):
        self._on_line = on_line
        self._buf = ""
        self.closed = False

    def write(self, text: str) -> int:
        if self.closed:
            raise BrokenPipeError("stdin closed")
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self._on_line(line)
        return len(text)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _QueueIterStream:
    """A stdout/stderr-like blocking line iterator backed by a queue.
    ``None`` is the close sentinel."""

    def __init__(self) -> None:
        self._q: "queue.Queue[Optional[str]]" = queue.Queue()

    def put_line(self, line: str) -> None:
        if not line.endswith("\n"):
            line = line + "\n"
        self._q.put(line)

    def close(self) -> None:
        self._q.put(None)

    def __iter__(self):
        return self

    def __next__(self) -> str:
        item = self._q.get()
        if item is None:
            raise StopIteration
        return item


class FakePiProcess:
    """A fake ``pi --mode rpc`` process.

    ``command_handler(process, obj)`` is called synchronously on every
    parsed JSONL line written to stdin; the default implementation replies
    to every command with ``success: True`` using ``self.responses`` /
    ``self.state`` / ``self.entries``, and can be overridden per test for
    failure injection (timeouts are simulated by simply never responding).
    """

    # Built-in default sessionFile for the unscripted get_state response.
    # A test that assigns its own state/sessionFile (any other value,
    # including None) is scripting its own response and the spawn-time
    # sync below must not clobber it.
    DEFAULT_SESSION_FILE = "/tmp/fake-session-1.jsonl"

    def __init__(self, command_handler: Optional[Callable[["FakePiProcess", Dict[str, Any]], None]] = None):
        self.pid = next(_pid_counter)
        self.stdout = _QueueIterStream()
        self.stderr = _QueueIterStream()
        self.stdin = _FakeStdin(self._on_stdin_line)
        self._returncode: Optional[int] = None
        self._lock = threading.Lock()
        self._exit_event = threading.Event()
        self.commands_received: List[Dict[str, Any]] = []
        self.command_handler = command_handler or self._default_handler
        self.state: Dict[str, Any] = {
            "sessionId": "sess-fake-1",
            "sessionFile": self.DEFAULT_SESSION_FILE,
            "isStreaming": False,
            "isCompacting": False,
            "messageCount": 1,
            "pendingMessageCount": 0,
        }
        self.entries: Dict[str, Any] = {"entries": [], "leafId": None}
        self.fail_get_state = False
        self.hang_commands: set = set()
        self.exit_after_command: Optional[str] = None
        self.terminate_is_effective = True

    # -- test-facing helpers ---------------------------------------------

    def sync_session_state_from_argv(self, argv: List[str]) -> None:
        """Emulate real Pi at spawn time: a process started with
        ``--session <path>`` reports exactly that path in get_state, since
        managed tasks always pass the pre-allocated session file. Skipped
        when a test has already scripted a sessionFile (any value other
        than the built-in default, including None)."""
        if self.state.get("sessionFile") != self.DEFAULT_SESSION_FILE:
            return
        if "--session" in argv:
            idx = argv.index("--session")
            if idx + 1 < len(argv):
                self.state["sessionFile"] = argv[idx + 1]

    def emit(self, obj: Dict[str, Any]) -> None:
        self.stdout.put_line(json.dumps(obj, ensure_ascii=False))

    def emit_raw(self, line: str) -> None:
        self.stdout.put_line(line)

    def finish(self, exit_code: int = 0) -> None:
        with self._lock:
            if self._returncode is not None:
                return
            self._returncode = exit_code
        self.stdout.close()
        self.stderr.close()
        self._exit_event.set()

    def wait_until_command(self, command: str, timeout: float = 5.0) -> bool:
        """Wait for an outgoing command whose real Pi RPC wire field is
        ``type`` (verified: Pi parses ``command.type``, not
        ``command.command``)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(c.get("type") == command for c in self.commands_received):
                return True
            time.sleep(0.01)
        return False

    # -- PopenLike surface -------------------------------------------------

    def poll(self) -> Optional[int]:
        return self._returncode

    def terminate(self) -> None:
        if self.terminate_is_effective:
            self.finish(-15)
        # else: simulate a process that ignores SIGTERM; only kill() ends it.

    def kill(self) -> None:
        self.finish(-9)

    def wait(self, timeout: Optional[float] = None) -> int:
        if self._exit_event.wait(timeout=timeout):
            return self._returncode if self._returncode is not None else 0
        raise subprocess.TimeoutExpired(cmd="fake-pi", timeout=timeout or 0)

    # -- internal -----------------------------------------------------

    def _on_stdin_line(self, line: str) -> None:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return
        self.commands_received.append(obj)
        self.command_handler(self, obj)

    def _default_handler(self, process: "FakePiProcess", obj: Dict[str, Any]) -> None:
        # Outgoing commands carry the real Pi wire field "type"; the
        # (unaffected) response direction still echoes it back as "command".
        command = obj.get("type")
        req_id = obj.get("id")
        if command in self.hang_commands:
            return  # simulate a timeout: never respond
        if command == "get_state" and self.fail_get_state:
            self.emit({"type": "response", "command": command, "id": req_id,
                       "success": False, "error": "simulated failure"})
        elif command == "get_state":
            self.emit({"type": "response", "command": command, "id": req_id,
                       "success": True, "data": dict(self.state)})
        elif command == "get_entries":
            self.emit({"type": "response", "command": command, "id": req_id,
                       "success": True, "data": dict(self.entries)})
        else:
            self.emit({"type": "response", "command": command, "id": req_id,
                       "success": True, "data": {}})
        if self.exit_after_command == command:
            threading.Timer(0.02, self.finish, args=(0,)).start()


def fake_popen_factory(process: FakePiProcess):
    def _spawn(argv, cwd):
        process.sync_session_state_from_argv(argv)
        return process
    return _spawn


class FakeClock:
    """Manually advanced monotonic-ish clock for deterministic watchdog tests."""

    def __init__(self, start: float = 1_000_000.0):
        self._t = start
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._t

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._t += seconds
