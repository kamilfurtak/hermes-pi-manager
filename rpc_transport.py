"""Pi RPC transport — one Popen, one JSONL reader thread, one command
correlation map, per task.

Real production processes are started with ``pi --mode rpc`` (verified via
``pi --help`` on the installed 0.84.2 binary: ``--mode <mode>`` accepts
``text``, ``json``, or ``rpc``). Tests inject a fake process object via
``popen_factory`` so the whole state machine can be exercised deterministically
without a real Pi/NInfer round trip.

Protocol (verified facts from the installed Pi 0.84.2 RPC source,
``dist/modes/rpc/rpc-mode.js`` / ``rpc-types.d.ts``):
  - stdin/stdout are JSONL.
  - Outgoing commands are parsed by Pi from ``command.type`` (NOT
    ``command.command``), e.g. ``{"type": "get_state", "id": ...}``,
    ``{"type": "prompt", "id": ..., "message": ...}``,
    ``{"type": "steer", "id": ..., "message": ...}``,
    ``{"type": "abort", "id": ...}``,
    ``{"type": "get_entries", "id": ..., "since": ...}``. This module
    assigns a bounded string ``id`` to every outgoing command for
    correlation.
  - Responses (the reverse direction, unaffected by the above) look like
    ``{"type": "response", "command": ..., "success": bool, "id": ...,
    "data": ..., "error": ...}``.
  - Events are any other JSON object read from stdout (``type`` is the
    event name, e.g. ``agent_settled``).
  - Malformed JSONL lines are skipped and recorded, never fatal.
"""

from __future__ import annotations

import itertools
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol

STDERR_TAIL_MAX_CHARS = 4000
STDERR_TAIL_MAX_LINES = 80


class PopenLike(Protocol):
    """Minimal surface this module needs from a process object.

    ``subprocess.Popen`` satisfies this natively; tests provide fakes.
    """

    pid: int
    stdin: Any
    stdout: Any
    stderr: Any

    def poll(self) -> Optional[int]: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def wait(self, timeout: Optional[float] = None) -> int: ...


def find_pi_binary() -> str:
    configured = os.environ.get("PI_BIN")
    candidates: List[str] = []
    if configured:
        candidates.append(configured)
    discovered = shutil.which("pi")
    if discovered:
        candidates.append(discovered)
    candidates.append("/opt/homebrew/bin/pi")
    for candidate in candidates:
        if candidate and Path(candidate).expanduser().is_file():
            return str(Path(candidate).expanduser())
    raise FileNotFoundError("pi binary was not found")


def default_popen_factory(argv: List[str], cwd: str) -> subprocess.Popen:
    """Real production process factory: pi in RPC mode."""
    return subprocess.Popen(
        argv,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def build_rpc_argv(
    pi_bin: str,
    *,
    provider: str,
    model: str,
    thinking: str,
    session_file: Optional[str] = None,
    session_id: Optional[str] = None,
    no_session: bool = False,
    system_prompt_file: Optional[str] = None,
) -> List[str]:
    argv = [pi_bin, "--mode", "rpc", "--provider", provider, "--model", model,
            "--thinking", thinking]
    if session_file:
        argv += ["--session", session_file]
    elif session_id:
        argv += ["--session-id", session_id]
    elif no_session:
        argv += ["--no-session"]
    if system_prompt_file:
        argv += ["--append-system-prompt", system_prompt_file]
    return argv


class RpcTimeoutError(TimeoutError):
    """An RPC command did not receive a response within the timeout."""


class RpcTransportClosed(RuntimeError):
    """The transport's process has already exited."""


class PiRpcTransport:
    """Wraps one Pi RPC process: JSONL send/receive with command correlation.

    ``on_event`` is invoked (from the reader thread) for every parsed line
    that is not itself a correlated command response. ``on_malformed`` is
    invoked for lines that fail to parse as JSON.
    """

    def __init__(
        self,
        process: PopenLike,
        on_event: Callable[[Dict[str, Any]], None],
        on_malformed: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.process = process
        self.pid = getattr(process, "pid", None)
        self._on_event = on_event
        self._on_malformed = on_malformed
        self._write_lock = threading.Lock()
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._pending_lock = threading.Lock()
        self._id_counter = itertools.count(1)
        self._closed = threading.Event()
        self._stderr_tail: List[str] = []
        self._stderr_lock = threading.Lock()

        self._reader_thread = threading.Thread(
            target=self._read_loop, name="pi-rpc-reader", daemon=True
        )
        self._reader_thread.start()
        if getattr(process, "stderr", None) is not None:
            self._stderr_thread = threading.Thread(
                target=self._drain_stderr, name="pi-rpc-stderr", daemon=True
            )
            self._stderr_thread.start()
        else:
            self._stderr_thread = None

    # -- outgoing --------------------------------------------------------

    def _next_id(self) -> str:
        return f"req-{next(self._id_counter)}-{uuid.uuid4().hex[:8]}"

    def send_command(
        self,
        command: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """Send a command and block for its correlated response."""
        if self.process.poll() is not None:
            raise RpcTransportClosed(f"process already exited (rc={self.process.poll()})")
        req_id = self._next_id()
        # Real Pi RPC parses outgoing commands from ``type``, not
        # ``command`` (verified against the installed 0.84.2 source) — do
        # not regress this back to ``{"command": ...}``.
        payload = {"type": command, "id": req_id}
        if params:
            payload.update(params)
        event = threading.Event()
        box: Dict[str, Any] = {}
        with self._pending_lock:
            self._pending[req_id] = {"event": event, "box": box}
        line = json.dumps(payload, ensure_ascii=False)
        try:
            with self._write_lock:
                self.process.stdin.write(line + "\n")
                self.process.stdin.flush()
        except (BrokenPipeError, ValueError, OSError) as exc:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise RpcTransportClosed(f"failed to write command {command!r}: {exc}") from exc

        if not event.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise RpcTimeoutError(f"command {command!r} timed out after {timeout}s")
        with self._pending_lock:
            self._pending.pop(req_id, None)
        response = box.get("response", {})
        if not response.get("success", False):
            raise RuntimeError(
                f"command {command!r} failed: {response.get('error')!r}"
            )
        return response.get("data") or {}

    def send_prompt(self, text: str) -> str:
        # Real Pi RPC expects the prompt text under "message", not "prompt".
        self.send_command("prompt", {"message": text}, timeout=5.0)
        return "accepted"

    def send_steer(self, text: str) -> str:
        self.send_command("steer", {"message": text}, timeout=5.0)
        return "accepted"

    def send_abort(self, timeout: float = 5.0) -> bool:
        try:
            self.send_command("abort", {}, timeout=timeout)
            return True
        except (RpcTimeoutError, RpcTransportClosed, RuntimeError):
            return False

    def get_state(self, timeout: float = 5.0) -> Dict[str, Any]:
        return self.send_command("get_state", {}, timeout=timeout)

    def get_entries(self, since: Optional[str] = None, timeout: float = 5.0) -> Dict[str, Any]:
        params = {"since": since} if since else {}
        return self.send_command("get_entries", params, timeout=timeout)

    # -- lifecycle ---------------------------------------------------------

    def terminate(self) -> None:
        try:
            self.process.terminate()
        except Exception:
            pass

    def kill(self) -> None:
        try:
            self.process.kill()
        except Exception:
            pass

    def poll(self) -> Optional[int]:
        return self.process.poll()

    def wait(self, timeout: Optional[float] = None) -> Optional[int]:
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def stderr_tail(self) -> str:
        with self._stderr_lock:
            return "\n".join(self._stderr_tail)[-STDERR_TAIL_MAX_CHARS:]

    # -- reader threads ------------------------------------------------

    def _drain_stderr(self) -> None:
        try:
            for line in self.process.stderr:
                with self._stderr_lock:
                    self._stderr_tail.append(line.rstrip("\n"))
                    if len(self._stderr_tail) > STDERR_TAIL_MAX_LINES:
                        self._stderr_tail = self._stderr_tail[-STDERR_TAIL_MAX_LINES:]
        except Exception:
            pass

    def _read_loop(self) -> None:
        try:
            for line in self.process.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    if self._on_malformed:
                        try:
                            self._on_malformed(line)
                        except Exception:
                            pass
                    continue
                if not isinstance(obj, dict):
                    if self._on_malformed:
                        try:
                            self._on_malformed(line)
                        except Exception:
                            pass
                    continue
                req_id = obj.get("id")
                if obj.get("type") == "response" and req_id is not None:
                    with self._pending_lock:
                        entry = self._pending.get(req_id)
                    if entry is not None:
                        entry["box"]["response"] = obj
                        entry["event"].set()
                        continue
                    # Unmatched response (late/duplicate) -> treat as event.
                try:
                    self._on_event(obj)
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            self._closed.set()
            # Wake up any commands still waiting — the process end (EOF)
            # means no more responses are coming.
            with self._pending_lock:
                pending = list(self._pending.values())
            for entry in pending:
                entry["event"].set()
