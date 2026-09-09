"""Bounded, disposable Desktop view of Pi RPC events; never task authority.

The RPC reader only updates memory. One coalescing writer publishes private,
atomic snapshots for the separate ``hermes serve`` process. No model calls,
notifications, registry writes or execution decisions live here.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import closing
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import threading
import time

LOG = logging.getLogger(__name__)
TEXT_LIMIT = 8192
HISTORY_LIMIT = 8
MAX_BYTES = 131072
MAX_TASKS = 32
RETENTION_SECONDS = 7 * 86400
_CONTROL = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|[\x00-\x08\x0b-\x1f\x7f]")


def clipped(value, limit=TEXT_LIMIT):
    text = _CONTROL.sub("", value if isinstance(value, str) else "")
    return text if len(text) <= limit else "[… wcześniejszy tekst pominięty …]\n" + text[-limit:]


def text_content(message):
    content = message.get("content", []) if isinstance(message, dict) else []
    if isinstance(content, str):
        return clipped(content)
    return clipped("\n".join(clipped(item.get("text")) for item in content
                             if isinstance(item, dict) and item.get("type") == "text"))


def snapshot_path(directory, task_id):
    return Path(directory) / (hashlib.sha256(task_id.encode()).hexdigest() + ".json")


class Projection:
    """Pi 0.85 RPC: text deltas append; partial tool results are cumulative."""
    def __init__(self, task_id):
        self.data = {"schema": 1, "task_id": task_id, "seq": 0, "updated_at": None,
                     "text": "", "tool": None, "entries": [], "tools_completed": 0}
        self.blocks = {}
        self.completed = set()

    def _remember(self, entry):
        if entry.get("text") or entry.get("kind") == "tool":
            self.data["entries"] = (self.data["entries"] + [
                {**entry, "text": clipped(entry.get("text"), 1024)}])[-HISTORY_LIMIT:]

    def apply(self, event, now):
        kind = event.get("type")
        if kind == "message_start" and event.get("message", {}).get("role") == "assistant":
            self.blocks = {}
            self.data["text"] = ""
        elif kind == "message_update":
            update = event.get("assistantMessageEvent") or {}
            index = str(update.get("contentIndex", 0))[:12]
            if update.get("type") not in ("text_start", "text_delta", "text_end"):
                return False  # Thinking and tool arguments are not the display stream.
            if len(self.blocks) >= 16 and index not in self.blocks:
                return False
            if update["type"] == "text_start":
                self.blocks[index] = ""
            elif update["type"] == "text_delta":
                self.blocks[index] = clipped(self.blocks.get(index, "") + clipped(update.get("delta")))
            else:
                self.blocks[index] = clipped(update.get("content"))
            self.data["text"] = clipped("\n".join(self.blocks.values()))
        elif kind == "message_end" and event.get("message", {}).get("role") == "assistant":
            self.data["text"] = text_content(event["message"])
            self._remember({"kind": "assistant", "text": self.data["text"]})
        elif kind == "tool_execution_start":
            self.data["tool"] = {"id": clipped(event.get("toolCallId"), 128),
                                 "name": clipped(event.get("toolName"), 80), "text": ""}
        elif kind in ("tool_execution_update", "tool_execution_end"):
            tool_id = clipped(event.get("toolCallId"), 128)
            tool = self.data["tool"]
            if tool is None or tool["id"] != tool_id:
                return False  # A late event must not replace a newer active tool.
            result = event.get("partialResult" if kind.endswith("update") else "result", {})
            tool["text"] = text_content(result)  # Replace; Pi sends cumulative output.
            if kind.endswith("end"):
                if tool_id not in self.completed:
                    self.data["tools_completed"] += 1
                    self.completed.add(tool_id)
                    # Only recent IDs are needed to suppress retransmission.
                    if len(self.completed) > 256:
                        self.completed = {tool_id}
                    self._remember({**tool, "kind": "tool", "error": bool(event.get("isError"))})
                self.data["tool"] = None
        elif kind not in ("agent_start", "agent_end", "agent_settled"):
            return False
        self.data["seq"] += 1
        self.data["updated_at"] = now
        return True


class ActivityRecorder:
    def __init__(self, directory, *, interval=0.5, redact=lambda text: text):
        self.directory = Path(directory)
        self.interval, self.redact = interval, redact
        self._lock = threading.Lock()
        self._tasks = OrderedDict()
        self._dirty = set()
        self._stop = threading.Event()
        self._thread = None

    def observe(self, task_id, event):
        with self._lock:
            if self._stop.is_set():
                return
            if task_id not in self._tasks:
                if len(self._tasks) >= MAX_TASKS:
                    old, _ = self._tasks.popitem(last=False)
                    self._dirty.discard(old)
                self._tasks[task_id] = Projection(task_id)
            self._tasks.move_to_end(task_id)
            if self._tasks[task_id].apply(event, time.time()):
                self._dirty.add(task_id)
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name="pi-activity", daemon=True)
                self._thread.start()

    def _run(self):
        while not self._stop.wait(self.interval):
            self.flush()

    def flush(self):
        with self._lock:
            pending = [(key, json.loads(json.dumps(self._tasks[key].data))) for key in self._dirty]
            self._dirty.clear()
        for task_id, data in pending:
            tmp = None
            try:
                data["text"] = self.redact(data["text"])
                if data["tool"]:
                    data["tool"]["text"] = self.redact(data["tool"]["text"])
                for entry in data["entries"]:
                    entry["text"] = self.redact(entry["text"])
                encoded = json.dumps(data, ensure_ascii=False).encode()
                if len(encoded) > MAX_BYTES:
                    raise ValueError("activity snapshot exceeds display bound")
                self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                with tempfile.NamedTemporaryFile(dir=self.directory, prefix=".activity-", delete=False) as stream:
                    tmp = Path(stream.name)  # mkstemp creates 0600, including after replacement.
                    stream.write(encoded)
                os.replace(tmp, snapshot_path(self.directory, task_id))
            except Exception:
                LOG.warning("Pi live view could not publish a snapshot", exc_info=True)
            finally:
                if tmp is not None:
                    tmp.unlink(missing_ok=True)
        if pending:
            self._prune()

    def _prune(self):
        try:
            cutoff = time.time() - RETENTION_SECONDS
            files = sorted(self.directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            for index, path in enumerate(files):
                if re.fullmatch(r"[a-f0-9]{64}\.json", path.name) and (
                        index >= 256 or path.stat().st_mtime < cutoff):
                    path.unlink(missing_ok=True)
        except OSError:
            pass

    def close(self):
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
        self.flush()


def _readonly(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def same_conversation(home, origin_id, requested_id):
    if origin_id and origin_id == requested_id:
        return True
    # Only follow published compression continuations, never branches/delegates.
    try:
        with closing(_readonly(Path(home) / "state.db")) as conn:
            current = origin_id
            for _ in range(64):
                row = conn.execute("SELECT ended_at, end_reason FROM sessions WHERE id=?", (current,)).fetchone()
                if not row or row["ended_at"] is None or row["end_reason"] != "compression":
                    return False
                children = conn.execute(
                    "SELECT id FROM sessions WHERE parent_session_id=? AND COALESCE(source,'')!='tool' "
                    "AND COALESCE(json_extract(COALESCE(model_config,'{}'),'$._branched_from'),'')!=? "
                    "AND COALESCE(json_extract(COALESCE(model_config,'{}'),'$._delegate_from'),'')!=? LIMIT 2",
                    (current, current, current)).fetchall()
                if len(children) != 1:
                    return False
                current = children[0]["id"]
                if current == requested_id:
                    return True
    except (OSError, sqlite3.Error):
        pass
    return False


def read_activity(home, task_id, session_id):
    """Read-only, session-scoped view; absent and foreign tasks look identical."""
    home = Path(home).resolve()
    directory = home / "state" / "pi-manager"
    with closing(_readonly(directory / "registry.sqlite3")) as conn:
        row = conn.execute(
            "SELECT task_id, origin, execution_state, verification_state, active_tool, "
            "started_at, last_event_at, wake_state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None:
        raise KeyError(task_id)
    origin = json.loads(row["origin"] or "{}")
    parent = origin.get("session_id") or origin.get("session_key")
    if (not (origin.get("ui_session_id") or origin.get("source") in ("desktop", "tui"))
            or not parent or not same_conversation(home, parent, session_id)
            or (origin.get("hermes_home") and Path(origin["hermes_home"]).resolve() != home)):
        raise KeyError(task_id)
    result = {key: row[key] for key in row.keys() if key != "origin"}
    result.update(session_id=session_id, activity=None)
    try:
        path = snapshot_path(directory / "activity", task_id)
        with path.open("rb") as stream:
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) <= MAX_BYTES:
            data = json.loads(raw)
            if data.get("schema") == 1 and data.get("task_id") == task_id:
                result["activity"] = data
    except (OSError, ValueError, TypeError):
        pass
    return result
