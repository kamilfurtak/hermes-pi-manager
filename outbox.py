"""Durable, plugin-owned notification outbox for pi-manager.

Replaces the old passive delivery path, which pushed events onto a private
internal Hermes queue disguised as a framework-internal delegation event.
A task's routing
origin is captured at dispatch time, persisted with the task in the registry,
and every notification — progress and terminal — is a durable row in the
``notifications`` table. A notifier worker drains it by calling the
plugin-local host adapter (``host_adapter.deliver_send_message``) directly,
and that adapter imports and directly calls the Hermes-native
tools.send_message_tool.send_message_tool: no Telegram Bot API calls, no
message-injection into sessions, no private completion queues, and no agent
turn. ``send_message`` is deliberately NOT registered in the ToolRegistry
(core keeps that name host-only); delivery reaches the native rail by
calling its implementation, not through the registry.

Delivery semantics are durable at-least-once with best-effort local dedupe:

- stable notification ids (deterministic per task/kind for terminal kinds,
  monotonic per task for progress) plus ``INSERT OR IGNORE`` stop local
  duplicate enqueues;
- a claim takes a lease (``lease_until``); if the worker dies before
  ``mark_sent``, the next claim requeues the row — a crash or a manager
  restart never loses a notification;
- a row whose send succeeded but whose ``mark_sent`` was lost after a crash
  may be delivered again. Callers must therefore treat messages as
  idempotent. This plugin deliberately does NOT claim exactly-once.

The module is stdlib-only and Hermes-free (it depends on ``registry_db``
and the plugin-local ``host_adapter`` only, both of which defer any Hermes
core import to delivery time): ``NotificationOutbox`` can be unit tested
with a temp SQLite file, and ``OutboxWorker`` can be driven with a fake
clock and a fake/stubbed deliver callable.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

try:  # pragma: no cover - normal path: loaded as a real package by Hermes
    from .registry_db import Registry, bound  # type: ignore
    from . import host_adapter  # type: ignore
except ImportError:  # pragma: no cover - standalone/test import (no package)
    from registry_db import Registry, bound  # type: ignore
    import host_adapter  # type: ignore

logger = logging.getLogger(__name__)

# -- statuses -------------------------------------------------------------

STATUS_PENDING = "pending"
STATUS_LEASED = "leased"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

TERMINAL_KINDS = ("settled", "verifier", "failed", "stalled")
PROGRESS_KIND = "progress"

# -- retry policy ----------------------------------------------------------

# Bounded backoff schedule applied after the 1st..5th failed attempt. After
# the 6th failure the row goes to the terminal 'failed' status (kept for
# audit, never retried). Failures stay observable either way: attempts and
# last_error are persisted on the row.
RETRY_BACKOFF_SECONDS = (5, 15, 60, 300, 900)
MAX_ATTEMPTS = 6

# Lease a claimed row for at least this long. Must be comfortably above one
# real send_message round-trip; expiry is what requeues a row whose worker
# died mid-send.
DEFAULT_LEASE_SECONDS = 30.0

# Bounded message body: one chat message, not a transcript.
MAX_MESSAGE_CHARS = 4000

DEFAULT_WORKER_INTERVAL_SECONDS = 5.0


# -- target construction ----------------------------------------------------

_PLATFORM_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_MAX_REF_CHARS = 200


def _safe_ref(value: Any) -> Optional[str]:
    """Validate one target component defensively.

    Rejects empty/oversized values and anything with whitespace or control
    characters — those would corrupt the 'platform:chat_id[:thread_id]'
    grammar or the JSONL/message stream around it. Colons are allowed inside
    chat ids (Matrix room ids are '!room:server'); the platform component is
    strictly simple so the first colon always separates it. Returns the
    stripped value or None.
    """
    if value is None:
        return None
    v = str(value).strip()
    if not v or len(v) > _MAX_REF_CHARS:
        return None
    if any(ch.isspace() or ord(ch) < 0x20 for ch in v):
        return None
    return v


def build_target(
    platform: Any, chat_id: Any, thread_id: Any,
) -> Optional[str]:
    """Build a send_message target: 'platform:chat_id' or
    'platform:chat_id:thread_id'. Platform-neutral by construction — nothing
    here is Telegram-specific. Returns None when the values cannot form a
    safe target (missing, malformed, or unbounded)."""
    p = _safe_ref(platform)
    if p is None or not _PLATFORM_RE.match(p.lower()):
        return None
    c = _safe_ref(chat_id)
    if c is None:
        return None
    target = f"{p.lower()}:{c}"
    if thread_id not in (None, ""):
        # A thread part was explicitly given: it must validate. Silently
        # dropping a malformed id would route the message to the wrong topic.
        t = _safe_ref(thread_id)
        if t is None or not re.match(r"^[^\s:]{1,64}$", t):
            # The thread component must be unambiguous; chat ids may already
            # contain colons, a thread id may not.
            return None
        target = f"{target}:{t}"
    return target if len(target) <= 300 else None


def terminal_notification_id(task_id: str, kind: str) -> str:
    """Deterministic id for a terminal notification: one stable id per
    (task, kind), so the same event observed twice (crash, restart,
    re-run of recovery) can never enqueue a second copy."""
    return f"term:{task_id}:{kind}"


def progress_notification_id(task_id: str, seq: int) -> str:
    """Monotonic, deterministic id for the seq-th progress notification of a
    task. The sequence is derived from the durable row count, so it stays
    monotonic across a manager restart."""
    return f"prog:{task_id}:{seq}"


def parse_origin(raw: Optional[str]) -> Dict[str, Any]:
    """Parse a persisted routing snapshot; never raises on bad data."""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


# -- error classification ----------------------------------------------------

# Signatures that say the send will never succeed as targeted: no point
# burning the retry budget. Matched case-insensitively against the tool's
# bounded error text.
_PERMANENT_ERROR_SIGNATURES = (
    "unknown platform",
    "no chat specified",
    "no home channel",
    "invalid target",
    "cannot resolve",
    "unresolvable",
)
# Signatures that say "try again later". Anything unrecognized is treated as
# transient by default: the retry budget is bounded, and missing a real
# completion notice is worse than one extra retry of a permanent failure.
_TRANSIENT_ERROR_SIGNATURES = (
    "429", "rate limit", "too many requests",
    "timeout", "timed out",
    "502", "503", "504", "service unavailable", "bad gateway", "gateway timeout",
    "try again", "temporary", "transient", "connection", "reset", "refused",
)


def classify_transient(error_text: str) -> bool:
    text = str(error_text or "").lower()
    if any(sig in text for sig in _PERMANENT_ERROR_SIGNATURES):
        return False
    return True  # transient signatures AND the unrecognized default


def extract_tool_error(result: Any) -> Optional[str]:
    """send_message (like Hermes tools generally) returns a JSON string on
    success and a JSON string with an 'error' member on failure. A non-JSON
    non-empty string is treated as an error: send_message's success shape is
    always JSON, so prose back is a refusal, not a delivery receipt."""
    if isinstance(result, dict):
        err = result.get("error")
        return bound(str(err), 500) if err else None
    if result is None or result == "":
        return None
    text = str(result)
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return bound(text, 500)
    if isinstance(data, dict):
        err = data.get("error")
        return bound(str(err), 500) if err else None
    return None


# -- outbox -----------------------------------------------------------------


class NotificationOutbox:
    """Enqueue/claim/mark operations for the durable notification outbox.

    Pure registry I/O — no delivery, no Hermes imports. ``now_fn`` is
    injectable for deterministic tests; it defaults to the wall clock, which
    is what production wants (retries are wall-clock deadlines, and mixing
    the fake watchdog clock in here would skew real delivery).
    """

    def __init__(self, registry: Registry, now_fn: Callable[[], float] = time.time) -> None:
        self.registry = registry
        self._now_fn = now_fn

    def _now(self) -> float:
        return self._now_fn()

    def enqueue(
        self, task_id: str, kind: str, message: str, now: Optional[float] = None,
    ) -> Optional[str]:
        """Durably enqueue one notification; returns the notification_id, or
        None when a row with the same stable id already exists (local dedupe).
        Never raises: an outbox failure must never disturb a task."""
        now = now if now is not None else self._now()
        try:
            row = self.registry.get_task(task_id) or {}
            origin = parse_origin(row.get("origin"))
            platform = origin.get("platform")
            chat_id = origin.get("chat_id")
            thread_id = origin.get("thread_id")
            target = build_target(platform, chat_id, thread_id)
            message = bound(str(message or ""), MAX_MESSAGE_CHARS)
            if kind == PROGRESS_KIND:
                nid = progress_notification_id(task_id, self.registry.progress_seq(task_id) + 1)
            else:
                nid = terminal_notification_id(task_id, kind)
            inserted = self.registry.insert_notification({
                "notification_id": nid,
                "task_id": task_id,
                "kind": kind,
                "target": target,
                "platform": _safe_ref(platform),
                "chat_id": _safe_ref(chat_id),
                "thread_id": _safe_ref(thread_id),
                "session_key": _safe_ref(origin.get("session_key")),
                "scope_id": _safe_ref(origin.get("scope_id")),
                "message": message,
                "status": STATUS_PENDING,
                "attempts": 0,
                "next_retry_at": None,
                "lease_until": None,
                "worker_id": None,
                "created_at": now,
                "sent_at": None,
                "last_error": None,
            })
            if inserted:
                try:
                    self.registry.append_event(
                        task_id, "outbox", "notification_enqueued", None, None,
                        {"notification_id": nid, "kind": kind, "target": target or ""},
                    )
                except Exception:  # the audit note must never fail the enqueue
                    pass
            return nid if inserted else None
        except Exception as exc:
            logger.warning("outbox enqueue failed for %s (%s): %s", task_id, kind, exc)
            return None

    def claim(
        self, worker_id: str, lease_seconds: float, limit: int = 8,
        now: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        now = now if now is not None else self._now()
        return self.registry.claim_notifications(now, worker_id, lease_seconds, limit)

    def mark_sent(self, notification_id: str, now: Optional[float] = None) -> None:
        self.registry.set_notification_sent(notification_id,
                                            now if now is not None else self._now())

    def mark_failed(
        self, row: Dict[str, Any], error: str, transient: bool,
        now: Optional[float] = None,
    ) -> None:
        """Record a delivery failure. Transient failures get the bounded
        backoff schedule; permanent ones (and the 6th attempt) go to the
        terminal 'failed' status. attempts was already incremented on claim."""
        now = now if now is not None else self._now()
        attempts = int(row.get("attempts") or 1)
        error = bound(str(error), 500) or "unknown error"
        if transient and attempts < MAX_ATTEMPTS:
            delay = RETRY_BACKOFF_SECONDS[min(attempts - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
            self.registry.set_notification_retry(
                row["notification_id"], now + delay, error,
            )
        else:
            self.registry.fail_notification(row["notification_id"], error)

    def stats(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for row in self.registry.list_notifications(limit=10_000):
            out[row["status"]] = out.get(row["status"], 0) + 1
        return out


# -- worker -----------------------------------------------------------------


class OutboxWorker:
    """Drains the outbox through the plugin-local host adapter.

    The ONLY side effect of delivery is one call of
    ``host_adapter.deliver_send_message({"action": "send", "target": ...,
    "message": ...})`` — which imports and directly calls the Hermes-native
    ``tools.send_message_tool.send_message_tool``. No LLM/agent turn is
    involved anywhere on this path, no PluginContext is involved (the
    worker never dispatches through the ToolRegistry), no other tool is
    ever dispatched, and ``send_message`` is never registered in the
    ToolRegistry. An explicit ``deliver`` callable may be injected (tests);
    the default is the real host adapter, resolved at construction so a
    test that stubs the native tool in sys.modules before building the
    worker is picked up.

    ``start()`` returns immediately (daemon thread, first loop pass is a
    cheap claim), so plugin registration never blocks on delivery.
    ``stop()`` is clean: it signals the loop and joins with a bounded
    timeout.
    """

    def __init__(
        self,
        outbox: NotificationOutbox,
        deliver: Optional[Callable[..., Any]] = None,
        interval_seconds: float = DEFAULT_WORKER_INTERVAL_SECONDS,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        max_per_tick: int = 8,
        now_fn: Callable[[], float] = time.time,
        worker_id: Optional[str] = None,
    ) -> None:
        self.outbox = outbox
        self._deliver = (
            deliver if deliver is not None
            else host_adapter.deliver_send_message
        )
        self.interval = float(interval_seconds)
        self.lease_seconds = float(lease_seconds)
        self.max_per_tick = int(max_per_tick)
        self._now_fn = now_fn
        self.worker_id = worker_id or f"pi-outbox-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        thread = threading.Thread(
            target=self._loop, name=f"pi-outbox-{self.worker_id}", daemon=True,
        )
        self._thread = thread
        thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the loop and join with a bounded timeout. Never blocks
        plugin unload indefinitely: if the worker is mid-send past the
        timeout it is a daemon thread and the row's lease will requeue it."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        self._thread = None

    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:  # a broken tick must never kill the worker
                logger.warning("pi-outbox worker tick failed: %s", exc)
            self._stop.wait(self.interval)

    # -- delivery ------------------------------------------------------------

    def run_once(self, now: Optional[float] = None) -> int:
        """One drain pass: requeue expired leases, claim due rows, deliver
        each. Public so tests can drive it deterministically. Returns the
        number of rows claimed."""
        now = now if now is not None else self._now_fn()
        rows = self.outbox.claim(self.worker_id, self.lease_seconds,
                                 self.max_per_tick, now=now)
        for row in rows:
            self._deliver_one(row, now)
        return len(rows)

    def _deliver_one(self, row: Dict[str, Any], now: float) -> None:
        nid = row["notification_id"]
        target = row.get("target")
        if not target:
            # Unroutable at enqueue time (e.g. a CLI task captured no
            # platform/chat). Permanent by definition — retrying changes
            # nothing — but the failure is persisted and queryable.
            self.outbox.mark_failed(
                row, "no routable target: task origin has no usable "
                     "platform/chat_id (dispatched outside a messaging session)",
                transient=False, now=now,
            )
            return
        try:
            result = self._deliver(
                {"action": "send", "target": target,
                 "message": row.get("message") or ""},
            )
        except Exception as exc:
            # A raised delivery (host tool unavailable, gateway down) is
            # treated as transient and bounded by the retry budget.
            self.outbox.mark_failed(row, f"host adapter raised: {exc}",
                                    transient=True, now=now)
            return
        error = extract_tool_error(result)
        if error is None:
            self.outbox.mark_sent(nid, now=now)
        else:
            self.outbox.mark_failed(row, error,
                                    transient=classify_transient(error), now=now)