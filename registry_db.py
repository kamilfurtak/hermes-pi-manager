"""SQLite-backed durable registry for the pi-manager plugin.

Three tables:

- ``tasks``  — one row per task, holding the mutable current state. Fields
  match the spec in ``tasks/2026-08-26-engineering-pi-rpc-monitoring``.
  The ``origin`` column holds the routing snapshot captured at dispatch
  time (serializable JSON, never a context object).
- ``events`` — append-only diagnostic trace. Never updated, only inserted.
- ``notifications`` — the plugin's durable delivery outbox. Rows move
  pending -> leased -> sent (or failed); a stale lease is requeued by the
  next claim, so a crash mid-send never loses a notification.

The database lives at ``$HERMES_HOME/state/pi-manager/registry.sqlite3`` by
default (overridable for tests). WAL mode is enabled so a concurrent reader
(e.g. ``pi_status`` from another process) never blocks a writer.

Payload summaries are bounded (``MAX_SUMMARY_CHARS``) — this module never
persists full message text or a token firehose, only short diagnostic
strings (event type, tool name, truncated head of any text field).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

MAX_SUMMARY_CHARS = 500
MAX_ERROR_CHARS = 2000

# Event types that are volume rather than evidence: each one says "the agent
# is still producing output", which tasks.last_event_at already records to
# the second. On 2026-08-28 they were 94% of a 46 MB registry — 315 195
# message_update rows out of 336 353 — and nothing had ever deleted one.
# Every type NOT listed here is structural (state transitions, verifier
# verdicts, aborts, recovery, identity) and is kept for the life of the
# registry.
HIGH_VOLUME_EVENTS = ("message_update", "tool_execution_update",
                      "diagnostic_snapshot", "queue_update")

# Retention for the types above. Nothing is dropped until it is older than any
# plausible live task, so a task still being diagnosed keeps its full trace.
# Two hours clears the longest real delegations by a wide margin — the slowest
# measured are 49 min and 27.5 min — while the 6 h window this started at left
# ~113 000 rows of chatter standing in steady state.
PRUNE_MIN_AGE_SECONDS = 2 * 3600

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    pid                 INTEGER,
    session_id          TEXT,
    session_file        TEXT,
    expected_session_id TEXT,
    expected_session_file TEXT,
    cwd                 TEXT,
    started_at          REAL,
    last_event_at       REAL,
    last_state_at       REAL,
    last_progress_at    REAL,
    last_state_changed_at REAL,
    last_event_type     TEXT,
    last_state_hash     TEXT,
    last_entry_id       TEXT,
    execution_state     TEXT NOT NULL,
    verification_state  TEXT NOT NULL DEFAULT 'NOT_RUN',
    active_tool         TEXT,
    active_request_id   TEXT,
    message_count       INTEGER,
    is_streaming        INTEGER,
    is_compacting       INTEGER,
    settled_at          REAL,
    exit_code           INTEGER,
    last_error          TEXT,
    stderr_tail         TEXT,
    verifier_spec       TEXT,
    abort_requested      INTEGER NOT NULL DEFAULT 0,
    origin              TEXT,
    continuation_enabled INTEGER NOT NULL DEFAULT 0,
    wake_state          TEXT,
    wake_attempts       INTEGER NOT NULL DEFAULT 0,
    wake_requested_at   REAL,
    wake_accepted_at    REAL,
    wake_last_error     TEXT,
    created_at          REAL,
    updated_at          REAL
);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    kind            TEXT NOT NULL,
    target          TEXT,
    platform        TEXT,
    chat_id         TEXT,
    thread_id       TEXT,
    session_key     TEXT,
    scope_id        TEXT,
    message         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_retry_at   REAL,
    lease_until     REAL,
    worker_id       TEXT,
    created_at      REAL NOT NULL,
    sent_at         REAL,
    last_error      TEXT
);

CREATE INDEX IF NOT EXISTS idx_notifications_dispatch ON notifications(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_notifications_task ON notifications(task_id);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    ts          REAL NOT NULL,
    source      TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    state_before TEXT,
    state_after  TEXT,
    summary     TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events(event_type, ts);
"""

TASK_FIELDS = [
    "task_id", "pid", "session_id", "session_file", "expected_session_id",
    "expected_session_file", "cwd", "started_at", "last_event_at",
    "last_state_at", "last_progress_at", "last_state_changed_at",
    "last_event_type", "last_state_hash", "last_entry_id",
    "execution_state", "verification_state", "active_tool",
    "active_request_id", "message_count", "is_streaming", "is_compacting",
    "settled_at", "exit_code", "last_error", "stderr_tail", "verifier_spec",
    "abort_requested", "origin", "continuation_enabled", "wake_state",
    "wake_attempts", "wake_requested_at", "wake_accepted_at", "wake_last_error",
    "created_at", "updated_at",
]

# Columns added to the tasks table after the first schema revision, with the
# DDL used to extend databases created before they existed. A missing column
# is added with ALTER TABLE at open time — existing rows simply get the column
# default (NULL for the wake state fields, 0 for the two NOT-NULL flags), so
# pre-existing tasks stay wake-disabled unless the new code says otherwise.
#   origin — routing snapshot captured at dispatch (notification outbox)
#   continuation_enabled — 1 when the task was dispatched from a gateway/
#       Telegram session carrying a valid origin.session_key; its terminal
#       outcome may resume the original orchestrator session exactly once.
#   wake_state ... wake_last_error — the one logical terminal wake per task.
#       The state machine deliberately lives on the task row (no second
#       outbox table): NULL -> 'pending' -> 'dispatching' -> 'accepted' | 'exhausted'
#       (bounded retries loop through 'pending'), plus 'disabled' (task not
#       eligible) and 'uncertain' (a 'dispatching' row that survived a
#       process death; the gateway may have accepted the injection, so it is
#       NEVER retried — a duplicate orchestrator turn is the failure this
#       state exists to prevent). wake_requested_at doubles as the next-retry
#       deadline.
_TASKS_MIGRATIONS = {
    "origin": "TEXT",
    "continuation_enabled": "INTEGER NOT NULL DEFAULT 0",
    "wake_state": "TEXT",
    "wake_attempts": "INTEGER NOT NULL DEFAULT 0",
    "wake_requested_at": "REAL",
    "wake_accepted_at": "REAL",
    "wake_last_error": "TEXT",
    # Compact terminal verdict, written once when the task reaches its
    # terminal state. Both feed format_terminal_wake_message so the ONE
    # continuation wake can carry the outcome instead of telling the
    # orchestrator to go and fetch it — that follow-up pi_digest call was
    # a guaranteed extra turn on every completed task, including the ones
    # where nothing needed looking at.
    "verifier_summary": "TEXT",
    # NULL means "no LSP verdict" (disabled, non-git, no server, nothing
    # touched) and is NOT the same as a clean scan, which stores text.
    "lsp_summary": "TEXT",
}


def bound(text: Optional[str], limit: int = MAX_SUMMARY_CHARS) -> Optional[str]:
    """Truncate a diagnostic string; never persist unbounded text."""
    if text is None:
        return None
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + f"...(+{len(text) - limit} chars truncated)"


def default_db_path(hermes_home: Optional[str] = None) -> Path:
    home = hermes_home or os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
    return Path(home).expanduser() / "state" / "pi-manager" / "registry.sqlite3"


class Registry:
    """Thread-safe wrapper around the SQLite registry.

    One connection per Registry instance, guarded by a re-entrant lock —
    sqlite3 connections are not safe for concurrent use from multiple
    threads without external serialization, and this manager talks to the
    DB from the watchdog thread, the event reader thread, and the tool
    handler thread simultaneously.
    """

    def __init__(self, db_path: Optional[os.PathLike] = None, hermes_home: Optional[str] = None):
        self.path = Path(db_path) if db_path else default_db_path(hermes_home)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        # Teardown state (see close()): every DB operation checks this
        # INSIDE the same lock it holds for the execute, so a post-close
        # call is a no-op returning its safe default instead of raising
        # ``ProgrammingError: Cannot operate on a closed database`` from a
        # surviving daemon thread. The daemon threads (per-task boot and
        # event readers, watchdog, verifier, prune) are still in flight
        # when the owner closes — that is the expected case, not an error.
        self._closed = False
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.executescript(_SCHEMA)
            self._migrate_tasks()
            self._conn.commit()

    def _migrate_tasks(self) -> None:
        """Backward-compatible schema migration for pre-existing databases.

        ``CREATE TABLE IF NOT EXISTS`` never touches an existing table, so
        columns added after a registry was first created (e.g. ``origin``)
        are extended here with plain ALTER TABLE ADD COLUMN statements. No
        data is touched; old rows read the new columns as NULL."""
        existing = {
            r[1] for r in self._conn.execute("PRAGMA table_info(tasks)")
        }
        for column, decl in _TASKS_MIGRATIONS.items():
            if column not in existing:
                self._conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} {decl}")

    def close(self) -> None:
        """Idempotent teardown. After this, every public method is a
        no-op returning its safe default: the check and the execute share
        the same lock, so a call that acquires the lock first finishes on
        an open connection and one that acquires it later sees
        ``_closed`` and returns the default. Surviving daemon threads are
        the expected case, not an error."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._conn.close()
            except sqlite3.Error as exc:
                logger.debug("registry close: %s", exc)

    # -- tasks ---------------------------------------------------------

    def create_task(self, task_id: str, **fields: Any) -> None:
        now = time.time()
        row = {name: None for name in TASK_FIELDS}
        row.update(
            task_id=task_id,
            execution_state="STARTING",
            verification_state="NOT_RUN",
            abort_requested=0,
            # NOT-NULL wake columns always get their schema default here —
            # an explicit NULL insert would violate the constraint even
            # though the column has a DEFAULT (defaults apply only when the
            # column is omitted from the INSERT).
            continuation_enabled=0,
            wake_attempts=0,
            created_at=now,
            updated_at=now,
            started_at=now,
            last_event_at=now,
            last_state_at=now,
            last_progress_at=now,
            last_state_changed_at=now,
        )
        row.update(fields)
        cols = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                f"INSERT INTO tasks ({cols}) VALUES ({placeholders})",
                list(row.values()),
            )
            self._conn.commit()

    def update_task(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields = dict(fields)
        fields["updated_at"] = time.time()
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                f"UPDATE tasks SET {assignments} WHERE task_id = ?",
                list(fields.values()) + [task_id],
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._closed:
                return None
            cur = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def transition_task_state(
        self, task_id: str, new_state: str, changed_at: Optional[float] = None,
    ) -> bool:
        """Atomically set ``execution_state`` ONLY if the currently persisted
        state is not final (SETTLED/CRASHED/ABORTED); True iff applied.

        The RPC reader thread and the watchdog tick update the same row from
        different threads. A plain read-modify-write loses: the reader can
        snapshot the row while RUNNING, the tick can finalize it CRASHED, and
        the reader's write can resurrect it back to RUNNING (observed as a
        flaky crash test). The WHERE clause makes "a final state is never
        resurrected by a stale snapshot" a property of the single UPDATE,
        not of the (unwinnable) interleaving of two statements."""
        fields: Dict[str, Any] = {"execution_state": new_state}
        if changed_at is not None:
            fields["last_state_changed_at"] = changed_at
        fields["updated_at"] = time.time()
        assignments = ", ".join(f"{k} = ?" for k in fields)
        final_states = ("SETTLED", "CRASHED", "ABORTED")
        ph = ", ".join("?" for _ in final_states)
        with self._lock:
            if self._closed:
                return False
            cur = self._conn.execute(
                f"UPDATE tasks SET {assignments} "
                f"WHERE task_id = ? AND execution_state NOT IN ({ph})",
                [*fields.values(), task_id, *final_states],
            )
            self._conn.commit()
        return bool(cur.rowcount)

    def list_tasks(self, execution_states: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if self._closed:
                return []
            if execution_states:
                placeholders = ", ".join("?" for _ in execution_states)
                cur = self._conn.execute(
                    f"SELECT * FROM tasks WHERE execution_state IN ({placeholders})",
                    list(execution_states),
                )
            else:
                cur = self._conn.execute("SELECT * FROM tasks")
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    # -- events (append-only) -------------------------------------------

    def append_event(
        self,
        task_id: str,
        source: str,
        event_type: str,
        state_before: Optional[str] = None,
        state_after: Optional[str] = None,
        summary: Optional[Any] = None,
        ts: Optional[float] = None,
    ) -> None:
        if summary is not None and not isinstance(summary, str):
            try:
                summary = json.dumps(summary, ensure_ascii=False, default=str)
            except Exception:
                summary = str(summary)
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "INSERT INTO events (task_id, ts, source, event_type, state_before, "
                "state_after, summary) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id,
                    ts if ts is not None else time.time(),
                    source,
                    event_type,
                    state_before,
                    state_after,
                    bound(summary),
                ),
            )
            self._conn.commit()

    def recent_events(self, task_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            if self._closed:
                return []
            cur = self._conn.execute(
                "SELECT * FROM events WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                (task_id, limit),
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows][::-1]

    # -- notifications (plugin-owned delivery outbox) ---------------------
    #
    # The outbox is durable at-least-once with best-effort local dedupe by
    # notification_id (INSERT OR IGNORE). It never claims exactly-once: a
    # send may happen and its mark_sent may not, in which case the row can be
    # redelivered after lease expiry. Callers must treat the message as
    # idempotent for that reason.

    def insert_notification(self, row: Dict[str, Any]) -> bool:
        """INSERT OR IGNORE one outbox row; True iff a row was actually added
        (False = the stable notification_id was already present = deduped)."""
        cols = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        with self._lock:
            if self._closed:
                return False
            cur = self._conn.execute(
                f"INSERT OR IGNORE INTO notifications ({cols}) VALUES ({placeholders})",
                list(row.values()),
            )
            inserted = (cur.rowcount or 0) > 0
            self._conn.commit()
        return inserted

    def progress_seq(self, task_id: str) -> int:
        """Number of progress notifications already recorded for a task —
        the monotonic sequence number its next progress id must use. Reading
        and inserting are separate statements, so two concurrent enqueues
        can collide on the same id; INSERT OR IGNORE makes the loser a no-op,
        which is the intended best-effort local dedupe."""
        with self._lock:
            if self._closed:
                return 0
            cur = self._conn.execute(
                "SELECT COUNT(*) FROM notifications WHERE task_id = ? AND kind = 'progress'",
                (task_id,),
            )
            return int(cur.fetchone()[0])

    def requeue_expired_leases(self, now: float) -> int:
        """Rows still 'leased' past their lease deadline: their worker died
        (crash, restart, kill) between claim and mark_sent. They go back to
        pending so the next claim can pick them up. Returns the count."""
        with self._lock:
            if self._closed:
                return 0
            cur = self._conn.execute(
                "UPDATE notifications SET status = 'pending', lease_until = NULL, worker_id = NULL "
                "WHERE status = 'leased' AND lease_until IS NOT NULL AND lease_until <= ?",
                (now,),
            )
            n = cur.rowcount or 0
            self._conn.commit()
        return n

    def claim_notifications(
        self, now: float, worker_id: str, lease_seconds: float, limit: int = 8,
    ) -> List[Dict[str, Any]]:
        """Requeue expired leases, then lease up to ``limit`` due pending rows
        for this worker in one atomic step (attempts is incremented on
        claim). The claim and the UPDATE are inside one connection lock, so
        two workers in this process can never lease the same row."""
        with self._lock:
            if self._closed:
                return []
            self._conn.execute(
                "UPDATE notifications SET status = 'pending', lease_until = NULL, worker_id = NULL "
                "WHERE status = 'leased' AND lease_until IS NOT NULL AND lease_until <= ?",
                (now,),
            )
            cur = self._conn.execute(
                "SELECT notification_id FROM notifications "
                "WHERE status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?) "
                "ORDER BY created_at, notification_id LIMIT ?",
                (now, limit),
            )
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                ph = ", ".join("?" for _ in ids)
                self._conn.execute(
                    f"UPDATE notifications SET status = 'leased', worker_id = ?, "
                    f"lease_until = ?, attempts = attempts + 1 "
                    f"WHERE notification_id IN ({ph})",
                    [worker_id, now + lease_seconds, *ids],
                )
                rows = self._conn.execute(
                    f"SELECT * FROM notifications WHERE notification_id IN ({ph})", ids
                ).fetchall()
                result = [dict(r) for r in rows]
            else:
                result = []
            self._conn.commit()
        return result

    def set_notification_sent(self, notification_id: str, now: float) -> None:
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE notifications SET status = 'sent', sent_at = ?, "
                "lease_until = NULL, worker_id = NULL WHERE notification_id = ?",
                (now, notification_id),
            )
            self._conn.commit()

    def set_notification_retry(
        self, notification_id: str, next_retry_at: float, last_error: str,
    ) -> None:
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE notifications SET status = 'pending', next_retry_at = ?, "
                "lease_until = NULL, worker_id = NULL, last_error = ? "
                "WHERE notification_id = ?",
                (next_retry_at, last_error, notification_id),
            )
            self._conn.commit()

    def fail_notification(self, notification_id: str, last_error: str) -> None:
        """Terminal delivery failure: no further retries (permanent error or
        the bounded retry budget is exhausted). The row stays for audit."""
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE notifications SET status = 'failed', next_retry_at = NULL, "
                "lease_until = NULL, worker_id = NULL, last_error = ? "
                "WHERE notification_id = ?",
                (last_error, notification_id),
            )
            self._conn.commit()

    def get_notification(self, notification_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._closed:
                return None
            cur = self._conn.execute(
                "SELECT * FROM notifications WHERE notification_id = ?", (notification_id,)
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def list_notifications(
        self, task_id: Optional[str] = None, status: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)} " if clauses else ""
        with self._lock:
            if self._closed:
                return []
            cur = self._conn.execute(
                f"SELECT * FROM notifications {where}ORDER BY created_at, notification_id LIMIT ?",
                [*params, limit],
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    # -- terminal continuation wake (one per task, on the task row) ------
    #
    # See _TASKS_MIGRATIONS for the state machine. Every transition below is
    # a CAS guarded on the current wake_state, so the "at most one wake per
    # task, dispatched at most once per attempt" guarantees hold across
    # concurrent workers and process deaths.
    #
    # Clock convention: the deadline fields (wake_requested_at, the
    # wake_accepted_at marker) use the caller's injectable clock (tests run
    # these on a FakeClock); updated_at stays the wall-clock convention
    # every other registry write follows — a wake transition must never
    # move updated_at backwards relative to real time.

    def begin_terminal_wake(self, task_id: str, now: float) -> str:
        """CAS the task's wake state from NULL to 'pending' (continuation
        enabled) or 'disabled' (not eligible). Returns the new state, or the
        pre-existing one ('' when the task row is gone). Idempotent by
        construction: a second terminal observation of the same task can
        never open a second wake."""
        wall = time.time()
        with self._lock:
            if self._closed:
                return ""
            cur = self._conn.execute(
                "UPDATE tasks SET wake_state = 'pending', wake_requested_at = ?, "
                "updated_at = ? WHERE task_id = ? AND wake_state IS NULL "
                "AND continuation_enabled = 1",
                (now, wall, task_id),
            )
            if cur.rowcount:
                self._conn.commit()
                return "pending"
            cur = self._conn.execute(
                "UPDATE tasks SET wake_state = 'disabled', updated_at = ? "
                "WHERE task_id = ? AND wake_state IS NULL",
                (wall, task_id),
            )
            self._conn.commit()
            if cur.rowcount:
                return "disabled"
            cur = self._conn.execute(
                "SELECT wake_state FROM tasks WHERE task_id = ?", (task_id,))
            row = cur.fetchone()
            return (row[0] or "") if row else ""

    def list_wake_pending(self, now: float, limit: int = 8) -> List[Dict[str, Any]]:
        """Due 'pending' wakes (wake_requested_at doubles as next-retry-at)."""
        with self._lock:
            if self._closed:
                return []
            cur = self._conn.execute(
                "SELECT * FROM tasks WHERE wake_state = 'pending' "
                "AND continuation_enabled = 1 "
                "AND (wake_requested_at IS NULL OR wake_requested_at <= ?) "
                "ORDER BY wake_requested_at, task_id LIMIT ?",
                (now, limit),
            )
            return [dict(r) for r in cur.fetchall()]

    def claim_terminal_wake(self, task_id: str, now: float) -> bool:
        """CAS 'pending' -> 'dispatching', incrementing wake_attempts.
        False when another claimant moved the row first."""
        with self._lock:
            if self._closed:
                return False
            cur = self._conn.execute(
                "UPDATE tasks SET wake_state = 'dispatching', "
                "wake_attempts = wake_attempts + 1, updated_at = ? "
                "WHERE task_id = ? AND wake_state = 'pending'",
                (time.time(), task_id),
            )
            self._conn.commit()
            return bool(cur.rowcount)

    def mark_wake_accepted(self, task_id: str, now: float) -> None:
        """The gateway accepted the injection: terminal, no further action."""
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE tasks SET wake_state = 'accepted', wake_accepted_at = ?, "
                "wake_last_error = NULL, updated_at = ? WHERE task_id = ?",
                (now, time.time(), task_id),
            )
            self._conn.commit()

    def mark_wake_retry(
        self, task_id: str, next_retry_at: float, last_error: str, now: float,
    ) -> None:
        """Back a failed dispatch out to 'pending' with a new deadline."""
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE tasks SET wake_state = 'pending', wake_requested_at = ?, "
                "wake_last_error = ?, updated_at = ? "
                "WHERE task_id = ? AND wake_state = 'dispatching'",
                (next_retry_at, bound(last_error, MAX_ERROR_CHARS),
                 time.time(), task_id),
            )
            self._conn.commit()

    def mark_wake_exhausted(self, task_id: str, last_error: str, now: float) -> None:
        """Terminal dispatch failure: the bounded retry budget is spent. The
        row stays for audit and is never dispatched again."""
        with self._lock:
            if self._closed:
                return
            self._conn.execute(
                "UPDATE tasks SET wake_state = 'exhausted', wake_last_error = ?, "
                "updated_at = ? WHERE task_id = ? AND wake_state = 'dispatching'",
                (bound(last_error, MAX_ERROR_CHARS), time.time(), task_id),
            )
            self._conn.commit()

    def settle_stale_wake_dispatching(self, now: float) -> List[str]:
        """Called at a fresh plugin (re)load: every 'dispatching' row belongs
        to a dead process (it claimed the wake and died before recording the
        outcome). The gateway may already have accepted that injection, so
        each row is marked 'uncertain' and is NEVER dispatched again — a
        duplicate orchestrator turn is worse than a missed wake. Returns the
        affected task ids (for the audit events)."""
        with self._lock:
            if self._closed:
                return []
            ids = [r[0] for r in self._conn.execute(
                "SELECT task_id FROM tasks WHERE wake_state = 'dispatching'"
            ).fetchall()]
            if ids:
                self._conn.execute(
                    "UPDATE tasks SET wake_state = 'uncertain', wake_last_error = ?, "
                    "updated_at = ? WHERE wake_state = 'dispatching'",
                    (bound("worker died between claim and acceptance; not retried "
                           "(the gateway may have already accepted the injection)"),
                     time.time()),
                )
                self._conn.commit()
        return ids

    # -- retention -------------------------------------------------------

    def prune_events(
        self,
        min_age_seconds: float = PRUNE_MIN_AGE_SECONDS,
        now: Optional[float] = None,
    ) -> int:
        """Drop high-volume rows older than the window; return how many.

        Structural events are never touched, so the audit trail this plugin
        exists to provide — what state a task went through, what the gate said,
        what was aborted or recovered — stays complete forever. Only the "still
        typing" chatter is bounded, and only once it is older than any task
        still being diagnosed.

        Deliberately a plain range delete over idx_events_type_ts. An earlier
        version also kept the newest N rows per task, which needed a window
        function and a NOT IN: it took 410-656 ms against 133 000 rows *while
        deleting nothing*, all of it holding the lock every other task's event
        writes need. Those N rows bought no diagnostic value the structural
        events did not already carry.
        """
        cutoff = (now if now is not None else time.time()) - min_age_seconds
        placeholders = ", ".join("?" for _ in HIGH_VOLUME_EVENTS)
        with self._lock:
            if self._closed:
                return 0
            cur = self._conn.execute(
                f"DELETE FROM events WHERE event_type IN ({placeholders}) AND ts < ?",
                (*HIGH_VOLUME_EVENTS, cutoff),
            )
            deleted = cur.rowcount or 0
            self._conn.commit()
            if deleted:
                # Without this the WAL keeps every deleted page and the disk
                # footprint grows even as rows go away.
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                except sqlite3.Error:
                    pass
        return deleted
