"""Pi activity and scoped controls in the host's native CLI subagent monitor.

All attachment is instance-local and reversible. No Hermes source or native
subagent registry is modified; native rows and controls retain their owner.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

try:
    from .activity import snapshot_path, load_snapshot, activity_summary
    from .live_transcript import transcript_path
except ImportError:
    from activity import snapshot_path, load_snapshot, activity_summary
    from live_transcript import transcript_path

FINAL = {"SETTLED", "CRASHED", "ABORTED"}


def plain(text):
    import unicodedata
    return ''.join(c for c in str(text or '') if c in '\n\t' or not unicodedata.category(c).startswith('C'))


def presentation(row, data, now):
    state = row['execution_state']
    verdict = row.get('verification_state') or 'NOT_RUN'
    if state == 'SETTLED':
        status = {'PASS': 'Gotowe · weryfikacja OK', 'FAIL': 'Błąd weryfikacji',
                  'UNRUNNABLE': 'Nie udało się zweryfikować', 'PENDING': 'Weryfikacja'}.get(verdict, 'Praca zakończona')
    elif state in FINAL:
        status = {'ABORTED': 'Przerwane'}.get(state, 'Błąd procesu')
    else:
        status = {'STARTING': 'Uruchamianie', 'STALLED': 'Brak postępu',
                  'UNRESPONSIVE': 'Brak odpowiedzi', 'WAITING': 'Oczekiwanie'}.get(state, 'Pracuje')
    text = plain(data.get('text'))
    tool = data.get('tool') or {}
    activity = plain(activity_summary(data)) or status
    if state in FINAL or state in ('STALLED', 'UNRESPONSIVE'):
        activity = status
    tool_name = tool.get('name') or row.get('active_tool')
    details = f"narzędzia: {data.get('tools_completed', 0)}"
    if tool_name:
        details += f' · {tool_name}'
    age = max(0, int(now - (data.get('updated_at') or row.get('last_progress_at') or now)))
    if state not in FINAL and age >= 30:
        details += f' · bez nowych wpisów {age}s'
    blocks = []
    for entry in data.get('entries', []):
        if entry.get('kind') == 'assistant' and entry.get('id') == data.get('text_id'):
            continue
        label = 'Pi' if entry.get('kind') == 'assistant' else (entry.get('name') or 'Narzędzie')
        if entry.get('error'):
            label += ' · błąd'
        blocks.append(f"{label}\n{plain(entry.get('text'))}")
    if text:
        blocks.append('Pi\n' + text)
    if tool:
        blocks.append(f"{plain(tool.get('name'))} · w trakcie\n{plain(tool.get('text')) or 'Oczekiwanie na wynik…'}")
    tail = '\n\n'.join(blocks) or 'Oczekiwanie na pierwsze wpisy Pi…'
    # Aborted/crashed tasks have no settled_at. Match the manager's status
    # clock while their final transcript remains open in the inspector.
    ended_at = (row.get('settled_at') or row.get('last_event_at') or now) if state in FINAL else now
    return dict(row, status=status, activity=activity, details=details,
                tail=f'{status} · {details}\nOstatnie wpisy; długie wyniki są skracane.\n\n{tail}',
                subagent_id=row['task_id'], pi=True,
                goal=plain(row.get('prompt') or row['task_id']),
                elapsed=max(0, int(ended_at - (row.get('started_at') or now))))


class Monitor:
    """Adapt only this CLI's native monitor; reuse its UI and keybindings."""
    def __init__(self, cli, manager, owns):
        self.cli, self.manager, self.registry, self.owns = cli, manager, manager.registry, owns
        self.native = cli._subagent_monitor
        self.original_refresh = self.native.refresh
        self.original_control = self.native.control
        self.rows = {}
        self.closed = False
        self._lock = threading.RLock()
        self._cache = {}
        self._statuses = {}
        self._actions = {}
        self._action_times = {}
        self._pruned_at = 0

    def refresh(self, now=None):
        try:
            return self._refresh(now)
        except Exception:
            # A view incompatibility must not kill the native spinner thread.
            changed = bool(self.rows)
            self.rows = {}
            self.native.entries = [r for r in self.native.entries if not r.get('pi')]
            return changed

    def _refresh(self, now=None):
        with self._lock:
            if self.closed:
                return False
            selected = self.native.selected_id
            native_changed = self.original_refresh(now=now)
            previous = self.rows
            native_rows = self.native.entries
            now = time.time() if now is None else now
            with self.registry._lock:
                candidates = self.registry._conn.execute(
                    'SELECT task_id,origin,execution_state,verification_state,started_at,settled_at,'
                    'active_tool,last_progress_at,last_event_at,wake_state,last_error '
                    'FROM tasks ORDER BY created_at DESC LIMIT 256').fetchall()
            rows = {}
            for task_id, delivered_at in list(self._action_times.items()):
                if time.monotonic() - delivered_at >= 8:
                    self._actions.pop(task_id, None)
                    self._action_times.pop(task_id, None)
            for item in candidates:
                row = dict(item)
                try:
                    origin = json.loads(row.pop('origin') or '{}')
                except ValueError:
                    continue
                if not self.owns(origin):
                    continue
                # Keep a finished task while its inspector is open, so the
                # final output doesn't disappear underneath the reader.
                if (row['execution_state'] in FINAL and row['verification_state'] != 'PENDING'
                        and row['wake_state'] not in ('pending', 'dispatching')
                        and not (self.native.app or self.native.opening)):
                    continue
                data = load_snapshot(self.registry.path.parent / 'activity', row['task_id'])
                entry = presentation(row, data, now)
                if row['task_id'] in self._actions:
                    entry['details'] = self._actions[row['task_id']]
                    entry['tail'] = self._actions[row['task_id']] + '\n\n' + entry['tail']
                entry['goal'] = 'Pi · ' + entry['activity']
                entry['last_tool'] = entry['details']
                path = transcript_path(self.registry.path.parent / 'cli-transcripts', row['task_id'])
                if path.is_file():
                    # The native viewer now tails the growing event log directly.
                    # The compact snapshot remains the dock/Desktop projection.
                    stamp = path.stat()
                    entry['transcript_version'] = (stamp.st_mtime_ns, stamp.st_size)
                    self._record_status(entry)
                else:
                    # Tasks started by an older plugin still have a useful view.
                    path = self._write_preview(entry)
                entry['live_transcript'] = str(path)
                rows[row['task_id']] = entry
                if len(rows) >= 16:
                    break
            self.rows = rows
            self._cache = {key: value for key, value in self._cache.items() if key in rows}
            self._statuses = {key: value for key, value in self._statuses.items() if key in rows}
            self._actions = {key: value for key, value in self._actions.items() if key in rows}
            self.native.entries = native_rows + list(rows.values())
            ids = [entry['subagent_id'] for entry in self.native.entries]
            self.native.selected_id = selected if selected in ids else (ids[0] if ids else None)
            # Keep the host's idle repaint policy once the last task is gone.
            return native_changed or rows != previous

    def _record_status(self, row):
        task_id = row['task_id']
        status = (row['status'], row.get('last_error'), self._actions.get(task_id))
        if self._statuses.get(task_id) == status:
            return
        recorder = getattr(self.manager, '_activity_recorder', None)
        if recorder is not None:
            try:
                recorder.note(task_id, ' · '.join(str(part) for part in status if part))
            except Exception:
                return  # Display integration must never break the native monitor.
        self._statuses[task_id] = status

    def _write_preview(self, row):
        import os
        import tempfile
        # Compatibility fallback for tasks without a recorded CLI transcript.
        # Never hand the host a raw Pi session (which includes private thinking).
        directory = self.registry.path.parent / 'cli-view'
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        now = time.time()
        if now - self._pruned_at > 300:
            self._pruned_at = now
            files = sorted(directory.glob('*.txt'), key=lambda p: p.stat().st_mtime, reverse=True)
            for index, old in enumerate(files):
                if index >= 256 or old.stat().st_mtime < now - 7 * 86400:
                    old.unlink(missing_ok=True)
        path = snapshot_path(directory, row['task_id']).with_suffix('.txt')
        text = row['tail'][:24000]
        if self._cache.get(row['task_id']) != text or not path.exists():
            tmp = None
            try:
                with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
                    tmp = Path(stream.name)
                    stream.write(text.encode())
                os.replace(tmp, path)
                self._cache[row['task_id']] = text
            finally:
                if tmp is not None:
                    tmp.unlink(missing_ok=True)
        return path

    def control(self, action, message=None, *, target=None):
        task_id = target or self.native.selected_id
        if task_id not in self.rows and not str(task_id).startswith('pi-'):
            return self.original_control(action, message, target=task_id)
        row = self.registry.get_task(task_id) or {}
        try:
            if not self.owns(json.loads(row.get('origin') or '{}')):
                return {'error': 'Zadanie nie należy do bieżącej rozmowy.'}
            if row.get('execution_state') in FINAL:
                return {'error': 'Pi zakończył już to zadanie.'}
            if action not in ('steer', 'stop'):
                return {'error': 'Nieobsługiwana akcja.'}
            if action == 'steer' and not str(message or '').strip():
                return {'error': 'Wpisz wskazówkę dla Pi.'}
            with self._lock:
                if self._actions.get(task_id) == 'Przekazywanie polecenia…':
                    return {'error': 'Poprzednie polecenie jest jeszcze przekazywane.'}
                self._actions[task_id] = 'Przekazywanie polecenia…'
                self._action_times.pop(task_id, None)

            def dispatch():
                try:
                    current = self.registry.get_task(task_id) or {}
                    if self.closed or not self.owns(json.loads(current.get('origin') or '{}')):
                        raise RuntimeError('scope changed')
                    if action == 'steer':
                        self.manager.steer_task(task_id, str(message))
                        note = 'Wskazówka przekazana do Pi.'
                    else:
                        self.manager.abort_task(task_id)
                        note = 'Pi zatrzymane.'
                except Exception:
                    note = 'Błąd: nie udało się przekazać polecenia do Pi.'
                with self._lock:
                    if not self.closed:
                        self._actions[task_id] = note
                        self._action_times[task_id] = time.monotonic()
                self.refresh()
                self.native.invalidate()
            threading.Thread(target=dispatch, name='pi-cli-control', daemon=True).start()
            return {'note': 'Przekazywanie polecenia; wynik pojawi się przy zadaniu.'}
        except Exception:
            return {'error': 'Nie udało się przekazać polecenia do Pi.'}

    def attach(self):
        self.native.refresh = self.refresh
        self.native.control = self.control
        self.refresh()
        self.cli._app.invalidate()

    def close(self):
        with self._lock:
            self.closed = True
            if self.native.refresh == self.refresh:
                self.native.refresh = self.original_refresh
            if self.native.control == self.control:
                self.native.control = self.original_control
            self.original_refresh()
            self.rows = {}
            self._cache.clear()
            self._statuses.clear()
            self.cli._app.invalidate()
