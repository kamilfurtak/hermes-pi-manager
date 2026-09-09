"""Private, append-only Pi event log for the native CLI and Desktop viewers.

Only visible text and executed tool calls enter this projection. The RPC reader
updates bounded memory; ActivityRecorder's existing writer performs all I/O.
Complete lines are redacted before publication, including credentials split
across multiple RPC deltas. This log is disposable display data, not task state.
"""
from __future__ import annotations

from collections import OrderedDict, deque
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import threading
import time
import unicodedata

LOG = logging.getLogger(__name__)
MAX_TASKS = 32
MAX_TOOLS = 256
MAX_LINE_CHARS = 16384
MAX_RECORD_CHARS = 65536
MAX_PENDING_CHARS = 262144
MAX_PENDING_RECORDS = 1024
MAX_LOG_BYTES = 2 * 1024 * 1024
MAX_VIEW_BYTES = 256 * 1024
RETENTION_SECONDS = 7 * 86400
_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def transcript_path(directory, task_id):
    return Path(directory) / (hashlib.sha256(task_id.encode()).hexdigest() + '.log')


def read_transcript(directory, task_id, cursor=''):
    """Read complete UTF-8 lines after an opaque cursor, or a bounded tail.

    The caller must authorize the task/conversation before opening this file.
    An inode change (rotation), invalid cursor or lag beyond the display bound
    resets the window explicitly. No writer, manager or task state is created.
    """
    path = transcript_path(directory, task_id)
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK)
    except FileNotFoundError:
        return {'available': False}
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise OSError('Transcript is not a regular file')
        generation = f'{info.st_dev:x}-{info.st_ino:x}'
        match = re.fullmatch(r'([a-f0-9]+-[a-f0-9]+):([0-9]{1,20})', cursor)
        offset = int(match[2]) if match else -1
        resume = (match is not None and match[1] == generation
                  and 0 <= offset <= info.st_size
                  and info.st_size - offset <= MAX_VIEW_BYTES)
        start = offset if resume else max(0, info.st_size - MAX_VIEW_BYTES)
        # Drop a leading fragment if a bounded tail begins inside a line.
        stream.seek(max(0, start - 1))
        boundary = start == 0 or stream.read(1) == b'\n'
        raw = stream.read(min(MAX_VIEW_BYTES, info.st_size - start))
        if not boundary:
            cut = raw.find(b'\n') + 1
            start += cut
            raw = raw[cut:]
        # An append may still be in progress. Leave its unfinished line for
        # the next poll instead of splitting a character or publishing twice.
        raw = raw[:raw.rfind(b'\n') + 1]
        return {'available': True, 'text': raw.decode('utf-8', errors='replace'),
                'cursor': f'{generation}:{start + len(raw)}', 'reset': not resume,
                'truncated': not resume and (start > 0 or bool(cursor)
                                             or path.with_suffix('.log.1').exists())}


def _visible(message):
    content = message.get('content', []) if isinstance(message, dict) else []
    if isinstance(content, str):
        return {0: content}
    return {i: part['text'] for i, part in enumerate(content)
            if isinstance(part, dict) and part.get('type') == 'text'
            and isinstance(part.get('text'), str)}


class TextStream:
    """Turn deltas or cumulative snapshots into new, complete display lines."""
    def __init__(self, emit):
        self.emit = emit
        self.seen = 0
        self.suffix = ''
        self.pending = ''
        self.oversize = False

    def append(self, delta, now):
        if not isinstance(delta, str) or not delta:
            return
        self.seen += len(delta)
        self.suffix = (self.suffix + delta)[-128:]
        pieces = delta.split('\n')
        for index, piece in enumerate(pieces):
            if not self.oversize:
                if len(self.pending) + len(piece) > MAX_LINE_CHARS:
                    self.pending = ''
                    self.oversize = True
                else:
                    self.pending += piece
            if index < len(pieces) - 1:
                self.finish(now)

    def replace(self, value, now, *, final=False):
        # Pi tool updates contain accumulated output, not independent deltas.
        if self.seen and (len(value) < self.seen or
                          value[max(0, self.seen - 128):self.seen] != self.suffix):
            self.emit('[Pi zaktualizowało wcześniejszy tekst; aktualna wersja poniżej]', now)
            self.seen, self.suffix, self.pending, self.oversize = 0, '', '', False
        self.append(value[self.seen:], now)
        if final:
            self.finish(now)

    def finish(self, now):
        if self.oversize:
            self.emit('[Pominięto linię przekraczającą limit podglądu]', now)
        elif self.pending:
            self.emit(self.pending, now)
        self.pending, self.oversize = '', False


class Transcript:
    def __init__(self, emit):
        self.emit = emit
        self.blocks = {}
        self.tools = OrderedDict()
        self.completed = OrderedDict()
        self.tool_number = 0

    def _block(self, index):
        if index not in self.blocks and len(self.blocks) < 32:
            self.blocks[index] = TextStream(lambda text, now: self.emit('Pi', text, now))
        return self.blocks.get(index)

    def _finish_text(self, now):
        for block in self.blocks.values():
            block.finish(now)

    def observe(self, event, now):
        kind = event.get('type')
        if kind == 'message_start' and event.get('message', {}).get('role') == 'assistant':
            self.blocks = {}
        elif kind == 'message_update':
            update = event.get('assistantMessageEvent') or {}
            if update.get('type') not in ('text_start', 'text_delta', 'text_end'):
                return  # No thinking or speculative, unfinished tool arguments.
            block = self._block(str(update.get('contentIndex', 0)))
            if block is not None:
                if update['type'] == 'text_delta':
                    block.append(update.get('delta'), now)
                elif update['type'] == 'text_end':
                    block.replace(update.get('content') or '', now, final=True)
        elif kind == 'message_end' and event.get('message', {}).get('role') == 'assistant':
            for index, value in _visible(event['message']).items():
                block = self._block(str(index))
                if block is not None:
                    block.replace(value, now, final=True)
            self._finish_text(now)
            self.blocks = {}
        elif kind == 'tool_execution_start':
            tool_id = str(event.get('toolCallId') or '')[:128]
            if not tool_id or tool_id in self.tools or tool_id in self.completed:
                return
            self._finish_text(now)
            self.tool_number += 1
            label = f"{str(event.get('toolName') or 'tool')[:80]} #{self.tool_number}"
            stream = TextStream(lambda text, at: self.emit('output', f'{label}: {text}', at))
            self.tools[tool_id] = (label, now, stream)
            if len(self.tools) > MAX_TOOLS:
                self.tools.popitem(last=False)
                self.emit('status', '[Przekroczono limit jednoczesnych narzędzi podglądu]', now)
            args = event.get('args') or {}
            if isinstance(args, dict) and isinstance(args.get('command'), str):
                command = args['command']
                extra = {key: value for key, value in args.items() if key != 'command'}
                if extra:
                    command += '\n' + json.dumps(extra, ensure_ascii=False)
            else:
                command = json.dumps(args, ensure_ascii=False, indent=2)
            if len(command) > MAX_RECORD_CHARS - 128:
                command = '[Argumenty przekraczają limit podglądu]'
            self.emit('tool', f'→ {label}\n{command}', now)
        elif kind in ('tool_execution_update', 'tool_execution_end'):
            tool_id = str(event.get('toolCallId') or '')[:128]
            if tool_id not in self.tools:
                return
            label, started, stream = self.tools[tool_id]
            final = kind == 'tool_execution_end'
            result = event.get('result' if final else 'partialResult') or {}
            stream.replace('\n'.join(_visible(result).values()), now, final=final)
            if final:
                status = 'BŁĄD' if event.get('isError') else 'OK'
                self.emit('result', f'{label} · {status} · {max(0, now - started):.1f}s', now)
                if (result.get('details') or {}).get('truncation'):
                    self.emit('status', f'{label}: wynik został skrócony przez Pi', now)
                del self.tools[tool_id]
                self.completed[tool_id] = True
                if len(self.completed) > MAX_TOOLS:
                    self.completed.popitem(last=False)
        elif kind == 'agent_start':
            self.emit('status', 'Pi pracuje', now)
        elif kind == 'agent_settled':
            self._finish_text(now)
            self.emit('status', 'Pi zakończyło generowanie; wynik weryfikacji pokazuje status zadania.', now)
        elif kind in ('auto_retry_start', 'extension_error'):
            self.emit('status', str(event.get('errorMessage') or event.get('error') or kind), now)


class TranscriptRecorder:
    def __init__(self, directory, redact):
        self.directory, self.redact = Path(directory), redact
        self._lock, self._write_lock = threading.Lock(), threading.Lock()
        self._tasks = OrderedDict()
        self._pending = deque()
        self._pending_chars = 0
        self._dropped = OrderedDict()
        self._pruned_at = 0

    def _emit(self, task_id, role, text, now):
        if not text:
            return
        if len(text) > MAX_RECORD_CHARS:
            text = '[Wpis przekracza limit podglądu]'
        while self._pending and (self._pending_chars + len(text) > MAX_PENDING_CHARS
                                 or len(self._pending) >= MAX_PENDING_RECORDS):
            old_id, _, _, old_text = self._pending.popleft()
            self._pending_chars -= len(old_text)
            self._dropped[old_id] = self._dropped.get(old_id, 0) + 1
            if len(self._dropped) > MAX_TASKS:
                self._dropped.popitem(last=False)
        self._pending.append((task_id, now, role, text))
        self._pending_chars += len(text)

    def observe(self, task_id, event, now):
        with self._lock:
            if task_id not in self._tasks:
                self._tasks[task_id] = Transcript(lambda role, text, at: self._emit(task_id, role, text, at))
                if len(self._tasks) > MAX_TASKS:
                    self._tasks.popitem(last=False)
            self._tasks.move_to_end(task_id)
            self._tasks[task_id].observe(event, now)

    def note(self, task_id, text, now):
        with self._lock:
            self._emit(task_id, 'status', text, now)

    def _render(self, now, role, text):
        text = self.redact(text)
        text = _ANSI.sub('', text)
        text = ''.join(c for c in text if c in '\n\t' or not unicodedata.category(c).startswith('C'))
        prefix = f"{time.strftime('%H:%M:%S', time.localtime(now))} {role:<9}| "
        return prefix + text.replace('\n', '\n' + ' ' * len(prefix)) + '\n'

    def flush(self):
        # close()/manual flush and the background writer must preserve order.
        with self._write_lock:
            with self._lock:
                pending, self._pending = self._pending, deque()
                dropped, self._dropped = self._dropped, OrderedDict()
                self._pending_chars = 0
            groups = OrderedDict()
            for task_id, count in dropped.items():
                groups[task_id] = [(time.time(), 'status', f'[Pominięto {count} wpisów: przepełniony bufor podglądu]')]
            for task_id, now, role, text in pending:
                groups.setdefault(task_id, []).append((now, role, text))
            for task_id, records in groups.items():
                try:
                    text = ''.join(self._render(*record) for record in records)
                    self._append(task_id, text.encode('utf-8'))
                except Exception:
                    LOG.warning('Pi live transcript could not publish entries', exc_info=True)
            if groups and time.time() - self._pruned_at > 300:
                self._prune()

    def _append(self, task_id, data):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = transcript_path(self.directory, task_id)
        if len(data) > MAX_LOG_BYTES // 2:
            data = ('[Pominięto wcześniejszą część dużej aktualizacji]\n' +
                    data[-MAX_LOG_BYTES // 2:].decode('utf-8', errors='ignore')).encode('utf-8')
        if path.exists() and path.stat().st_size + len(data) > MAX_LOG_BYTES:
            os.replace(path, path.with_suffix('.log.1'))
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'ab') as stream:
            os.fchmod(stream.fileno(), 0o600)
            if stream.tell() == 0:
                stream.write(b'=== Pi live transcript ===\n')
                if path.with_suffix('.log.1').exists():
                    stream.write('Wcześniejsza część dziennika: plik .log.1\n'.encode())
            stream.write(data)

    def _prune(self):
        self._pruned_at = time.time()
        try:
            files = sorted(self.directory.glob('*.log'), key=lambda p: p.stat().st_mtime, reverse=True)
            for index, path in enumerate(files):
                if re.fullmatch(r'[a-f0-9]{64}\.log', path.name) and (
                        index >= 256 or path.stat().st_mtime < self._pruned_at - RETENTION_SECONDS):
                    path.unlink(missing_ok=True)
                    path.with_suffix('.log.1').unlink(missing_ok=True)
        except OSError:
            LOG.debug('Pi live transcript cleanup failed', exc_info=True)
