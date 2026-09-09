"""Present one Pi start acknowledgement in the CLI dock, not the chat.

Hermes retries empty model replies, so the tool supplies a nonempty, ordinary
sentence. Only that exact sentence, in the dispatching turn, is consumed by
this view adapter. History keeps the acknowledgement. No conversation-loop
state, persistence, terminal queue or host class is changed.
"""
from __future__ import annotations

import inspect
import threading

ACKNOWLEDGEMENT = "Pi pracuje w panelu Subagents."


class QuietStart:
    """Reversible instance bindings; all other responses pass through verbatim."""

    def __init__(self, cli, owns, on_close=None):
        self.cli = cli
        self.agent = cli.agent
        self.owns = owns
        self._on_close = on_close
        self.closed = False
        self._pending = ""
        self._passing = False
        self._lock = threading.RLock()
        self._bindings = []
        self._original_render = cli._chat_render_turn
        self._original_flush = cli._flush_stream
        self._original_delta = self.agent.stream_delta_callback

    @classmethod
    def supported(cls, cli):
        agent = getattr(cli, 'agent', None)
        # TTS has a separate stream consumer; retain an ordinary short reply
        # there instead of allowing a hidden visual reply to be spoken aloud.
        if (agent is None or getattr(cli, '_voice_tts', False)
                or getattr(agent, '_stream_callback', None) is not None
                or not hasattr(agent, 'stream_delta_callback')):
            return False
        try:
            inspect.signature(cli._chat_render_turn).bind(None, None, None)
            inspect.signature(cli._flush_stream).bind()
        except (AttributeError, TypeError, ValueError):
            return False
        return not getattr(cli, 'streaming_enabled', False) or callable(agent.stream_delta_callback)

    def current(self):
        try:
            return not self.closed and self.cli.agent is self.agent and self.owns()
        except Exception:
            return False

    def _bind(self, obj, name, replacement):
        local = name in vars(obj)
        original = getattr(obj, name)
        setattr(obj, name, replacement)
        self._bindings.append((obj, name, replacement, original, local))

    def attach(self):
        try:
            self._bind(self.cli, '_chat_render_turn', self.render)
            self._bind(self.cli, '_flush_stream', self.flush)
            if callable(self._original_delta):
                self._bind(self.agent, 'stream_delta_callback', self.delta)
        except Exception:
            self.close()
            raise

    def _drain(self, *, hide_ack):
        pending, self._pending = self._pending, ""
        if pending and not (hide_ack and pending.strip() == ACKNOWLEDGEMENT):
            self._original_delta(pending)
            return True
        return False

    def delta(self, text):
        with self._lock:
            if not self.current():
                self._drain(hide_ack=False)
                return self._original_delta(text)
            if text is None:
                self._drain(hide_ack=True)
                self._passing = False
                return self._original_delta(None)
            if not isinstance(text, str) or self._passing:
                return self._original_delta(text)
            self._pending += text
            candidate = self._pending.lstrip()
            # Hold at most one short acknowledgement, including token splits
            # and the paragraph break Hermes prepends after tools. The first
            # mismatch releases everything; never classify prose by meaning.
            if (len(self._pending) <= len(ACKNOWLEDGEMENT) + 64
                    and (ACKNOWLEDGEMENT.startswith(candidate)
                         or candidate.rstrip() == ACKNOWLEDGEMENT)):
                return
            self._passing = True
            self._drain(hide_ack=False)

    def flush(self):
        with self._lock:
            # Hermes flushes the final stream before calling its turn renderer.
            # Release incomplete prefixes here so partial output is not lost.
            self._drain(hide_ack=self.current())
            return self._original_flush()

    def render(self, turn, agent_thread, interrupt_msg):
        result = getattr(turn, 'result', None)
        quiet = (self.current() and isinstance(result, dict) and result.get('completed')
                 and not any(result.get(key) for key in ('failed', 'partial', 'interrupted', 'error'))
                 and isinstance(result.get('final_response'), str)
                 and result['final_response'].strip() == ACKNOWLEDGEMENT)
        try:
            if self._pending:
                self.flush()
            if quiet:
                # Only the display copy is empty. The real model response has
                # already passed Hermes' empty-response guard and is persisted.
                turn.result = dict(result, final_response='')
            return self._original_render(turn, agent_thread, interrupt_msg)
        finally:
            if quiet:
                turn.result = result
            self.close()

    def close(self):
        with self._lock:
            if self.closed:
                return
            self.closed = True
            try:
                if self._drain(hide_ack=False):
                    self._original_flush()
            finally:
                try:
                    for obj, name, installed, original, local in reversed(self._bindings):
                        # A later plugin binding owns itself; do not overwrite it.
                        if getattr(obj, name, None) is installed:
                            if local:
                                setattr(obj, name, original)
                            else:
                                delattr(obj, name)
                finally:
                    self._bindings.clear()
                    if self._on_close:
                        self._on_close(self)
