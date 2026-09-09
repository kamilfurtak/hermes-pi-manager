"""Quiet dispatch, native post-turn display and subsequent human/terminal turns."""
import importlib.util
import io
from pathlib import Path
import queue
import sys
from contextlib import nullcontext
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from test_pi_manager import PiManagerTestCase
from cli_startup import ACKNOWLEDGEMENT, QuietStart


class QuietStartTests(unittest.TestCase):
    def host(self, *, streaming=False):
        printed, deltas = [], []
        cli = SimpleNamespace(session_id='mine', streaming_enabled=streaming,
                              _voice_tts=False, _pending_input=queue.Queue())
        agent = cli.agent = SimpleNamespace(stream_delta_callback=deltas.append if streaming else None,
                                            _stream_callback=None)
        cli._chat_render_turn = lambda turn, thread, message: printed.append(turn.result.copy())
        cli._flush_stream = Mock()
        adapter = QuietStart(cli, lambda: cli.session_id == 'mine')
        adapter.attach()
        self.addCleanup(adapter.close)
        return cli, agent, adapter, printed, deltas

    def render(self, cli, text=ACKNOWLEDGEMENT, **flags):
        result = dict(final_response=text, completed=True, messages=[{'role': 'assistant', 'content': text}], **flags)
        turn = SimpleNamespace(result=result)
        cli._flush_stream()
        cli._chat_render_turn(turn, None, None)
        self.assertIs(turn.result, result, 'persisted result must remain intact')
        self.assertEqual(result['messages'][0]['content'], text)
        return result

    def test_only_exact_start_reply_is_hidden_and_bindings_restore_after_one_turn(self):
        cli, agent, adapter, printed, _ = self.host()
        self.assertTrue(ACKNOWLEDGEMENT.strip(), 'Hermes must see a nonempty final response')
        self.render(cli)
        self.assertEqual(printed[-1]['final_response'], '')
        self.assertTrue(adapter.closed)
        self.assertIsNone(agent.stream_delta_callback)
        self.assertEqual(cli._flush_stream.call_count, 1, 'do not close the response box twice')
        self.render(cli, '5 × 5 = 25')
        self.render(cli, 'Wynik Pi: weryfikacja FAIL')
        self.render(cli)
        self.assertEqual([p['final_response'] for p in printed[1:]],
                         ['5 × 5 = 25', 'Wynik Pi: weryfikacja FAIL', ACKNOWLEDGEMENT])

    def test_stream_splits_and_whitespace_never_flash_the_start_reply(self):
        text = '\n\n' + ACKNOWLEDGEMENT + '\n'
        for split in range(len(text) + 1):
            with self.subTest(split=split):
                cli, agent, _, printed, deltas = self.host(streaming=True)
                agent.stream_delta_callback(text[:split])
                agent.stream_delta_callback(text[split:])
                self.render(cli, text)
                self.assertEqual(deltas, [])
                self.assertEqual(printed[-1]['final_response'], '')
                agent.stream_delta_callback('ordinary next answer')
                self.assertEqual(deltas, ['ordinary next answer'])

    def test_mismatching_text_and_explicit_task_id_are_preserved_verbatim(self):
        for text in ('Pi pracuje; task_id: pi-one', ACKNOWLEDGEMENT + '\nWażne: testy nie przeszły.',
                     'Nie mogę uruchomić zadania.', 'Pi', ' ' * 100):
            with self.subTest(text=text):
                cli, agent, _, printed, deltas = self.host(streaming=True)
                for c in text:
                    agent.stream_delta_callback(c)
                self.render(cli, text)
                self.assertEqual(''.join(deltas), text)
                self.assertEqual(printed[-1]['final_response'], text)

    def test_tool_boundary_flushes_partial_text_and_final_start_can_still_be_quiet(self):
        cli, agent, _, printed, deltas = self.host(streaming=True)
        agent.stream_delta_callback('Pi')
        agent.stream_delta_callback(None)
        agent.stream_delta_callback(ACKNOWLEDGEMENT)
        self.render(cli)
        self.assertEqual(deltas, ['Pi', None])
        self.assertEqual(printed[-1]['final_response'], '')

    def test_failed_partial_and_interrupted_responses_are_never_hidden(self):
        for flag in ('failed', 'partial', 'interrupted', 'error'):
            with self.subTest(flag=flag):
                cli, _, _, printed, _ = self.host()
                self.render(cli, **{flag: True})
                self.assertEqual(printed[-1]['final_response'], ACKNOWLEDGEMENT)

    def test_session_switch_agent_replacement_and_scope_error_fail_open(self):
        for change in ('session', 'agent', 'error'):
            cli, agent, adapter, printed, deltas = self.host(streaming=True)
            agent.stream_delta_callback(ACKNOWLEDGEMENT[:5])
            if change == 'session':
                cli.session_id = 'other'
            elif change == 'agent':
                cli.agent = object()
            else:
                adapter.owns = Mock(side_effect=RuntimeError('scope unavailable'))
            agent.stream_delta_callback(ACKNOWLEDGEMENT[5:])
            self.render(cli)
            self.assertEqual(''.join(deltas), ACKNOWLEDGEMENT)
            self.assertEqual(printed[-1]['final_response'], ACKNOWLEDGEMENT)

    def test_unload_flushes_pending_text_and_preserves_a_later_binding(self):
        cli, agent, adapter, _, deltas = self.host(streaming=True)
        agent.stream_delta_callback('Pi')
        later = cli._chat_render_turn = Mock()
        adapter.close()
        adapter.close()
        self.assertEqual(deltas, ['Pi'])
        self.assertIs(cli._chat_render_turn, later)
        self.assertEqual(cli._flush_stream.call_count, 1)

    def test_render_failure_restores_response_and_all_bindings(self):
        cli, agent, adapter, _, _ = self.host(streaming=True)
        adapter._original_render = Mock(side_effect=RuntimeError('renderer failed'))
        result = {'final_response': ACKNOWLEDGEMENT, 'completed': True}
        turn = SimpleNamespace(result=result)
        with self.assertRaisesRegex(RuntimeError, 'renderer failed'):
            cli._chat_render_turn(turn, None, None)
        self.assertIs(turn.result, result)
        self.assertTrue(adapter.closed)
        self.assertIsNotNone(agent.stream_delta_callback)

    def test_missing_host_methods_and_voice_use_visible_fallback(self):
        cli, agent, _, _, _ = self.host()
        self.assertTrue(QuietStart.supported(cli))
        cli._voice_tts = True
        self.assertFalse(QuietStart.supported(cli))
        cli._voice_tts = False
        agent._stream_callback = Mock()
        self.assertFalse(QuietStart.supported(cli))
        agent._stream_callback = None
        cli._chat_render_turn = lambda incompatible: None
        self.assertFalse(QuietStart.supported(cli))


class NativeRenderTests(unittest.TestCase):
    def test_installed_cli_renders_next_answer_and_keeps_interrupt_and_steer_queues(self):
        source = Path.home() / '.hermes/hermes-agent/hermes_cli/cli_chat_turn_mixin.py'
        if not source.exists():
            self.skipTest('installed Hermes CLI required')
        spec = importlib.util.spec_from_file_location('native_cli_turn_contract', source)
        host = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(host)
        output = io.StringIO()
        from rich.console import Console
        from rich.text import Text
        console = Console(file=output, color_system=None, width=100)
        module = ModuleType('cli')
        for key in ('_DIM', '_RST', '_ACCENT'):
            setattr(module, key, '')
        module._cprint = console.print
        module._suspend_output_history = nullcontext
        module.ChatConsole = lambda: console
        module._maybe_remap_for_light_mode = lambda color: color
        module._post_stream_transform_output = lambda response, result: ''
        module._render_final_assistant_content = lambda text, **kw: Text(text)
        cli = host.CLIChatTurnMixin()
        cli.session_id = 'mine'
        cli.streaming_enabled = False
        cli.agent = SimpleNamespace(stream_delta_callback=None, _stream_callback=None,
                                    _interrupt_requested=False, clear_interrupt=Mock())
        cli._voice_tts = cli._voice_continuous = False
        cli._stream_started = cli._stream_box_opened = False
        cli._interrupt_queue = queue.Queue()
        cli._pending_input = queue.Queue()
        cli._flush_stream = Mock()
        cli._chat_print_reasoning_box = Mock()
        cli._ring_bell = Mock()
        cli._scrollback_box_width = lambda: 100
        cli.final_response_markdown = 'strip'
        thread = SimpleNamespace(is_alive=lambda: False)
        quiet = QuietStart(cli, lambda: True)
        quiet.attach()
        self.addCleanup(quiet.close)
        result = dict(final_response=ACKNOWLEDGEMENT, completed=True, pending_steer='follow-up')
        turn = SimpleNamespace(result=result, use_streaming_tts=False)
        with patch.dict(sys.modules, {'cli': module}):
            self.assertEqual(cli._chat_render_turn(turn, thread, None), '')
            self.assertNotIn(ACKNOWLEDGEMENT, output.getvalue())
            self.assertNotIn('Hermes', output.getvalue(), 'no empty response box')
            self.assertEqual(cli._pending_input.get_nowait(), 'follow-up')
            turn.result = dict(final_response='5 × 5 = 25', completed=True)
            self.assertEqual(cli._chat_render_turn(turn, thread, None), '5 × 5 = 25')
            self.assertIn('5 × 5 = 25', output.getvalue())
            turn.result = dict(final_response='Pi: weryfikacja PASS', completed=True)
            cli._chat_render_turn(turn, thread, 'human interrupt')
            self.assertIn('Pi: weryfikacja PASS', output.getvalue())
            self.assertEqual(cli._pending_input.get_nowait(), 'human interrupt')
        self.assertNotIn('_chat_render_turn', vars(cli), 'restore the host descriptor')


if __name__ == '__main__':
    unittest.main()
