"""Observable transcript contracts: streaming, history, privacy and host use."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from activity import ActivityRecorder, snapshot_path
from live_transcript import TranscriptRecorder, transcript_path, read_transcript
from test_pi_manager import wait_until


def text(delta, index=0):
    return {'type': 'message_update', 'assistantMessageEvent': {
        'type': 'text_delta', 'contentIndex': index, 'delta': delta}}


def start(tool_id='a', name='bash', command='printf "one\\ntwo\\n"'):
    return {'type': 'tool_execution_start', 'toolCallId': tool_id,
            'toolName': name, 'args': {'command': command}}


def output(value, tool_id='a', *, final=False, error=False):
    return {'type': 'tool_execution_end' if final else 'tool_execution_update',
            'toolCallId': tool_id, 'isError': error,
            'result' if final else 'partialResult': {'content': [{'type': 'text', 'text': value}]}}


class DesktopTranscriptReadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.path = transcript_path(self.directory, 'pi-one')

    def test_incremental_read_waits_for_complete_utf8_lines_and_never_repeats(self):
        self.path.write_bytes('polecenie\n'.encode())
        first = read_transcript(self.directory, 'pi-one')
        self.assertTrue(first['reset'])
        self.assertEqual(first['text'], 'polecenie\n')
        self.assertFalse(first['truncated'])
        encoded = 'żółw 😀\n'.encode()
        with self.path.open('ab') as stream:
            stream.write(encoded[:-3])
        partial = read_transcript(self.directory, 'pi-one', first['cursor'])
        self.assertEqual(partial['text'], '')
        self.assertEqual(partial['cursor'], first['cursor'])
        with self.path.open('ab') as stream:
            stream.write(encoded[-3:])
        next_view = read_transcript(self.directory, 'pi-one', partial['cursor'])
        self.assertEqual(next_view['text'], 'żółw 😀\n')
        self.assertFalse(next_view['reset'])
        self.assertEqual(read_transcript(self.directory, 'pi-one', next_view['cursor'])['text'], '')

    def test_tail_and_slow_client_reset_are_bounded_and_start_on_a_line(self):
        self.path.write_text('początek\n')
        initial = read_transcript(self.directory, 'pi-one')
        with self.path.open('a') as stream:
            stream.write('żółw\n' * 20)
        with patch('live_transcript.MAX_VIEW_BYTES', 31):
            for cursor in ('', initial['cursor'], 'invalid', initial['cursor'].split(':')[0] + ':999999'):
                view = read_transcript(self.directory, 'pi-one', cursor)
                self.assertTrue(view['reset'])
                self.assertTrue(view['truncated'])
                self.assertLessEqual(len(view['text'].encode()), 31)
                self.assertEqual(set(view['text'].splitlines()), {'żółw'})

    def test_rotation_resets_cursor_even_when_the_new_file_has_the_same_length(self):
        self.path.write_text('before\n')
        first = read_transcript(self.directory, 'pi-one')
        self.path.rename(self.path.with_suffix('.log.1'))
        self.path.write_text('after!\n')
        rotated = read_transcript(self.directory, 'pi-one', first['cursor'])
        self.assertTrue(rotated['reset'])
        self.assertTrue(rotated['truncated'])
        self.assertEqual(rotated['text'], 'after!\n')

    def test_absent_logs_create_nothing_and_symlinks_are_not_served(self):
        absent = self.directory / 'absent'
        self.assertEqual(read_transcript(absent, 'pi-one'), {'available': False})
        self.assertFalse(absent.exists())
        target = self.directory / 'not-a-transcript.txt'
        target.write_text('private file')
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            read_transcript(self.directory, 'pi-one')


class TranscriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / 'logs'
        self.recorder = TranscriptRecorder(self.directory, lambda value: value.replace('secret-token', '[redacted]'))
        self.path = transcript_path(self.directory, 'pi-one')

    def emit(self, event, now=100):
        self.recorder.observe('pi-one', event, now)

    def read(self):
        self.recorder.flush()
        return self.path.read_text()

    def test_command_then_partial_output_before_completion_without_duplicates(self):
        self.emit(text('Etap pierwszy'))
        self.emit(start(), 101)
        self.assertFalse(self.directory.exists(), 'RPC ingestion must not do disk I/O')
        initial = self.read()
        self.assertIn('Etap pierwszy', initial)
        self.assertIn('→ bash #1', initial)
        self.assertIn('printf "one\\ntwo\\n"', initial)
        self.emit(output('one\n'), 103)
        first = self.read()
        self.assertTrue(first.startswith(initial), 'history must be appended, not replaced')
        self.assertIn('bash #1: one', first)
        self.assertNotIn('result', first)
        self.emit(output('one\ntwo\n'), 105)
        self.emit(output('one\ntwo\n'), 106)
        self.emit(output('one\ntwo\nthree', final=True), 107)
        self.emit(output('one\ntwo\nthree', final=True), 108)
        final = self.read()
        for value in ('one', 'two', 'three'):
            self.assertEqual(final.count('bash #1: ' + value), 1)
        self.assertIn('bash #1 · OK · 6.0s', final)
        self.assertEqual(final.count('=== Pi live transcript ==='), 1)

    def test_history_survives_more_than_eight_entries_and_recorder_restart(self):
        for i in range(16):
            self.emit(start(str(i), command=f'printf step-{i}'), 100 + i * 2)
            self.emit(output(f'step-{i}\n', str(i), final=True), 101 + i * 2)
            self.recorder.flush()
        first = self.read()
        self.assertIn('printf step-0', first)
        self.assertIn('printf step-15', first)
        other = TranscriptRecorder(self.directory, lambda value: value)
        other.note('pi-one', 'Wznowiono podgląd', 150)
        other.flush()
        self.assertTrue(self.path.read_text().startswith(first))

    def test_parallel_tools_keep_independent_output_and_elapsed_times(self):
        self.emit(start('a'), 10)
        self.emit(start('b', name='read', command='file.txt'), 11)
        self.emit(output('line-a\n', 'a'), 12)
        self.emit(output('line-b\n', 'b', final=True, error=True), 13)
        self.emit(output('line-a\nlast-a\n', 'a', final=True), 14)
        view = self.read()
        self.assertIn('bash #1: line-a', view)
        self.assertIn('read #2: line-b', view)
        self.assertIn('read #2 · BŁĄD · 2.0s', view)
        self.assertIn('bash #1 · OK · 4.0s', view)
        self.assertEqual(view.count('bash #1: line-a'), 1)

    def test_split_secrets_are_redacted_before_append_and_thinking_is_absent(self):
        self.emit(text('secret-'))
        self.recorder.flush()
        self.assertFalse(self.path.exists())
        self.emit(text('token\n'))
        self.emit({'type': 'message_update', 'assistantMessageEvent': {
            'type': 'thinking_delta', 'delta': 'private reasoning'}})
        self.emit(start(command='printf secret-token'))
        self.emit(output('secret-'))
        interim = self.read()
        self.assertNotIn('secret-', interim)
        self.emit(output('secret-token\n', final=True))
        final = self.read()
        self.assertNotIn('secret-', final)
        self.assertNotIn('private reasoning', final)
        self.assertIn('bash #1: [redacted]', final)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)

    def test_text_blocks_and_authoritative_message_end_are_not_duplicated(self):
        self.emit({'type': 'message_start', 'message': {'role': 'assistant'}})
        self.emit(text('Hello\n', 1))
        self.emit(text('World', 2))
        self.emit({'type': 'message_end', 'message': {'role': 'assistant', 'content': [
            {'type': 'thinking', 'thinking': 'private'}, {'type': 'text', 'text': 'Hello\n'},
            {'type': 'text', 'text': 'World'}]}})
        view = self.read()
        self.assertEqual(view.count('Hello'), 1)
        self.assertEqual(view.count('World'), 1)
        self.assertNotIn('private', view)

    def test_changed_authoritative_result_is_labeled_instead_of_silently_appended(self):
        self.emit(start())
        self.emit(output('provisional\n'))
        self.recorder.flush()
        self.emit(output('corrected\n', final=True))
        view = self.read()
        self.assertIn('zaktualizowało wcześniejszy tekst', view)
        self.assertIn('bash #1: corrected', view)

    def test_oversize_lines_and_bursts_are_bounded_with_visible_gap_markers(self):
        with patch('live_transcript.MAX_LINE_CHARS', 16), patch('live_transcript.MAX_PENDING_RECORDS', 4):
            self.emit(text('x' * 1000))
            self.emit(text('\n'))
            self.assertIn('Pominięto linię', self.read())
            for i in range(20):
                self.emit(text(f'line-{i}\n'))
            view = self.read()
            self.assertIn('przepełniony bufor', view)
            self.assertIn('line-19', view)
            self.assertNotIn('x' * 20, view)

    def test_rotation_keeps_previous_part_private_and_paths_are_hashed(self):
        with patch('live_transcript.MAX_LOG_BYTES', 600):
            for i in range(15):
                self.recorder.note('../pi-one', f'line-{i} ' + 'x' * 80, 100)
                self.recorder.flush()
            path = transcript_path(self.directory, '../pi-one')
            self.assertEqual(path.parent, self.directory)
            previous = path.with_suffix('.log.1')
            self.assertTrue(previous.exists())
            self.assertLessEqual(path.stat().st_size, 600)
            self.assertLessEqual(previous.stat().st_size, 600)
            self.assertEqual(previous.stat().st_mode & 0o777, 0o600)
            self.assertIn('line-14', path.read_text())

    def test_concurrent_flushes_keep_event_order(self):
        entered, release = threading.Event(), threading.Event()
        def redact(value):
            if value == 'first':
                entered.set()
                self.assertTrue(release.wait(2))
            return value
        self.recorder.redact = redact
        self.recorder.note('pi-one', 'first', 100)
        first = threading.Thread(target=self.recorder.flush)
        first.start()
        self.assertTrue(entered.wait(2))
        self.recorder.note('pi-one', 'second', 101)
        second = threading.Thread(target=self.recorder.flush)
        second.start()
        release.set()
        first.join(2)
        second.join(2)
        self.assertLess(self.path.read_text().index('first'), self.path.read_text().index('second'))

    def test_background_activity_writer_publishes_log_and_unchanged_desktop_snapshot(self):
        recorder = ActivityRecorder(Path(self.tmp.name) / 'activity', interval=0.01)
        self.addCleanup(recorder.close)
        recorder.observe('pi-live', start())
        recorder.observe('pi-live', output('live line\n'))
        path = transcript_path(Path(self.tmp.name) / 'cli-transcripts', 'pi-live')
        self.assertTrue(wait_until(lambda: path.exists() and 'live line' in path.read_text()))
        recorder.close()
        snapshot = json.loads(snapshot_path(recorder.directory, 'pi-live').read_text())
        self.assertEqual(snapshot['tool']['text'], 'live line\n')
        self.assertNotIn('command', snapshot['tool'])
        self.assertEqual(snapshot['tools_completed'], 0)

    def test_log_failure_cannot_disable_snapshot_publication(self):
        recorder = ActivityRecorder(Path(self.tmp.name) / 'activity', interval=3600)
        self.addCleanup(recorder.close)
        recorder.observe('pi-live', text('visible\n'))
        with patch.object(recorder._transcripts, '_append', side_effect=OSError('test write failure')):
            with self.assertLogs('live_transcript', level='WARNING'):
                recorder.flush()
        self.assertIn('visible', snapshot_path(recorder.directory, 'pi-live').read_text())
