"""Native host adapter contract with real SQLite and redacted snapshots."""
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from test_pi_manager import PiManagerTestCase, wait_until
from activity import ActivityRecorder, snapshot_path
from cli_monitor import Monitor
from live_transcript import transcript_path


class TestNativeCLIMonitor(PiManagerTestCase):
    def setUp(self):
        super().setUp()
        # Optional installed-host integration; hermes itself stays read-only.
        source = Path.home() / '.hermes/hermes-agent/hermes_cli/cli_subagent_monitor.py'
        if not source.is_file():
            self.skipTest('installed Hermes native CLI monitor required')
        spec = importlib.util.spec_from_file_location('native_monitor_contract', source)
        self.host = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.host)
        delegate = ModuleType('tools.delegate_tool_registry')
        self.native_row = {'subagent_id': 'native-1', 'goal': 'Native task', 'status': 'running'}
        delegate._list_payload = lambda parent: {'subagents': [dict(self.native_row)]}
        delegate.list_active_subagents = lambda: [{'subagent_id': 'native-1', 'started_at': 10, 'last_tool': 'read'}]
        delegate._handle_control_action = Mock(return_value='{"note":"native control"}')
        self.delegate = delegate
        swap = patch.dict(sys.modules, {'tools.delegate_tool_registry': delegate})
        swap.start()
        self.addCleanup(swap.stop)
        self.cli = SimpleNamespace(agent=object(), _app=SimpleNamespace(invalidate=Mock()))
        self.native = self.host.SubagentMonitor(self.cli)
        self.native.invalidate = Mock()
        self.cli._subagent_monitor = self.native
        self.manager = SimpleNamespace(registry=self.registry, steer_task=Mock(), abort_task=Mock())
        self.monitor = Monitor(self.cli, self.manager, lambda origin: origin.get('session_id') == 'mine')
        self.addCleanup(self.monitor.close)

    def task(self, task_id='pi-one', **over):
        self.registry.create_task(task_id, origin=json.dumps({'session_id': 'mine'}),
                                  execution_state='TOOL_RUNNING', started_at=100, active_tool='bash', **over)
        directory = self.registry.path.parent / 'activity'
        directory.mkdir(exist_ok=True)
        data = {'schema': 1, 'task_id': task_id, 'updated_at': 150, 'tools_completed': 2,
                'text': 'Etap 3/6: sprawdzam wynik', 'text_id': 'm1',
                'entries': [{'id': 't1', 'kind': 'tool', 'name': 'bash', 'text': 'ETAP 2/6'},
                            {'id': 'm1', 'kind': 'assistant', 'text': 'Etap 3/6: sprawdzam wynik'}],
                'tool': {'id': 't2', 'name': 'bash', 'text': 'live output'}}
        snapshot_path(directory, task_id).write_text(json.dumps(data))
        return data

    def test_native_roster_dock_and_tail_preserve_other_agents_and_actual_activity(self):
        self.task()
        self.registry.create_task('pi-foreign', execution_state='RUNNING', origin='{"session_id":"other"}')
        self.monitor.attach()
        self.native.selected_id = 'pi-one'
        self.native.refresh(now=155)
        self.assertEqual([r['subagent_id'] for r in self.native.entries], ['native-1', 'pi-one'])
        self.assertEqual(self.native.selected_id, 'pi-one')
        self.assertIn('Etap 3/6', self.native.dock_text(columns=169, rows=51))
        self.assertIn('narzędzia: 2', self.native.dock_text(columns=169, rows=51))
        tail = self.host.read_tail(self.native.selected['live_transcript'])
        self.assertEqual(tail.count('Etap 3/6'), 1)
        self.assertIn('ETAP 2/6', tail)
        self.assertIn('live output', tail)
        self.assertEqual(Path(self.native.selected['live_transcript']).stat().st_mode & 0o777, 0o600)
        self.native.control('steer', 'native guidance', target='native-1')
        self.delegate._handle_control_action.assert_called_once()
        self.manager.steer_task.assert_not_called()
        self.monitor.close()
        self.assertEqual([r['subagent_id'] for r in self.native.entries], ['native-1'])

    def test_verification_and_final_tail_stay_visible_until_wake_and_inspector_finish(self):
        self.task()
        self.monitor.attach()
        self.registry.update_task('pi-one', execution_state='SETTLED', verification_state='PENDING')
        self.monitor.refresh()
        self.assertEqual(self.monitor.rows['pi-one']['status'], 'Weryfikacja')
        self.registry.update_task('pi-one', verification_state='FAIL', wake_state='pending')
        self.monitor.refresh()
        self.assertEqual(self.monitor.rows['pi-one']['status'], 'Błąd weryfikacji')
        self.native.opening = True
        self.registry.update_task('pi-one', wake_state='accepted')
        self.monitor.refresh()
        self.assertIn('pi-one', self.monitor.rows)
        self.native.opening = False
        self.monitor.refresh()
        self.assertNotIn('pi-one', self.monitor.rows)
        self.monitor.refresh(now=170)
        self.assertFalse(self.monitor.refresh(now=170), 'must not repaint an idle CLI forever')

    def test_corrupt_snapshot_falls_back_to_state_and_marks_silent_work(self):
        self.task()
        snapshot_path(self.registry.path.parent / 'activity', 'pi-one').write_text('[]')
        self.registry.update_task('pi-one', last_progress_at=100, execution_state='STALLED')
        self.monitor.refresh(now=160)
        self.assertEqual(self.monitor.rows['pi-one']['activity'], 'Brak postępu')
        self.assertIn('bez nowych wpisów 60s', self.monitor.rows['pi-one']['details'])

    def test_controls_are_scoped_and_report_rpc_errors_without_blocking_native_ui(self):
        self.task()
        self.monitor.attach()
        self.manager.steer_task.side_effect = RuntimeError('RPC disconnected')
        result = self.native.control('steer', 'continue', target='pi-one')
        self.assertIn('note', result)
        self.assertTrue(wait_until(lambda: self.monitor._actions.get('pi-one', '').startswith('Błąd:')))
        self.monitor.refresh()
        self.assertIn('Błąd:', self.monitor.rows['pi-one']['details'])
        self.registry.update_task('pi-one', origin='{"session_id":"other"}')
        self.assertIn('error', self.native.control('stop', target='pi-one'))
        self.manager.abort_task.assert_not_called()

    def test_native_tail_reads_append_log_with_history_beyond_compact_snapshot(self):
        self.task()
        recorder = ActivityRecorder(self.registry.path.parent / 'activity', interval=3600)
        self.addCleanup(recorder.close)
        self.manager._activity_recorder = recorder
        for i in range(12):
            recorder.observe('pi-one', {'type': 'tool_execution_start', 'toolCallId': str(i),
                                      'toolName': 'bash', 'args': {'command': f'printf STEP-{i}'}})
            recorder.observe('pi-one', {'type': 'tool_execution_end', 'toolCallId': str(i),
                                      'result': {'content': f'STEP-{i}\n'}})
        recorder.flush()
        with patch.object(self.monitor, '_write_preview', side_effect=AssertionError('snapshot tail used')):
            self.monitor.attach()
            self.native.selected_id = 'pi-one'
            self.native.refresh()
        path = transcript_path(self.registry.path.parent / 'cli-transcripts', 'pi-one')
        self.assertEqual(self.native.selected['live_transcript'], str(path))
        view = self.host.read_tail(str(path))
        self.assertIn('printf STEP-0', view)
        self.assertIn('printf STEP-11', view)
        self.assertNotIn('Ostatnie wpisy;', view)
        data = json.loads(snapshot_path(recorder.directory, 'pi-one').read_text())
        self.assertEqual(len(data['entries']), 8, 'Desktop keeps its original bounded projection')
        recorder.observe('pi-one', {'type': 'tool_execution_start', 'toolCallId': 'live',
                                  'toolName': 'bash', 'args': {'command': 'long command'}})
        recorder.observe('pi-one', {'type': 'tool_execution_update', 'toolCallId': 'live',
                                  'partialResult': {'content': 'OUTPUT BEFORE COMPLETION\n'}})
        recorder.flush()
        self.assertTrue(self.native.refresh())
        self.assertIn('OUTPUT BEFORE COMPLETION', self.host.read_tail(str(path)))
        self.assertTrue(path.read_text().startswith(view))

    def test_registry_verdict_appends_once_and_redacts_error_details(self):
        self.task()
        recorder = ActivityRecorder(self.registry.path.parent / 'activity', interval=3600,
                                    redact=lambda text: text.replace('secret-token', '[redacted]'))
        self.addCleanup(recorder.close)
        self.manager._activity_recorder = recorder
        recorder.observe('pi-one', {'type': 'agent_start'})
        recorder.flush()
        self.monitor.attach()
        self.registry.update_task('pi-one', execution_state='SETTLED', verification_state='FAIL',
                                  wake_state='pending', last_error='secret-token')
        self.monitor.refresh()
        recorder.flush()
        self.monitor.refresh()
        recorder.flush()
        path = transcript_path(self.registry.path.parent / 'cli-transcripts', 'pi-one')
        view = self.host.read_tail(str(path))
        self.assertEqual(view.count('Błąd weryfikacji'), 1)
        self.assertIn('[redacted]', view)
        self.assertNotIn('secret-token', view)
