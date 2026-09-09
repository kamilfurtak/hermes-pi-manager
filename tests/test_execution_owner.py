"""Several Hermes hosts sharing one registry must not reopen live Pi work."""
import json
import os
import subprocess
import sys
from unittest.mock import patch

from test_pi_manager import PiManagerTestCase, FakePiProcess, wait_until
from core import PiManager
from outbox import NotificationOutbox
from activity import snapshot_path


class TestExecutionOwner(PiManagerTestCase):
    def test_second_host_cannot_recover_an_active_task_or_repeat_its_prompt(self):
        process = FakePiProcess()
        first = self.make_manager(process)
        second_process = FakePiProcess()
        second = self.make_manager(second_process)
        self.addCleanup(first.shutdown)
        self.addCleanup(second.shutdown)
        task_id = self.start_and_boot(first, process)
        before = self.registry.get_task(task_id)
        self.assertEqual(second.recover_task(task_id)['reason'], 'execution_owner_active')
        self.assertEqual(first.recover_task(task_id)['reason'], 'execution_owner_active')
        self.assertEqual(self.registry.get_task(task_id), before)
        self.assertFalse(second_process.wait_until_command('get_state', timeout=0.05))

    def test_lock_is_held_across_processes_and_released_on_shutdown(self):
        first = self.make_manager(FakePiProcess())
        self.addCleanup(first.shutdown)
        self.assertTrue(first._claim_execution('pi-locked'))
        code = ('from pathlib import Path; import execution_owner as o; import sys; '
                'fd=o.acquire(Path(sys.argv[1]), "pi-locked"); print(fd is not None); '
                'o.release(fd) if fd is not None else None')
        args = [sys.executable, '-c', code, str(self.tmp / 'execution-locks')]
        self.assertEqual(subprocess.check_output(args, text=True).strip(), 'False')
        first.shutdown()
        self.assertEqual(subprocess.check_output(args, text=True).strip(), 'True')

    def test_legacy_live_pid_does_not_get_adopted_or_aborted(self):
        # A pre-upgrade host has no advisory lock; its live PID still protects it.
        self.registry.create_task('pi-legacy', execution_state='RUNNING', pid=os.getpid())
        manager = self.make_manager(FakePiProcess())
        self.addCleanup(manager.shutdown)
        self.assertEqual(manager.recover_task('pi-legacy')['reason'], 'worker_process_active')
        self.assertEqual(self.registry.get_task('pi-legacy')['execution_state'], 'RUNNING')

    def test_legacy_startup_grace_protects_only_a_recent_allocated_session(self):
        session = self.tmp / 'starting.jsonl'
        session.touch()
        self.registry.create_task('pi-starting', session_file=str(session), created_at=self.clock())
        manager = self.make_manager(FakePiProcess())
        self.addCleanup(manager.shutdown)
        self.assertEqual(manager.recover_task('pi-starting')['reason'], 'worker_starting')
        self.clock.advance(61)
        self.assertFalse(manager.recover_task('pi-starting')['recovered'])
        self.assertEqual(self.registry.get_task('pi-starting')['execution_state'], 'ABORTED')

    def test_spawn_failure_notifies_and_releases_execution_ownership(self):
        def fail_spawn(*args):
            raise OSError('test spawn failure')
        manager = PiManager(self.registry, popen_factory=fail_spawn, outbox=NotificationOutbox(self.registry))
        self.addCleanup(manager.shutdown)
        task_id = manager.start_task('test', str(self.tmp), origin={'session_key': 'test'})['task_id']
        self.assertTrue(wait_until(lambda: self.registry.get_task(task_id)['wake_state'] == 'pending'))
        self.assertEqual(self.registry.get_task(task_id)['execution_state'], 'CRASHED')
        self.assertEqual(self.registry.list_notifications(task_id=task_id)[0]['kind'], 'failed')
        self.assertNotIn(task_id, manager._execution_locks)

    def test_progress_uses_shared_redacted_activity_for_native_messaging(self):
        self.registry.create_task('pi-msg', origin=json.dumps({'platform': 'telegram', 'chat_id': '123'}),
                                  execution_state='TOOL_RUNNING', active_tool='bash')
        directory = self.tmp / 'activity'
        directory.mkdir()
        snapshot_path(directory, 'pi-msg').write_text(json.dumps({
            'schema': 1, 'task_id': 'pi-msg', 'text': '', 'tools_completed': 3,
            'entries': [{'kind': 'assistant', 'text': 'Etap 4/6: uruchamiam testy [redacted]'}]}))
        manager = PiManager(self.registry, outbox=NotificationOutbox(self.registry))
        self.addCleanup(manager.shutdown)
        nid = manager._enqueue_notification('pi-msg', 'progress')
        message = self.registry.get_notification(nid)['message']
        self.assertIn('Etap 4/6', message)
        self.assertIn('wywołania narzędzi: 3', message)
        self.assertIn('[redacted]', message)
        self.assertEqual(self.registry.get_notification(nid)['target'], 'telegram:123')
