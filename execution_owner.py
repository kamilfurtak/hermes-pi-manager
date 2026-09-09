"""Cross-process exclusion for Pi execution; independent of delivery ownership.

Locks are held from before STARTING is published through verification. The OS
releases them on process death. Empty lock files stay beside the durable task
registry: unlinking a lock while another process has opened it creates a race.
"""
import fcntl
import hashlib
import os


def acquire(directory, task_id):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / (hashlib.sha256(task_id.encode()).hexdigest() + '.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise


def release(fd):
    os.close(fd)


def process_alive(pid):
    """Conservative guard for live tasks started by pre-lock plugin versions."""
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
