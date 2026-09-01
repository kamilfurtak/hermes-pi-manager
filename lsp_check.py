"""Semantic verification of what Pi wrote, using the host's LSP servers.

WHY THIS EXISTS
---------------
Pi has no language server. It writes code without type checking, so a
delegated task can settle SETTLED/PASS while leaving undefined names,
missing imports or type errors that only surface much later — often in a
different session, after the context that would explain them is gone.

Hermes ships ~20 language servers and exposes them in-process through
``agent.lsp.LSPService.get_diagnostics_sync``. That is a plain function
call, NOT a model-visible tool: running it costs zero agent turns and zero
tokens, which is the whole point — the same reason the notification outbox
calls ``send_message_tool`` directly instead of registering a tool.

INTERNAL DEPENDENCY (deliberate, isolated here)
-----------------------------------------------
``agent.lsp`` is Hermes-internal: the documented ``PluginContext`` surface
has no LSP entry point, and the plugin docs warn that undocumented
internals carry no stability guarantee. This module is therefore the ONLY
place that imports it, every failure path returns "not run" rather than
raising, and the caller treats a ``None`` result as "no information" — never
as "clean". If a future Hermes moves the module, the verifier keeps working
and the wake simply stops mentioning LSP.

BOUNDS
------
A verifier step must not become the slow part of a delegation, so the scan
is capped on three axes: file count, wall-clock, and reported findings.
"""

from __future__ import annotations

import logging
import os
import subprocess
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Extensions worth asking a language server about. Anything else is either
# unserved or pure noise (lockfiles, generated bundles, data).
LSP_EXTENSIONS = {
    ".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs",
    ".go", ".rs", ".c", ".h", ".cc", ".cpp", ".hpp",
    ".java", ".sh", ".bash", ".rb", ".php", ".cs", ".ps1",
}

MAX_FILES = 40          # a task touching more than this is a refactor, not a fix
MAX_SECONDS = 25.0      # total wall-clock budget for the whole scan
MAX_REPORTED = 5        # findings quoted into the wake; the count is always exact
_GIT_TIMEOUT = 10.0

SEVERITY_ERROR = 1
SEVERITY_WARNING = 2


def _git(cwd: str, *args: str) -> List[str]:
    """Run one git command, return stdout lines. Empty list on any failure."""
    try:
        r = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True,
            timeout=_GIT_TIMEOUT, check=False, shell=False,
        )
    except Exception:  # noqa: BLE001 - git absent, cwd gone, timeout: all "no info"
        return []
    if r.returncode != 0:
        return []
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]


def collect_touched_files(cwd: Optional[str], *, limit: int = MAX_FILES) -> List[str]:
    """Absolute paths of source files the task created or modified.

    Uncommitted changes plus untracked files: Pi is not expected to commit,
    so the working tree IS the deliverable. Deletions are dropped (nothing
    to diagnose) and the list is capped.
    """
    if not cwd or not os.path.isdir(cwd):
        return []
    names = _git(cwd, "diff", "--name-only", "--diff-filter=d", "HEAD")
    names += _git(cwd, "ls-files", "--others", "--exclude-standard")
    out: List[str] = []
    seen = set()
    for name in names:
        if os.path.splitext(name)[1].lower() not in LSP_EXTENSIONS:
            continue
        path = os.path.abspath(os.path.join(cwd, name))
        if path in seen or not os.path.isfile(path):
            continue
        seen.add(path)
        out.append(path)
        if len(out) >= limit:
            break
    return out


def _service() -> Optional[Any]:
    """The host LSP service, or None when unavailable for any reason."""
    try:
        from agent.lsp import get_service  # type: ignore  # Hermes-internal
    except Exception:  # noqa: BLE001
        return None
    try:
        return get_service()
    except Exception:  # noqa: BLE001
        return None


def _server_id_for(path: str) -> Optional[str]:
    """Which language server would handle this file, if any."""
    try:
        from agent.lsp.servers import find_server_for_file  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    try:
        srv = find_server_for_file(path)
        return getattr(srv, "server_id", None) if srv else None
    except Exception:  # noqa: BLE001
        return None


def _running_server_ids(svc: Any) -> Optional[set]:
    """Server ids with a live client, or None when that cannot be determined."""
    try:
        status = svc.get_status() or {}
    except Exception:  # noqa: BLE001
        return None
    clients = status.get("clients")
    if clients is None:
        return None
    try:
        return {c.get("server_id") for c in clients if c.get("running")}
    except Exception:  # noqa: BLE001
        return None


def run(cwd: Optional[str], *, limit: int = MAX_FILES,
        budget_seconds: float = MAX_SECONDS,
        clock: Any = None) -> Optional[Dict[str, Any]]:
    """Diagnose the files a task touched.

    Returns ``None`` when no verdict could be reached — LSP disabled, not a
    git workspace, no servers, nothing to check. Callers MUST treat that as
    "unknown", never as "clean".

    On success returns a compact, JSON-safe dict:
    ``{"files": int, "errors": int, "warnings": int, "findings": [str, ...],
       "truncated": bool}``
    """
    import time as _time
    now = clock or _time.monotonic

    svc = _service()
    if svc is None:
        return None
    files = collect_touched_files(cwd, limit=limit)
    if not files:
        return None

    deadline = now() + budget_seconds
    errors = warnings = unavailable = 0
    findings: List[str] = []
    checked = 0
    truncated = False

    for path in files:
        if now() >= deadline:
            truncated = True
            break
        # An empty diagnostics list is NOT proof of a clean file: upstream
        # returns [] for "disabled", "no workspace", "no server matched",
        # "server could not spawn" and "timed out" as well. enabled_for()
        # rules out the first three; a live client for this file's server
        # rules out the rest. Anything we cannot positively confirm is
        # counted as unchecked, because reporting "clean" for a file no
        # server ever opened is the one failure mode that would make this
        # signal worse than having none.
        try:
            if not svc.enabled_for(path):
                unavailable += 1
                continue
        except Exception:  # noqa: BLE001
            unavailable += 1
            continue
        try:
            diags = svc.get_diagnostics_sync(
                path, delta=False, timeout=max(1.0, deadline - now()),
            ) or []
        except Exception as exc:  # noqa: BLE001 - never let LSP break a verdict
            logger.debug("pi-manager: LSP check failed for %s: %s", path, exc)
            unavailable += 1
            continue
        if not diags:
            running = _running_server_ids(svc)
            sid = _server_id_for(path)
            if running is None or sid is None or sid not in running:
                unavailable += 1
                continue
        checked += 1
        rel = os.path.relpath(path, cwd) if cwd else path
        for d in diags:
            sev = d.get("severity") or SEVERITY_ERROR
            if sev == SEVERITY_ERROR:
                errors += 1
            elif sev == SEVERITY_WARNING:
                warnings += 1
            else:
                continue
            if sev == SEVERITY_ERROR and len(findings) < MAX_REPORTED:
                line = ((d.get("range") or {}).get("start") or {}).get("line", 0)
                msg = str(d.get("message") or "").strip().splitlines()[:1]
                findings.append(f"{rel}:{int(line) + 1}: {msg[0] if msg else '?'}"[:200])

    if checked == 0:
        return None
    return {
        "files": checked,
        "errors": errors,
        "warnings": warnings,
        "unavailable": unavailable,
        "findings": findings,
        "truncated": truncated,
    }


def format_summary(result: Optional[Dict[str, Any]]) -> Optional[str]:
    """One line for the wake message, or None when there is no verdict."""
    if not result:
        return None
    files = result.get("files", 0)
    errors = int(result.get("errors") or 0)
    warnings = int(result.get("warnings") or 0)
    skipped = int(result.get("unavailable") or 0)
    if errors == 0 and warnings == 0:
        # NOT "clean". Upstream returns [] both for a genuinely clean file and
        # for "the server is alive but produced no fresh diagnostics within
        # the wait budget" — its own comment is "slow is not dead", and that
        # path deliberately does NOT mark the server broken, so neither
        # enabled_for() nor get_status() can tell the two apart. An empty
        # result is therefore evidence of nothing found, never proof that
        # nothing is there. LSP is a negative-signal detector here: errors it
        # reports are trustworthy and blocking; silence is not an acceptance
        # signal. The gate verdict remains the acceptance signal.
        head = f"no LSP errors detected in {files} touched file(s)"
        if skipped:
            head += (f"; {skipped} could not be checked (no server, or it "
                     f"did not respond)")
        return head
    parts = []
    if errors:
        parts.append(f"{errors} error(s)")
    if warnings:
        parts.append(f"{warnings} warning(s)")
    head = f"LSP found {' and '.join(parts)} in {files} touched file(s)"
    findings = result.get("findings") or []
    if findings:
        head += " — " + "; ".join(findings)
    if skipped:
        head += f"; {skipped} file(s) could not be checked"
    if result.get("truncated"):
        head += " [scan truncated by budget]"
    return head


__all__ = ["run", "format_summary", "collect_touched_files", "LSP_EXTENSIONS"]
