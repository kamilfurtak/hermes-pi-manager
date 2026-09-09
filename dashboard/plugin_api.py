"""Mounted by native hermes serve; authentication remains the host's job."""
import importlib.util
from pathlib import Path
import sqlite3

from fastapi import APIRouter, HTTPException, Query, Response

# The dashboard loader imports this file outside the Python plugin package.
# Load only the pure view module, never tools/get_manager/recovery workers.
_spec = importlib.util.spec_from_file_location(
    "pi_manager_activity_api", Path(__file__).resolve().parents[1] / "activity.py")
_view = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_view)
_transcript_spec = importlib.util.spec_from_file_location(
    "pi_manager_transcript_api", Path(__file__).resolve().parents[1] / "live_transcript.py")
_transcript = importlib.util.module_from_spec(_transcript_spec)
_transcript_spec.loader.exec_module(_transcript)
router = APIRouter()


def _profile_home(profile):
    from hermes_cli.config import get_process_hermes_home, load_config
    from hermes_cli.web_server_profiles import _resolve_profile_dir, _hermes_home_scope
    home = (_resolve_profile_dir(profile) if profile and profile != "current"
            else get_process_hermes_home())
    with _hermes_home_scope(home):
        plugins = load_config().get("plugins", {})
    if "pi-manager" not in plugins.get("enabled", []) or "pi-manager" in plugins.get("disabled", []):
        raise HTTPException(404, "Pi live view is not enabled for this profile")
    return home


@router.get("/activity")
def activity(response: Response, task_id: str = Query(min_length=1, max_length=160),
             session_id: str = Query(min_length=1, max_length=200), profile: str = "",
             transcript: bool = False, cursor: str = Query(default="", max_length=100)):
    response.headers["Cache-Control"] = "no-store"
    home = _profile_home(profile)
    try:
        result = _view.read_activity(home, task_id, session_id)
    except (KeyError, FileNotFoundError):
        raise HTTPException(404, "Pi task is not available in this conversation") from None
    except (sqlite3.Error, OSError, ValueError, TypeError):
        raise HTTPException(503, "Pi live view is temporarily unavailable") from None
    # Authorization above applies equally to the richer log. A display-file
    # failure must not hide the task's execution/verification status.
    if transcript:
        try:
            result["transcript"] = _transcript.read_transcript(
                Path(home) / "state" / "pi-manager" / "cli-transcripts", task_id, cursor)
        except OSError:
            result["transcript"] = {"available": False, "error": "Dziennik chwilowo niedostępny. Ponawiam odczyt…"}
    return result
