"""Verify the two authoring styles against the real local session store."""

import json
from pathlib import Path
import sqlite3
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def run(module, *args):
    return subprocess.run(
        [sys.executable, "-m", f"examples.mailboxes.{module}", *map(str, args)],
        cwd=ROOT, check=True, capture_output=True, text=True, timeout=10,
    ).stdout


def test_resident_handles_other_mail_before_tool_result():
    output = run("resident")
    assert output.index("Dispatched weather") < output.index("Handled other mail")
    assert output.index("Handled other mail") < output.index("Ingested tool result")


def test_restart_dispatches_saved_step_without_repeating_work(tmp_path):
    database = tmp_path / "restart.db"
    assert "saved next=finish" in run("resumable", database, "start")
    saved = json.loads(run("resumable", database, "show"))
    assert saved["next"] == "finish"
    assert saved["itinerary"] == ["Walk around Kyoto"]
    assert "No mail" in run("resumable", database, "run")
    assert json.loads(run("resumable", database, "show")) == saved

    run("resumable", database, "tool")
    assert "Resumed finish" in run("resumable", database, "run")
    final = json.loads(run("resumable", database, "show"))
    assert final["itinerary"] == [
        "Walk around Kyoto", "Mock weather for Kyoto: sunny, 24 C.",
    ]
    assert "No mail" in run("resumable", database, "run")
    with sqlite3.connect(database) as db:
        assert db.execute(
            "SELECT count(*) FROM wake_inputs WHERE session_id = 'weather'"
        ).fetchone()[0] == 1
        assert db.execute(
            "SELECT status FROM wake_sessions WHERE id = 'agent'"
        ).fetchone()[0] == "complete"
