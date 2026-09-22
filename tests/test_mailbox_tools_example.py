"""Exercise the author-facing walkthrough across independent processes."""

import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "examples" / "mailboxes" / "request_reply.py"


@pytest.mark.parametrize("scenario", ["lookup", "clarify"])
def test_tool_wakes_caller_after_process_exit(tmp_path, scenario):
    database = tmp_path / "demo.db"

    def run(*args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(database), *args],
            check=True, capture_output=True, text=True,
        ).stdout

    def state(trip):
        with sqlite3.connect(database) as db:
            return json.loads(db.execute(
                "SELECT state FROM wake_sessions WHERE id = ?", (trip,)
            ).fetchone()[0])

    run("start", "--scenario", scenario)
    assert "waiting_for_tool" in run("tick")
    assert state("kyoto")["phase"] == "waiting_for_tool"
    run("tick")
    run("tick")
    assert "No runnable" in run("tick")

    if scenario == "clarify":
        assert state("kyoto/tool")["phase"] == "waiting_for_answer"
        assert "Morning or afternoon tour?" in run("show")
        # A second parked trip shares definitions but must not receive this answer.
        run("start", "--scenario", scenario, "--trip", "tokyo")
        for _ in range(3):
            run("tick")
        run("answer", "Afternoon")
        run("answer", "Afternoon")  # delivery retry, before incorporation
        for _ in range(3):
            run("tick")
        run("answer", "Afternoon")  # retry after incorporation cannot wake again
        assert state("tokyo")["phase"] == "waiting_for_user"
        assert state("tokyo/tool")["phase"] == "waiting_for_answer"

    assert state("kyoto")["phase"] == "done"
    assert "Mock" in state("kyoto")["result"]
    assert "No runnable" in run("tick")
    with sqlite3.connect(database) as db:
        ui = [json.loads(row[0]) for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = 'kyoto/ui'"
        )]
    assert sum(event["kind"] == "result" for event in ui) == 1
    assert "Mock" in run("show")
