"""The standalone session-core examples run and print what their docstrings promise."""

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def run_example(name):
    return subprocess.run([sys.executable, f"examples/{name}.py"], cwd=ROOT, check=True,
                          capture_output=True, text=True, timeout=30).stdout


def test_spawn_supervisor_example():
    output = run_example("spawn_supervisor")
    assert "join: child reported 'cat_64px.png'" in output
    assert "'ok' finished fine" in output
    assert "death notice for ['doomed']" in output
    assert "all children accounted for" in output
    assert "timer woke us with 'job-9' still pending" in output


def test_waiting_session_example():
    output = run_example("waiting_session")
    assert "got user event: 'my printer is on fire'" in output
    assert "[bob is waiting, holding no worker]" in output
    assert "got user event: 'nevermind, fixed it'" in output
    assert "[bob is complete]" in output
    assert "the timer woke us" in output
    assert "[carol is complete after 2 wakes]" in output


def test_remote_tool_ingress_example():
    output = run_example("remote_tool_ingress")
    act1, act2 = output.split("Act 2")
    assert "report: rain in Tokyo (as if it were an inline tool call)" in act1
    assert "dropping ['forecast'] and moving on" in act2
    assert "late result 'rain in Tokyo' is ambient mail" in act2


def test_retry_timeout_example():
    output = run_example("retry_timeout")
    assert "flaky_api: attempt 3 succeeds" in output
    assert "flaky: complete, state={'data': 'payload'}" in output
    assert "broken: failed" in output
    assert "last error: \"RuntimeError('permanently misconfigured')\"" in output
    assert "slow_step: attempt 2 is quick" in output
    assert "slow: complete, state={'done_by_attempt': 2}" in output
