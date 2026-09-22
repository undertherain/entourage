"""SQLite-specific recovery across fresh processes and abrupt exits."""

import json
import subprocess
import sys

from entourage.sessions import LocalSessions


def mail(event_id, content="hello"):
    return {"event_id": event_id, "kind": "user", "content": content}


def park(store, session_id="trip", state=None, deadline=None):
    store.create(session_id, "travel:v1", state or {})
    activation = store.claim()
    store.commit(activation, activation.state, incorporated=[], deadline=deadline)


def test_restart_reconstructs_readiness_in_fresh_processes(tmp_path):
    path = tmp_path / "sessions.db"
    script = """
import json, sys
from pathlib import Path
from entourage.sessions import LocalSessions
store = LocalSessions(Path(sys.argv[1]))
if sys.argv[2] == 'park':
    store.create('trip', 'travel:v1', {})
    activation = store.claim()
    store.commit(activation, {'phase': 'clarify'}, incorporated=[])
else:
    activation = store.claim()
    print(json.dumps({'state': activation.state, 'events': activation.events}))
    store.commit(activation, {'phase': 'done'}, incorporated=['answer'])
"""
    subprocess.run([sys.executable, "-c", script, str(path), "park"], check=True)
    store = LocalSessions(path)
    assert store.claim() is None
    store.append("trip", mail("answer"))
    result = subprocess.run([sys.executable, "-c", script, str(path), "resume"],
                            check=True, capture_output=True, text=True)
    assert json.loads(result.stdout) == {
        "state": {"phase": "clarify"}, "events": [mail("answer")],
    }
    assert store.claim() is None


def test_abrupt_process_exit_replays_uncommitted_input(tmp_path):
    path = tmp_path / "sessions.db"
    store = LocalSessions(path, clock=lambda: 100)
    store.create("trip", "travel:v1", {"phase": "start"})
    store.append("trip", mail("request"))
    script = """
import os, sys
from pathlib import Path
from entourage.sessions import LocalSessions
store = LocalSessions(Path(sys.argv[1]), clock=lambda: 100)
assert store.claim(lease_seconds=1).events[0]['event_id'] == 'request'
os._exit(17)
"""
    result = subprocess.run([sys.executable, "-c", script, str(path)])
    assert result.returncode == 17
    assert store.claim() is None
    recovered = LocalSessions(path, clock=lambda: 101).claim()
    assert recovered.state == {"phase": "start"}
    assert recovered.events == [mail("request")]


def test_exit_after_commit_preserves_state_and_exactly_one_local_dispatch(tmp_path):
    path = tmp_path / "sessions.db"
    store = LocalSessions(path)
    park(store, "child")
    store.create("parent", "travel:v1", {})
    store.append("parent", mail("input"))
    script = """
import os, sys
from pathlib import Path
from entourage.sessions import LocalSessions, Publication
store = LocalSessions(Path(sys.argv[1]))
activation = store.claim()
store.commit(activation, {'phase': 'delegated'}, incorporated=['input'],
             publish=(Publication('child', {'event_id': 'request', 'kind': 'user'}),))
os._exit(17)
"""
    assert subprocess.run([sys.executable, "-c", script, str(path)]).returncode == 17
    child = store.claim()
    assert child.session_id == "child"
    assert child.events == [{"event_id": "request", "kind": "user"}]
    store.commit(child, {}, incorporated=["request"], complete=True)
    assert store.claim() is None
    store.append("parent", mail("reply"))
    parent = store.claim()
    assert parent.state == {"phase": "delegated"}
    assert parent.events == [mail("reply")]
