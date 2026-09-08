"""Wake correctness without graph, model, network or persistent workers."""

import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from entourage.sessions import LocalSessions, Publication, StaleActivation


@pytest.fixture
def sessions(tmp_path):
    now = [100.0]
    return LocalSessions(tmp_path / "sessions.db", clock=lambda: now[0]), now


def mail(event_id, content="hello"):
    return {"event_id": event_id, "kind": "user", "content": content}


def park(store, session_id="trip", state=None, deadline=None):
    store.create(session_id, "travel:v1", state or {})
    activation = store.claim()
    store.commit(activation, activation.state, incorporated=[], deadline=deadline)


def test_mail_arriving_during_activation_survives_park(sessions):
    store, _ = sessions
    store.create("trip", "travel:v1", {"phase": "start"})
    first = store.claim()
    store.append("trip", mail("answer"))
    assert store.claim() is None  # still leased
    store.commit(first, {"phase": "clarify"}, incorporated=[])

    resumed = store.claim()
    assert resumed.state == {"phase": "clarify"}
    assert resumed.events == [mail("answer")]
    store.commit(resumed, {"phase": "done"}, incorporated=["answer"])
    assert store.claim() is None
    assert store.append("trip", mail("answer")) is False
    assert store.claim() is None


def test_deadline_persists_and_retry_keeps_timer_identity(sessions):
    store, now = sessions
    park(store, deadline=110)
    assert store.claim() is None
    now[0] = 110
    first = store.claim(lease_seconds=5)
    assert first.events[0]["source"] == "timer"
    assert first.events[0]["payload"] == {"deadline": 110}
    now[0] = 115
    retry = store.claim()
    assert retry.events == first.events
    store.commit(retry, {"timed_out": True},
                 incorporated=[retry.events[0]["event_id"]])
    assert store.claim() is None


def test_stale_activation_cannot_overwrite_state_or_publish(sessions):
    store, now = sessions
    store.create("trip", "travel:v1", {})
    store.create("child", "booking:v1", {})
    first = store.claim(executable="travel:v1", lease_seconds=1)
    now[0] += 1
    with pytest.raises(StaleActivation):
        store.commit(first, {"stale": True}, incorporated=[])
    current = store.claim(executable="travel:v1")
    with pytest.raises(StaleActivation):
        store.commit(first, {}, incorporated=[], publish=(Publication("child", mail("bad")),))
    store.commit(current, {"accepted": True}, incorporated=[])
    child = store.claim(executable="booking:v1")
    assert child.events == []


def test_failed_publication_rolls_back_checkpoint_and_all_mail(sessions):
    store, _ = sessions
    park(store, "child")
    store.create("parent", "travel:v1", {})
    store.append("parent", mail("input"))
    activation = store.claim()
    publications = (Publication("child", mail("request")),
                    Publication("missing", mail("invalid")))
    with pytest.raises(KeyError):
        store.commit(activation, {"changed": True}, incorporated=["input"],
                     publish=publications)
    assert store.claim() is None  # child's first publication rolled back
    store.commit(activation, {"changed": True}, incorporated=["input"],
                 publish=publications[:1])
    child = store.claim()
    assert child.session_id == "child"
    assert child.events == [mail("request")]


def test_two_sessions_share_code_but_not_state_or_mail(sessions):
    store, _ = sessions
    park(store, "a", {"city": "Kyoto"})
    park(store, "b", {"city": "Tokyo"})
    store.append("b", mail("answer", "two people"))
    resumed = store.claim(executable="travel:v1")
    assert resumed.session_id == "b"
    assert resumed.state == {"city": "Tokyo"}
    assert resumed.events == [mail("answer", "two people")]
    assert store.claim(executable="unknown:v1") is None


def test_concurrent_workers_accept_only_one_lease(sessions):
    store, _ = sessions
    store.create("trip", "travel:v1", {})
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(lambda _: store.claim(), range(4)))
    assert sum(result is not None for result in results) == 1


def test_completion_cannot_discard_mail_and_is_terminal(sessions):
    store, _ = sessions
    store.create("trip", "travel:v1", {})
    activation = store.claim()
    store.append("trip", mail("late"))
    with pytest.raises(ValueError, match="unincorporated"):
        store.commit(activation, {}, incorporated=[], complete=True)
    with pytest.raises(ValueError, match="absent"):
        store.commit(activation, {}, incorporated=["late"])
    store.commit(activation, {}, incorporated=[])
    resumed = store.claim()
    store.commit(resumed, {}, incorporated=["late"], complete=True)
    assert store.claim() is None
    assert store.append("trip", mail("late")) is False
    with pytest.raises(ValueError, match="complete"):
        store.append("trip", mail("new"))


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


def test_external_mail_cannot_shadow_a_deadline_event(sessions):
    store, _ = sessions
    park(store)
    with pytest.raises(ValueError, match="reserved"):
        store.append("trip", mail("timer:trip:1"))
