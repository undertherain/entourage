"""The registered boundary: restoration, proposal ownership and local replay."""

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from threading import Event, Thread

import pytest

from entourage.executables import Dispatcher, Executable, Proposal
from entourage.sessions import LocalSessions, StaleActivation


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples/mailboxes/registered"


def test_dispatcher_import_does_not_load_a_concrete_session_backend():
    script = """
import sys
from entourage.executables import Dispatcher
from entourage.session_backend import SessionBackend
assert 'entourage.sessions' not in sys.modules
assert 'sqlite3' not in sys.modules
assert Dispatcher.__init__.__annotations__['store'] is SessionBackend
"""
    subprocess.run([sys.executable, "-c", script], cwd=ROOT, check=True,
                   capture_output=True, text=True, timeout=10)


@pytest.fixture
def runtime(tmp_path):
    now = [100.0]
    store = LocalSessions(tmp_path / "sessions.db", clock=lambda: now[0])
    return store, now


def inputs(store, destination):
    with sqlite3.connect(store.path) as db:
        return [json.loads(row[0]) for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = ? ORDER BY seq", (destination,))]


def test_copied_inputs_cannot_forge_acknowledgments(runtime):
    store, now = runtime
    attempts = []

    def resume(ctx, state, mail):
        assert not hasattr(ctx, "token") and not hasattr(ctx, "store")
        attempts.append(ctx.activation_id)
        assert state == {"saved": True}
        assert mail[0]["event_id"] == "real"
        if len(attempts) == 1:
            state["corrupt"] = True
            mail[0]["event_id"] = "forged"
            ctx.send("output", {}, key="must-rollback")
            return ctx.propose(state, incorporated=["forged"])
        return ctx.propose(state, incorporated=["real"])

    dispatcher = Dispatcher(store, lease_seconds=1).register(Executable("test:v1", resume))
    store.create("output", "output:v1", {})
    dispatcher.create("a", "test:v1", {"saved": True})
    store.append("a", {"event_id": "real"})
    failure = dispatcher.run_once()
    assert not failure.committed and "absent" in str(failure.error)
    assert inputs(store, "output") == []
    assert store.inspect("a")["state"] == {"saved": True}
    assert dispatcher.run_once() is None
    now[0] += 1
    assert dispatcher.run_once().committed
    assert len(set(attempts)) == 2


def test_late_mail_is_not_acknowledged_and_stale_proposal_cannot_publish(runtime):
    store, now = runtime

    def resume(ctx, state, mail):
        if not mail:
            store.append(ctx.session_id, {"event_id": "late"})
            now[0] += 1
        ctx.send("output", {}, key="answer")
        return ctx.propose({"seen": [e["event_id"] for e in mail]},
                           incorporated=[e["event_id"] for e in mail])

    dispatcher = Dispatcher(store, lease_seconds=1).register(Executable("test:v1", resume))
    store.create("output", "output:v1", {})
    dispatcher.create("a", "test:v1", {})
    failure = dispatcher.run_once()
    assert isinstance(failure.error, StaleActivation)
    assert inputs(store, "output") == []
    assert dispatcher.run_once().committed
    assert store.inspect("a")["state"] == {"seen": ["late"]}
    assert len(inputs(store, "output")) == 1


def test_bounded_batches_rotate_sessions_and_definitions(runtime):
    store, _ = runtime
    batches = []

    def resume(ctx, state, mail):
        batches.append((ctx.session_id, len(mail), ctx.has_more))
        return ctx.propose(state, incorporated=[e["event_id"] for e in mail])

    dispatcher = Dispatcher(store, max_events=1)
    dispatcher.register(Executable("test:v1", resume)).register(Executable("other:v1", resume))
    dispatcher.create("a", "test:v1", {})
    dispatcher.create("b", "test:v1", {})
    dispatcher.create("c", "other:v1", {})
    for i in range(3):
        store.append("a", {"event_id": str(i)})
    assert len(dispatcher.run_until_idle(max_activations=2)) == 2
    assert all(r.committed for r in dispatcher.run_until_idle())
    assert batches == [("a", 1, True), ("c", 0, False), ("b", 0, False),
                       ("a", 1, True), ("a", 1, False)]


def test_rejects_unknown_duplicate_and_changed_definitions(runtime):
    store, _ = runtime

    def resume(ctx, state, mail):
        return ctx.propose(state)

    definition = Executable("test:v1", resume)
    dispatcher = Dispatcher(store).register(definition)
    with pytest.raises(KeyError):
        dispatcher.create("a", "missing:v1", {})
    with pytest.raises(ValueError, match="duplicate"):
        dispatcher.register(definition)
    fresh = Dispatcher(store).register(definition)
    fresh.create("a", "test:v1", {})
    with pytest.raises(ValueError, match="new version"):
        Dispatcher(store).register(replace(definition, state_schema=2))
    with pytest.raises(ValueError, match="new version"):
        Dispatcher(store).register(replace(definition, config={"changed": True}))
    # Unknown executable versions remain unclaimed by this worker.
    store.create("external", "unregistered:v1", {})
    assert fresh.run_once().committed
    assert fresh.run_once() is None
    assert store.inspect("external")["status"] == "ready"


def test_manifest_detects_source_and_resource_changes(tmp_path):
    source = tmp_path / "agent.py"
    source.write_text("def resume(ctx, state, mail):\n    return ctx.propose(state)\n")
    prompt = tmp_path / "prompt.md"
    prompt.write_text("original")
    manifest = tmp_path / "agent.yaml"
    manifest.write_text("definition: test:v1\nprotocol: entourage.activation/v1\n"
                        "inbox: session\nentrypoint: agent.py:resume\nresources: [prompt.md]\n")
    store = LocalSessions(tmp_path / "sessions.db")
    Dispatcher(store).register(Executable.from_manifest(manifest))
    Dispatcher(store).register(Executable.from_manifest(manifest))
    prompt.write_text("changed")
    with pytest.raises(ValueError, match="new version"):
        Dispatcher(store).register(Executable.from_manifest(manifest))
    prompt.write_text("original")
    source.write_text(source.read_text() + "# edit\n")
    with pytest.raises(ValueError, match="new version"):
        Dispatcher(store).register(Executable.from_manifest(manifest))
    manifest.write_text(manifest.read_text().replace("test:v1", "test:dev") + "development: true\n")
    Dispatcher(store).register(Executable.from_manifest(manifest))
    source.write_text(source.read_text() + "# another edit\n")
    Dispatcher(store).register(Executable.from_manifest(manifest))


def test_resident_loop_commits_and_stops_without_holding_lease(runtime):
    store, _ = runtime
    stop = Event()
    committed = Event()

    def resume(ctx, state, mail):
        return ctx.propose({"seen": len(mail)}, incorporated=[e["event_id"] for e in mail])

    dispatcher = Dispatcher(store).register(Executable("test:v1", resume))
    dispatcher.create("a", "test:v1", {})
    assert dispatcher.run_once().committed
    worker = Thread(target=dispatcher.run_forever, args=(stop,),
                    kwargs={"poll_interval": 0.01, "on_result": lambda r: committed.set()})
    worker.start()
    try:
        store.append("a", {"event_id": "wake"})
        assert committed.wait(3)
    finally:
        stop.set()
        worker.join(3)
    assert not worker.is_alive()
    assert store.inspect("a")["status"] == "waiting"
    assert store.inspect("a")["state"] == {"seen": 1}
    # A new process/worker is free to claim immediately after more mail arrives.
    store.append("a", {"event_id": "again"})
    assert store.claim(executable="test:v1") is not None


@pytest.mark.parametrize("moment", ["before", "after"])
def test_abrupt_exit_at_dispatcher_checkpoint(tmp_path, moment):
    source = tmp_path / "agent.py"
    source.write_text('''import os
def resume(ctx, state, mail):
    ctx.send("output", {"value": 1}, key="request:1")
    if os.environ.get("CRASH_AT") == "before":
        os._exit(17)
    return ctx.propose({"saved": True}, incorporated=[e["event_id"] for e in mail])
''')
    manifest = tmp_path / "agent.yaml"
    manifest.write_text("definition: test:v1\nprotocol: entourage.activation/v1\n"
                        "inbox: session\nentrypoint: agent.py:resume\n")
    store = LocalSessions(tmp_path / "sessions.db", clock=lambda: 100)
    dispatcher = Dispatcher(store).register(Executable.from_manifest(manifest))
    dispatcher.create("a", "test:v1", {})
    store.create("output", "output:v1", {})
    store.append("a", {"event_id": "input"})
    script = '''import os, sys
from pathlib import Path
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions
os.environ["CRASH_AT"] = sys.argv[3]
d = Dispatcher(LocalSessions(Path(sys.argv[1]), clock=lambda: 100), lease_seconds=1)
d.register(Executable.from_manifest(Path(sys.argv[2])))
assert d.run_once().committed
os._exit(17)
'''
    result = subprocess.run([sys.executable, "-c", script, store.path, str(manifest), moment],
                            timeout=10, capture_output=True, text=True)
    assert result.returncode == 17, result.stderr
    recovered = Dispatcher(LocalSessions(Path(store.path), clock=lambda: 101))
    recovered.register(Executable.from_manifest(manifest))
    if moment == "before":
        assert store.inspect("a")["state"] == {}
        assert inputs(store, "output") == []
        assert recovered.run_once().committed
    else:
        assert recovered.run_once() is None
    assert store.inspect("a")["state"] == {"saved": True}
    assert len(inputs(store, "output")) == 1
    assert recovered.run_once() is None


def run_example(database, command, *args):
    result = subprocess.run([sys.executable, "-m", "examples.mailboxes.registered.run",
                             str(database), command, *args], cwd=ROOT, check=True,
                            capture_output=True, text=True, timeout=10)
    return result.stdout


def test_example_observation_and_tool_do_not_require_agent_source(tmp_path, monkeypatch, capsys):
    from examples.mailboxes.registered import run as cli

    database = tmp_path / "demo.db"
    run_example(database, "start")
    original_loader = Executable.from_manifest

    def load_available_definition(path):
        if path.name == "agent.yaml":
            raise FileNotFoundError("agent source no longer available")
        return original_loader(path)

    monkeypatch.setattr(Executable, "from_manifest", staticmethod(load_available_definition))
    monkeypatch.setattr(sys, "argv", ["run", str(database), "show"])
    cli.main()
    saved = json.loads(capsys.readouterr().out)
    assert saved["research"]["state"]["phase"] == "awaiting_sources"
    assert saved["events"]["status"] == "complete"

    monkeypatch.setattr(sys, "argv", ["run", str(database), "tool"])
    cli.main()
    assert "Committed sources" in capsys.readouterr().out
    store = LocalSessions(database)
    assert any(event["kind"] == "result" for event in inputs(store, "research"))


def test_two_sessions_correction_restart_and_duplicate_delivery(tmp_path):
    database = tmp_path / "demo.db"
    run_example(database, "start")
    store = LocalSessions(database)
    initial = store.inspect("research")["state"]
    assert initial["phase"] == "awaiting_sources"
    assert store.inspect("events")["status"] == "complete"
    assert len(inputs(store, "ui")) == 1  # Events finishes independently.

    run_example(database, "correct", "Quiet indoor activities in Kyoto")
    amended = store.inspect("research")["state"]
    assert amended["brief"] == "Quiet indoor activities in Kyoto"
    assert amended["pending"] == initial["pending"]
    assert store.inspect("research")["status"] == "waiting"
    assert len(inputs(store, "ui")) == 1  # No premature research answer.
    assert not store.append("research", inputs(store, "research")[0])
    request = inputs(store, "sources")[0]
    assert not store.append("sources", request)
    # Even a transport delivering the same logical request under another event ID
    # produces a single local reply, because its correlated operation key is stable.
    store.append("sources", {**request, "event_id": "transport-copy"})
    run_example(database, "tool")
    results = [e for e in inputs(store, "research") if e["kind"] == "result"]
    assert len(results) == 1
    assert not store.append("research", results[0])
    run_example(database, "tick")
    final = store.inspect("research")
    assert final["status"] == "complete"
    assert "Quiet indoor activities in Kyoto" in final["state"]["answer"]
    assert "pending" not in final["state"]
    assert len(inputs(store, "ui")) == 2
    assert run_example(database, "tick") == ""


def test_result_and_correction_split_across_batches(tmp_path):
    store = LocalSessions(tmp_path / "sessions.db")
    store.create("ui", "output:v1", {})
    store.create("sources", "sources:v1", {})
    dispatcher = Dispatcher(store, max_events=1).register(
        Executable.from_manifest(EXAMPLE / "agent.yaml"))
    dispatcher.create("research", "specialist:v1", {
        "task": "research", "phase": "ready", "brief": "Outdoor activities",
    })
    assert dispatcher.run_once().committed
    tool = Dispatcher(store).register(Executable.from_manifest(EXAMPLE / "tool.yaml"))
    assert tool.run_once().committed
    store.append("research", {"event_id": "correction", "kind": "user",
                              "payload": {"brief": "Indoor activities"}})
    assert dispatcher.run_once().committed  # Save result; correction is still pending.
    assert inputs(store, "ui") == []
    assert dispatcher.run_once().committed
    assert "Indoor activities" in store.inspect("research")["state"]["answer"]


def test_invalid_proposal_leaves_state_and_mail_untouched(runtime):
    store, now = runtime

    def resume(ctx, state, mail):
        return Proposal({"invalid": float("nan")}, incorporated=("input",))

    dispatcher = Dispatcher(store).register(Executable("test:v1", resume))
    dispatcher.create("a", "test:v1", {})
    store.append("a", {"event_id": "input"})
    assert not dispatcher.run_once().committed
    assert store.inspect("a")["state"] == {}
    now[0] += 30
    assert store.claim().events == [{"event_id": "input"}]


def test_old_database_restores_waiting_session_after_upgrade(tmp_path):
    database = tmp_path / "legacy.db"
    with sqlite3.connect(database) as db:
        db.execute("""CREATE TABLE wake_sessions (
            id TEXT PRIMARY KEY, executable TEXT NOT NULL, state TEXT NOT NULL,
            status TEXT NOT NULL, deadline REAL, token TEXT, lease_until REAL,
            revision INTEGER NOT NULL DEFAULT 0
        )""")
        db.execute("INSERT INTO wake_sessions (id, executable, state, status, revision) "
                   "VALUES ('legacy', 'test:v1', ?, 'waiting', 4)",
                   (json.dumps({"brief": "saved before upgrade"}),))

    def resume(ctx, state, mail):
        assert state == {"brief": "saved before upgrade"}
        assert mail == [{"event_id": "reply"}]
        return ctx.propose({**state, "resumed": True}, incorporated=["reply"])

    store = LocalSessions(database)
    dispatcher = Dispatcher(store).register(Executable("test:v1", resume))
    assert dispatcher.run_once() is None
    store.append("legacy", {"event_id": "reply"})
    assert dispatcher.run_once().committed
    fresh = LocalSessions(database)
    assert fresh.inspect("legacy")["revision"] == 5
    assert fresh.inspect("legacy")["state"] == {"brief": "saved before upgrade", "resumed": True}


def test_context_spawn_creates_child_and_briefs_it_atomically(runtime):
    store, now = runtime
    attempts = []

    def concierge(ctx, state, mail):
        attempts.append(ctx.activation_id)
        child = ctx.spawn("research:v1", {"phase": "ready"}, key="research:1")
        state["pending"] = ctx.request(child, {"brief": "Kyoto"}, key="research:1")
        state["child"] = child
        if len(attempts) == 1:
            raise RuntimeError("crash after staging, before commit")
        return ctx.propose(state, incorporated=[e["event_id"] for e in mail])

    def research(ctx, state, mail):
        request = mail[0]
        assert request["kind"] == "request" and request["reply_to"] == "concierge-main"
        ctx.reply(request, {"sources": ["guide"]})
        return ctx.propose({"phase": "done"}, incorporated=[request["event_id"]],
                           complete=True)

    dispatcher = (Dispatcher(store, lease_seconds=1)
                  .register(Executable("concierge:v1", concierge))
                  .register(Executable("research:v1", research)))
    dispatcher.create("concierge-main", "concierge:v1", {})
    store.append("concierge-main", {"event_id": "ask", "kind": "user", "payload": {}})
    failed = dispatcher.run_once()
    assert not failed.committed
    with pytest.raises(KeyError):
        store.inspect("concierge-main:research:1")  # nothing staged leaked
    now[0] += 1
    assert dispatcher.run_once().committed  # retry spawns the same child once
    child = store.inspect("concierge-main:research:1")
    assert child["executable"] == "research:v1" and child["state"] == {"phase": "ready"}
    assert dispatcher.run_once().committed  # research answers and completes
    resumed = store.claim(executable="concierge:v1")
    assert resumed.events[0]["kind"] == "result"
    assert resumed.events[0]["request_id"] == resumed.state["pending"]
    assert resumed.events[0]["source"] == "concierge-main:research:1"
    store.commit(resumed, resumed.state, incorporated=[resumed.events[0]["event_id"]])
    assert store.inspect("concierge-main:research:1")["status"] == "complete"


def test_repeated_logical_spawn_is_rejected_not_duplicated(runtime):
    store, now = runtime

    def resume(ctx, state, mail):
        ctx.spawn("research:v1", {}, key="research:1")
        return ctx.propose(state, incorporated=[e["event_id"] for e in mail])

    def child(ctx, state, mail):
        return ctx.propose(state, incorporated=[], complete=True)

    dispatcher = (Dispatcher(store).register(Executable("concierge:v1", resume))
                  .register(Executable("research:v1", child)))
    dispatcher.create("concierge-main", "concierge:v1", {})
    assert [r.committed for r in dispatcher.run_until_idle()] == [True, True]
    store.append("concierge-main", {"event_id": "again"})
    (result,) = dispatcher.run_until_idle()
    assert not result.committed and "concierge-main:research:1" in str(result.error)
    assert store.inspect("concierge-main")["revision"] == 1
    assert store.inspect("concierge-main:research:1")["status"] == "complete"
    assert inputs(store, "concierge-main")[0]["event_id"] == "again"
