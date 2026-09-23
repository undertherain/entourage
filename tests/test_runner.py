"""The runner reads the store and manages processes; workers decide their own exit."""

import json
import os
from pathlib import Path
import sqlite3
import sys
import textwrap
from threading import Event
import time

import pytest

from entourage.runner import Launcher, ProcessLauncher, Runner, Running, Shard
from entourage.sessions import LocalSessions


ROOT = Path(__file__).resolve().parents[1]

AGENTS = {
    "echo": "def resume(ctx, state, mail):\n"
            "    return ctx.propose({'seen': state.get('seen', 0) + len(mail)},"
            " incorporated=[e['event_id'] for e in mail])\n",
    "sleeper": "import time\n"
               "def resume(ctx, state, mail):\n"
               "    time.sleep(30)\n"
               "    return ctx.propose(state)\n",
    "eager": "def resume(ctx, state, mail):\n"
             "    return ctx.propose(state, incorporated=[e['event_id'] for e in mail])\n",
}


def write_shard(root: Path, manifest: str):
    for alias, code in AGENTS.items():
        folder = root / alias
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "agent.py").write_text(code)
        (folder / "agent.yaml").write_text(
            f"definition: {alias}:v1\nprotocol: entourage.activation/v1\n"
            "entrypoint: agent.py:resume\ninbox: session\n")
    (root / "shard.yaml").write_text(textwrap.dedent(manifest))
    return Shard.from_manifest(root / "shard.yaml")


class FakeLauncher(Launcher):
    def __init__(self, clock):
        self.clock = clock
        self.alive = {}
        self.log = []

    def start(self, name, member):
        self.alive[name] = Running(member.alias, self.clock())
        self.log.append(("start", member.alias))

    def running(self):
        return dict(self.alive)

    def stop(self, name):
        self.alive.pop(name, None)
        self.log.append(("stop", name))

    def kill(self, name):
        self.alive.pop(name, None)
        self.log.append(("kill", name))


@pytest.fixture
def policy(tmp_path):
    shard = write_shard(tmp_path, """
        shard: test
        store: state/sessions.db
        pool: 1
        notify: ops
        executables:
          eager:
            folder: ./eager
            runner: {start: eager}
          echo:
            folder: ./echo
            runner: {start: on_demand, parallel: 2}
            worker: {idle_exit: 1, lease: 5, max_attempts: 2}
          sleeper:
            folder: ./sleeper
            runner: {start: on_demand, parallel: 1}
            worker: {lease: 1, max_attempts: 1}
        """)
    shard.store.parent.mkdir()
    now = [1000.0]
    store = LocalSessions(shard.store, clock=lambda: now[0])
    launcher = FakeLauncher(lambda: now[0])
    runner = Runner(shard, store, launcher, clock=lambda: now[0])
    store.create("ops", "ops:v1", {})
    return shard, store, launcher, runner, now


def test_manifest_defaults_and_policy_split(policy):
    shard, *_ = policy
    eager, echo = shard.members["eager"], shard.members["echo"]
    assert (eager.start, eager.reserved, eager.parallel, eager.idle_exit) == ("eager", 1, 1, None)
    assert (echo.start, echo.reserved, echo.parallel, echo.idle_exit) == ("on_demand", 0, 2, 1.0)
    assert echo.worker_args() == ["--lease", "5.0", "--max-attempts", "2", "--idle-exit", "1.0"]
    assert eager.definition == "eager:v1" and shard.notify == "ops" and shard.pool == 1


def test_eager_starts_at_boot_and_restarts_with_backoff_after_quick_exit(policy):
    shard, store, launcher, runner, now = policy
    assert runner.tick() == ["eager"]
    assert runner.tick() == []  # one reserved worker is enough, no mail needed
    (name,) = launcher.alive
    launcher.alive.clear()  # crashed 2 seconds after start
    now[0] += 2
    assert runner.tick() == []  # quick exit: backoff, not an immediate restart
    now[0] += 1
    assert runner.tick() == ["eager"]
    launcher.alive.clear()
    now[0] += 60  # ran for a minute: healthy, backoff cleared
    assert runner.tick() == ["eager"]


def test_on_demand_starts_for_ready_work_only_and_respects_idle_workers_and_pool(policy):
    shard, store, launcher, runner, now = policy
    runner.tick()  # eager
    assert runner.tick() == []  # nothing ready for on-demand members
    store.create("echo:a", "echo:v1", {})
    assert runner.tick() == ["echo"]
    store.create("echo:b", "echo:v1", {})
    assert runner.tick() == []  # an idle worker exists; it will take echo:a then echo:b
    (worker,) = [n for n, r in launcher.alive.items() if r.alias == "echo"]
    store.claim(executable="echo:v1", worker=worker)  # now busy on echo:a
    assert runner.tick() == []  # pool of one shared slot is used by that worker
    store.create("sleeper:x", "sleeper:v1", {})
    assert runner.tick() == []  # still pool-bound
    launcher.alive.pop(worker)
    now[0] += 30
    assert runner.tick() in (["echo"], ["sleeper"])  # slot freed, one of them starts


def test_stuck_holder_is_killed_and_failed_sessions_are_reported_once(policy):
    shard, store, launcher, runner, now = policy
    store.create("sleeper:x", "sleeper:v1", {})
    assert runner.tick() == ["eager", "sleeper"]
    (worker,) = [n for n, r in launcher.alive.items() if r.alias == "sleeper"]
    store.claim(executable="sleeper:v1", worker=worker, lease_seconds=1, max_attempts=1)
    assert runner.tick() == []  # lease live, worker busy
    now[0] += 1.5
    runner.tick()
    assert ("kill", worker) in launcher.log and worker not in launcher.alive
    assert store.claim(executable="sleeper:v1", max_attempts=1) is None  # parked failed
    assert store.inspect("sleeper:x")["status"] == "failed"
    runner.tick()
    runner.tick()
    with sqlite3.connect(store.path) as db:
        notices = [json.loads(row[0]) for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = 'ops'")]
    assert len(notices) == 1
    assert notices[0]["payload"] == {"failed": "sleeper:x", "executable": "sleeper:v1", "attempts": 1}
    assert notices[0]["source"] == "runner"


def wait_for(predicate, timeout=15, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.fixture
def processes(tmp_path):
    shard = write_shard(tmp_path, """
        shard: proc
        store: state/sessions.db
        notify: ops
        executables:
          echo:
            folder: ./echo
            runner: {start: on_demand}
            worker: {idle_exit: 0.5, lease: 5}
          sleeper:
            folder: ./sleeper
            runner: {start: on_demand}
            worker: {lease: 1, max_attempts: 1, idle_exit: 0.5}
        """)
    shard.store.parent.mkdir()
    store = LocalSessions(shard.store)
    store.create("ops", "ops:v1", {})
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONDONTWRITEBYTECODE": "1"}
    launcher = ProcessLauncher(shard, env=env, stop_grace=2)
    runner = Runner(shard, store, launcher, quick_exit=0.0)
    yield shard, store, launcher, runner
    launcher.stop_all()


def test_worker_process_serves_ready_mail_then_exits_idle(processes):
    shard, store, launcher, runner = processes
    store.create("echo:1", "echo:v1", {})
    store.append("echo:1", {"event_id": "m1"})
    assert runner.tick() == ["echo"]
    assert wait_for(lambda: store.inspect("echo:1")["state"] == {"seen": 1})
    assert wait_for(lambda: not launcher.running())  # idle exit, decided by the worker
    assert runner.tick() == []
    store.append("echo:1", {"event_id": "m2"})
    assert runner.tick() == ["echo"]  # cold start for the follow-up
    assert wait_for(lambda: store.inspect("echo:1")["state"] == {"seen": 2})


def test_stuck_worker_process_is_killed_then_session_fails_and_ops_is_notified(processes):
    shard, store, launcher, runner = processes
    store.create("sleeper:1", "sleeper:v1", {})
    assert runner.tick() == ["sleeper"]
    assert wait_for(lambda: store.list_sessions(status="active"))
    (name,) = launcher.running()
    assert store.list_sessions(status="active")[0]["worker"] == name
    ticks = []

    def drive():
        ticks.extend(runner.tick())
        return store.inspect("sleeper:1")["status"] == "failed"

    assert wait_for(drive, timeout=10, interval=0.3)
    assert name not in launcher.running()  # the stuck holder was killed
    assert ticks == ["sleeper"]  # exactly one retry worker; it parked the session as failed
    assert wait_for(lambda: not launcher.running())
    runner.tick()
    ready = [r["session_id"] for r in store.list_sessions(ready=True)]
    assert ready == ["ops"]  # only the notified session has work; sleeper stays parked
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT COUNT(*) FROM wake_inputs WHERE session_id = 'ops'").fetchone()[0] == 1
