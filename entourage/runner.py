"""Shard runner: start, stop and kill worker processes by reading the store.

The runner never talks to a worker. It reads the session store to learn what is
ready, who holds which lease and what has failed, and it manages processes
through a Launcher (plain subprocesses, or podman containers). Workers decide
their own exit; the runner decides starts, kills stuck holders and notifies.

    python -m entourage.runner shard.yaml

Shard manifest:

    shard: second-brain
    store: ./state/sessions.db
    pool: 4                      # shared on-demand worker slots
    notify: concierge-main       # optional session receiving failure notices
    launcher: {kind: process}    # or {kind: podman, image: localhost/entourage-runtime, env: [OPENAI_API_KEY]}
    executables:
      concierge:
        folder: ./concierge      # contains agent.yaml
        runner: {start: eager, reserved: 1}
        worker: {idle_exit: never}
      events:
        folder: ./events
        runner: {start: on_demand, parallel: 2}
        worker: {idle_exit: 600, lease: 300, max_attempts: 3}
"""

from abc import ABC, abstractmethod
import argparse
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
from threading import Event
import time
from typing import Optional

from .session_backend import SessionBackend
from .sessions import LocalSessions


log = logging.getLogger(__name__)
LABEL = "entourage.shard"


@dataclass(frozen=True)
class Member:
    """One executable alias in a shard and its runner/worker policy."""

    alias: str
    folder: Path
    definition: str
    start: str = "on_demand"           # runner-owned: eager | on_demand
    reserved: int = 0                  # runner-owned: workers outside the shared pool
    parallel: int = 1                  # runner-owned: most workers at once
    idle_exit: Optional[float] = 600   # worker-owned: None means never
    lease: float = 300                 # worker-owned: step lock and hard limit
    max_attempts: int = 3              # worker-owned: attempts before failed

    def worker_args(self):
        args = ["--lease", str(self.lease), "--max-attempts", str(self.max_attempts)]
        if self.idle_exit is not None:
            args += ["--idle-exit", str(self.idle_exit)]
        return args


@dataclass(frozen=True)
class Shard:
    name: str
    store: Path
    members: dict
    pool: int = 4
    notify: Optional[str] = None
    launcher: dict = field(default_factory=lambda: {"kind": "process"})

    @classmethod
    def from_manifest(cls, path: Path):
        import yaml

        path = Path(path).resolve()
        data = yaml.safe_load(path.read_text())
        allowed = {"shard", "store", "pool", "notify", "launcher", "executables"}
        if not isinstance(data, dict) or set(data) - allowed or "shard" not in data:
            raise ValueError("invalid shard manifest or unknown fields")
        members = {}
        for alias, spec in (data.get("executables") or {}).items():
            if not isinstance(alias, str) or ":" in alias or not isinstance(spec, dict):
                raise ValueError(f"invalid member {alias!r}")
            if set(spec) - {"folder", "runner", "worker"}:
                raise ValueError(f"member {alias!r} has unknown fields")
            folder = (path.parent / spec["folder"]).resolve()
            agent = yaml.safe_load((folder / "agent.yaml").read_text())
            runner, worker = spec.get("runner") or {}, spec.get("worker") or {}
            if set(runner) - {"start", "reserved", "parallel"} or set(worker) - {"idle_exit", "lease", "max_attempts"}:
                raise ValueError(f"member {alias!r} has unknown policy fields")
            start = runner.get("start", "on_demand")
            if start not in ("eager", "on_demand"):
                raise ValueError(f"member {alias!r}: start must be eager or on_demand")
            reserved = runner.get("reserved", 1 if start == "eager" else 0)
            idle = worker.get("idle_exit", "never" if start == "eager" else 600)
            member = Member(alias, folder, agent["definition"], start, reserved,
                            max(runner.get("parallel", 1), reserved),
                            None if idle == "never" else float(idle),
                            float(worker.get("lease", 300)), int(worker.get("max_attempts", 3)))
            if member.reserved < 0 or member.parallel < 1 or member.lease <= 0 or member.max_attempts < 1:
                raise ValueError(f"member {alias!r}: invalid policy numbers")
            members[alias] = member
        return cls(data["shard"], (path.parent / data.get("store", "sessions.db")).resolve(),
                   members, int(data.get("pool", 4)), data.get("notify"),
                   data.get("launcher") or {"kind": "process"})


@dataclass
class Running:
    alias: str
    started_at: float


class Launcher(ABC):
    """Process management for one shard; the runner owns policy, this owns processes."""

    @abstractmethod
    def start(self, name: str, member: Member) -> None: ...

    @abstractmethod
    def running(self) -> dict: ...
    """name -> Running for workers of this shard that are alive now."""

    @abstractmethod
    def stop(self, name: str) -> None: ...
    """Ask politely, then kill after a grace period."""

    @abstractmethod
    def kill(self, name: str) -> None: ...

    def reap_orphans(self) -> int:
        """Retire workers left from a previous runner; returns how many."""
        return 0


class ProcessLauncher(Launcher):
    """Workers as child processes of the runner; dies with it, so no orphan scan."""

    def __init__(self, shard: Shard, *, python=None, env=None, stop_grace=10):
        self.shard = shard
        self.python = python or sys.executable
        self.env = env
        self.stop_grace = stop_grace
        self._procs = {}

    def start(self, name, member):
        argv = [self.python, "-m", "entourage.worker", "--store", str(self.shard.store),
                "--agent", str(member.folder), "--worker", name, *member.worker_args()]
        self._procs[name] = (subprocess.Popen(argv, env=self.env), Running(member.alias, time.time()))

    def running(self):
        for name, (proc, _) in list(self._procs.items()):
            if proc.poll() is not None:
                del self._procs[name]
        return {name: info for name, (_, info) in self._procs.items()}

    def stop(self, name):
        proc, _ = self._procs.get(name, (None, None))
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(self.stop_grace)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def kill(self, name):
        proc, _ = self._procs.get(name, (None, None))
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()

    def stop_all(self):
        for name in list(self._procs):
            self.stop(name)


class PodmanLauncher(Launcher):
    """Workers as containers of one runtime image with the agent folder mounted.

    The store directory is mounted read-write, the agent folder read-only; the
    image entrypoint is the worker module. Environment variables listed in the
    launcher config are passed through from the runner's environment.
    """

    def __init__(self, shard: Shard, *, podman=None, stop_grace=10):
        self.shard = shard
        self.podman = podman or shutil.which("podman") or "podman"
        self.image = shard.launcher.get("image", "localhost/entourage-runtime")
        self.env = list(shard.launcher.get("env", []))
        self.extra = list(shard.launcher.get("args", []))
        self.stop_grace = stop_grace

    def _run(self, *args, check=True):
        return subprocess.run([self.podman, *args], check=check, capture_output=True, text=True)

    def start(self, name, member):
        argv = ["run", "-d", "--rm", "--name", name,
                "--label", f"{LABEL}={self.shard.name}", "--label", f"entourage.agent={member.alias}",
                "-v", f"{self.shard.store.parent}:/state", "-v", f"{member.folder}:/agent:ro"]
        for var in self.env:
            argv += ["-e", var]
        argv += [*self.extra, self.image, "--store", f"/state/{self.shard.store.name}",
                 "--agent", "/agent", "--worker", name, *member.worker_args()]
        self._run(*argv)

    def running(self):
        out = self._run("ps", "--filter", f"label={LABEL}={self.shard.name}", "--format", "json").stdout
        result = {}
        for item in json.loads(out or "[]"):
            name = item["Names"][0] if isinstance(item.get("Names"), list) else item.get("Names")
            started = item.get("StartedAt") or item.get("Created") or 0
            result[name] = Running(item.get("Labels", {}).get("entourage.agent", "?"), float(started))
        return result

    def stop(self, name):
        self._run("stop", "-t", str(self.stop_grace), name, check=False)

    def kill(self, name):
        self._run("kill", name, check=False)

    def reap_orphans(self):
        ids = self._run("ps", "-aq", "--filter", f"label={LABEL}={self.shard.name}").stdout.split()
        if ids:
            self._run("rm", "-f", *ids, check=False)
        return len(ids)


def make_launcher(shard: Shard, **options) -> Launcher:
    kind = shard.launcher.get("kind", "process")
    if kind == "process":
        return ProcessLauncher(shard, **options)
    if kind == "podman":
        return PodmanLauncher(shard, **options)
    raise ValueError(f"unknown launcher kind {kind!r}")


class Runner:
    """One tick: reap, kill stuck holders, notify failures, then start what policy wants."""

    def __init__(self, shard: Shard, store: SessionBackend, launcher: Launcher, *,
                 clock=time.time, quick_exit=10.0, max_backoff=60.0):
        self.shard = shard
        self.store = store
        self.launcher = launcher
        self.clock = clock
        self.quick_exit = quick_exit
        self.max_backoff = max_backoff
        self._seen = {}        # name -> Running, to measure uptime of vanished workers
        self._backoff = {}     # alias -> seconds before the next start is allowed
        self._next_start = {}  # alias -> earliest clock time for a start
        self._serial = 0

    def _name(self, member):
        self._serial += 1
        return f"{self.shard.name}-{member.alias}-{self._serial}"

    def _start(self, member, now):
        if now < self._next_start.get(member.alias, 0):
            return False
        name = self._name(member)
        log.info("starting %s", name)
        self.launcher.start(name, member)
        backoff = self._backoff.get(member.alias, 0.0)
        self._next_start[member.alias] = now + backoff
        return True

    def _observe_exits(self, running, now):
        """Vanished workers that ran only briefly raise the backoff for their alias."""
        for name, info in self._seen.items():
            if name not in running:
                quick = now - info.started_at < self.quick_exit
                current = self._backoff.get(info.alias, 0.0)
                self._backoff[info.alias] = (min(self.max_backoff, max(1.0, current * 2))
                                             if quick else 0.0)
                if quick:
                    self._next_start[info.alias] = now + self._backoff[info.alias]
                    log.warning("%s exited after %.1fs; backoff %.0fs", name,
                                now - info.started_at, self._backoff[info.alias])
        self._seen = dict(running)

    def tick(self):
        now = self.clock()
        running = self.launcher.running()
        self._observe_exits(running, now)
        active = self.store.list_sessions(status="active")
        # Stuck: a live worker whose lease expired. Its commit is already fenced;
        # killing it frees the slot. A vanished worker needs nothing from us.
        for row in active:
            holder = row["worker"]
            if holder in running and row["lease_until"] is not None and row["lease_until"] <= now:
                log.warning("killing %s: lease on %s expired", holder, row["session_id"])
                self.launcher.kill(holder)
                del running[holder]
        if self.shard.notify:
            for row in self.store.list_sessions(status="failed"):
                event_id = f"runner:failed:{row['session_id']}:{row['revision']}:{row['attempts']}"
                try:
                    if self.store.append(self.shard.notify, {
                            "event_id": event_id, "kind": "system", "source": "runner",
                            "payload": {"failed": row["session_id"], "executable": row["executable"],
                                        "attempts": row["attempts"]}}):
                        log.error("session %s failed after %d attempts", row["session_id"], row["attempts"])
                except (KeyError, ValueError) as error:
                    log.error("cannot notify %s: %s", self.shard.notify, error)
        busy = {row["worker"] for row in active if row["worker"] in running}
        counts = {alias: 0 for alias in self.shard.members}
        idle = {alias: 0 for alias in self.shard.members}
        for name, info in running.items():
            if info.alias in counts:
                counts[info.alias] += 1
                idle[info.alias] += name not in busy
        shared_used = sum(max(0, counts[a] - m.reserved) for a, m in self.shard.members.items())
        started = []
        for alias, member in self.shard.members.items():
            want = 0
            if member.start == "eager":
                want = max(member.reserved, 1)
            if counts[alias] < want:
                if self._start(member, now):
                    counts[alias] += 1
                    started.append(alias)
                continue
            ready = self.store.list_sessions(executable=member.definition, ready=True)
            if not ready or len(ready) <= idle[alias] or counts[alias] >= member.parallel:
                continue
            beyond_reserved = counts[alias] >= member.reserved
            if beyond_reserved and shared_used >= self.shard.pool:
                log.info("pool exhausted; %s waits", alias)
                continue
            if self._start(member, now):
                counts[alias] += 1
                shared_used += beyond_reserved
                started.append(alias)
        return started

    def run(self, stop: Event, *, interval=1.0):
        log.info("runner for shard %s; reaped %d orphans", self.shard.name,
                 self.launcher.reap_orphans())
        while not stop.is_set():
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - a runner keeps running
                log.exception("runner tick failed")
            stop.wait(interval)
        if hasattr(self.launcher, "stop_all"):
            self.launcher.stop_all()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m entourage.runner", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    shard = Shard.from_manifest(args.manifest)
    shard.store.parent.mkdir(parents=True, exist_ok=True)
    runner = Runner(shard, LocalSessions(shard.store), make_launcher(shard))
    stop = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    runner.run(stop, interval=args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
