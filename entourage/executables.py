"""Registered, graph-independent Python activations over a SessionBackend.

This is a trusted in-process adapter, not a Python security sandbox. Code returns
proposals; only the dispatcher receives the store's activation/lease token.
"""

from copy import deepcopy
from dataclasses import dataclass, field, replace
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import math
from pathlib import Path
import sys
from threading import Event
from typing import Callable, Optional
import uuid

from .session_backend import Publication, SessionBackend, Spawn


PROTOCOL = "entourage.activation/v1"
log = logging.getLogger(__name__)


def _json_copy(value):
    return json.loads(json.dumps(value, allow_nan=False))


def _identity(*parts):
    return hashlib.sha256(json.dumps(parts, ensure_ascii=True).encode()).hexdigest()


@dataclass(frozen=True)
class Executable:
    """One version of local code. Development explicitly permits source changes.

    Source fingerprints cover the entrypoint file and declared resources, not
    the whole Python environment. Use a new version when state semantics change.
    """

    definition: str
    resume: Callable
    state_schema: int = 1
    config: dict = field(default_factory=dict)
    development: bool = False
    resources: tuple[Path, ...] = ()
    upgrades: dict = field(default_factory=dict)
    """Superseded definition -> migrate(context, state, mail) returning a Proposal.

    The dispatcher serves ready sessions still bound to those definitions with
    the migrate handler and rebinds them to this version at its checkpoint.
    Migration runs lazily, at the session's next wake, which is the only time a
    definition matters. Old code need not be loadable. Upgrade maps are not part
    of the persisted contract; they describe how to leave old versions.
    """

    def contract(self):
        if not isinstance(self.definition, str) or not all(self.definition.rpartition(":")[::2]):
            raise ValueError("definition must include a name and version, e.g. research:v1")
        if not callable(self.resume) or inspect.iscoroutinefunction(self.resume):
            raise ValueError("resume must be a synchronous callable")
        if type(self.state_schema) is not int or self.state_schema < 1:
            raise ValueError("state_schema must be a positive integer")
        if type(self.development) is not bool:
            raise ValueError("development must be a boolean")
        if not isinstance(self.config, dict):
            raise ValueError("config must be a JSON object")
        if not isinstance(self.upgrades, dict) or not all(
                isinstance(old, str) and old and old != self.definition and callable(migrate)
                and not inspect.iscoroutinefunction(migrate)
                for old, migrate in self.upgrades.items()):
            raise ValueError("upgrades map other definitions to synchronous callables")
        source = inspect.getsourcefile(self.resume)
        if not source and not self.development:
            raise ValueError("source unavailable; explicitly opt into development mode")
        fingerprints = [] if self.development else [
            hashlib.sha256(Path(path).read_bytes()).hexdigest()
            for path in (source, *self.resources)
        ]
        return {"protocol": PROTOCOL, "state_schema": self.state_schema,
                "entrypoint": self.resume.__qualname__, "source": fingerprints,
                "development": self.development, "config": _json_copy(self.config)}

    @classmethod
    def from_manifest(cls, path: Path):
        """Load a local YAML definition, with file paths relative to its manifest.

        Entrypoints are `agent.py:resume` or importable `package.module:resume`.
        Resource files are fingerprinted; the application decides how to use them.
        """
        import yaml

        path = Path(path).resolve()
        data = yaml.safe_load(path.read_text())
        allowed = {"definition", "protocol", "entrypoint", "state_schema", "inbox",
                   "config", "development", "resources", "upgrades"}
        if not isinstance(data, dict) or set(data) - allowed:
            raise ValueError("invalid manifest or unknown fields")
        if data.get("protocol") != PROTOCOL or data.get("inbox") != "session":
            raise ValueError("manifest requires entourage.activation/v1 and inbox: session")
        resources = data.get("resources", [])
        if not isinstance(resources, list) or not all(isinstance(p, str) for p in resources):
            raise ValueError("resources must be a list of file paths")
        upgrades = data.get("upgrades", {})
        if not isinstance(upgrades, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in upgrades.items()):
            raise ValueError("upgrades must map old definitions to entrypoints")
        loaded = {}  # one execution per source file for this manifest load
        definition = cls(data["definition"],
                         _load_entrypoint(path, data.get("entrypoint", ""), loaded),
                         state_schema=data.get("state_schema", 1),
                         config=data.get("config", {}),
                         development=data.get("development", False),
                         resources=tuple(path.parent / p for p in resources),
                         upgrades={old: _load_entrypoint(path, entry, loaded)
                                   for old, entry in upgrades.items()})
        definition.contract()
        return definition


def _load_entrypoint(manifest: Path, entrypoint, loaded: dict):
    if not isinstance(entrypoint, str) or ":" not in entrypoint:
        raise ValueError("entrypoint must be module:handler or file.py:handler")
    module_name, attribute = entrypoint.rsplit(":", 1)
    if module_name.endswith(".py"):
        source = (manifest.parent / module_name).resolve()
        if source not in loaded:
            name = "_entourage_executable_" + _identity(str(source))
            spec = importlib.util.spec_from_file_location(name, source)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            # Compile the current bytes: do not reuse stale same-second .pyc files.
            exec(compile(source.read_bytes(), str(source), "exec"), module.__dict__)
            loaded[source] = module
        module = loaded[source]
    else:
        module = importlib.import_module(module_name)
    return getattr(module, attribute)


@dataclass(frozen=True)
class Proposal:
    state: dict
    incorporated: tuple[str, ...] = ()
    publish: tuple[Publication, ...] = ()
    deadline: Optional[float] = None
    complete: bool = False
    spawn: tuple[Spawn, ...] = ()
    rebind: Optional[str] = None


class Context:
    """Per-attempt metadata and staged local mail, with no commit capability."""

    def __init__(self, session_id, definition, config, *, has_more=False,
                 upgrading_from=None):
        self.session_id = session_id
        self.definition = definition
        self.upgrading_from = upgrading_from
        self.activation_id = uuid.uuid4().hex
        self.config = _json_copy(config)
        self.has_more = has_more
        self._publish = []
        self._spawn = []

    def send(self, destination, payload, *, key, kind="message"):
        """Stage mail with a key stable across retries and unique per operation."""
        if not isinstance(key, str) or not key:
            raise ValueError("publication key must be a nonempty string")
        event_id = "mail:" + _identity(self.session_id, key)
        self._publish.append(Publication(destination, {
            "event_id": event_id, "kind": kind, "source": self.session_id,
            "payload": _json_copy(payload),
        }))
        return event_id

    def request(self, destination, payload, *, key):
        request_id = self.send(destination, payload, key=key, kind="request")
        self._publish[-1].event.update(request_id=request_id, reply_to=self.session_id)
        return request_id

    def reply(self, request, payload, *, key="result"):
        request_id = request["request_id"]
        event_id = self.send(request["reply_to"], payload,
                             key="reply:" + _identity(request_id, key), kind="result")
        self._publish[-1].event["request_id"] = request_id
        return event_id

    def spawn(self, definition, state, *, key):
        """Stage a child session `<session_id>:<key>` bound to a registered definition.

        The child is created with the parent's checkpoint, so mail staged to the
        returned ID in the same proposal is delivered atomically. Keys are stable
        across retries: an existing child rejects the whole proposal, which is how
        a repeated logical spawn is detected rather than duplicated.
        """
        if not isinstance(key, str) or not key:
            raise ValueError("spawn key must be a nonempty string")
        if not isinstance(state, dict):
            raise ValueError("child state must be a JSON object")
        child = f"{self.session_id}:{key}"
        self._spawn.append(Spawn(child, definition, _json_copy(state)))
        return child

    def propose(self, state, *, incorporated=(), deadline=None, complete=False,
                rebind=None):
        """Stage the checkpoint; rebind hands the session to another definition."""
        return Proposal(_json_copy(state), tuple(incorporated),
                        tuple(deepcopy(self._publish)), deadline, complete,
                        tuple(deepcopy(self._spawn)), rebind)


@dataclass(frozen=True)
class DispatchResult:
    """Outcome observed by this dispatcher call.

    committed=False means commit was not confirmed. Infrastructure errors can
    hide an accepted remote commit; backend recovery resolves that uncertainty.
    """

    session_id: str
    committed: bool
    error: Optional[Exception] = None


class Dispatcher:
    """Restore, invoke, validate and commit one bounded mail batch at a time.

    Handler/validation failures leave the checkpoint untouched; retry becomes
    eligible after lease expiry. Infrastructure failures may have an unknown
    commit outcome. There is no attempt limit or lease renewal yet.
    A blocked Python call cannot be killed here; process isolation is a follow-on.
    """

    def __init__(self, store: SessionBackend, *, max_events=64, lease_seconds=30):
        if type(max_events) is not int or max_events < 1:
            raise ValueError("max_events must be a positive integer")
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        self.store = store
        self.max_events = max_events
        self.lease_seconds = lease_seconds
        self._definitions = {}
        self._upgrades = {}
        self._cursor = 0

    def register(self, executable: Executable):
        """Serve a definition, plus lazily migrate sessions it declares upgrades for.

        A definition cannot be both served here and upgraded away by another
        registration: that would make the old version's sessions ambiguous.
        """
        name = executable.definition
        if name in self._definitions or name in self._upgrades:
            raise ValueError(f"duplicate or superseded definition {name!r}")
        for old in executable.upgrades:
            if old in self._definitions or old in self._upgrades:
                raise ValueError(f"definition {old!r} is already served or upgraded here")
        contract = executable.contract()
        self.store.bind_definition(name, contract)
        self._definitions[name] = (executable.resume, contract)
        for old, migrate in executable.upgrades.items():
            self._upgrades[old] = (migrate, name)
        return self

    def create(self, session_id, definition, state):
        if definition not in self._definitions:
            raise KeyError(definition)
        if not isinstance(state, dict):
            raise ValueError("state must be a JSON object")
        self.store.create(session_id, definition, _json_copy(state))

    def run_once(self):
        names = [*self._definitions, *self._upgrades]
        for offset in range(len(names)):
            index = (self._cursor + offset) % len(names)
            name = names[index]
            activation = self.store.claim(executable=name, max_events=self.max_events,
                                          lease_seconds=self.lease_seconds)
            if activation is None:
                continue
            self._cursor = (index + 1) % len(names)
            if name in self._definitions:
                handler, contract = self._definitions[name]
                target, upgrading_from = None, None
            else:
                handler, target = self._upgrades[name]
                contract, upgrading_from = self._definitions[target][1], name
            # Keep the original activation private and unchanged for commit validation.
            context = Context(activation.session_id, target or name, contract["config"],
                              has_more=activation.has_more, upgrading_from=upgrading_from)
            try:
                proposal = handler(context, deepcopy(activation.state),
                                   deepcopy(activation.events))
                if not isinstance(proposal, Proposal):
                    raise TypeError("resume must return a Proposal")
                if not isinstance(proposal.state, dict) or type(proposal.complete) is not bool:
                    raise ValueError("proposal requires object state and boolean complete")
                if target is not None:
                    if proposal.rebind not in (None, target) or proposal.complete:
                        raise ValueError(f"migration from {name!r} must rebind to {target!r}")
                    proposal = replace(proposal, rebind=target)
                self.store.commit(activation, _json_copy(proposal.state),
                                  incorporated=list(proposal.incorporated),
                                  publish=deepcopy(proposal.publish),
                                  deadline=proposal.deadline, complete=proposal.complete,
                                  spawn=deepcopy(proposal.spawn), rebind=proposal.rebind)
            except Exception as error:
                return DispatchResult(activation.session_id, False, error)
            return DispatchResult(activation.session_id, True)
        return None

    def run_until_idle(self, *, max_activations=100):
        """Drain ready work within a budget; failures are returned for inspection."""
        if type(max_activations) is not int or max_activations < 1:
            raise ValueError("max_activations must be a positive integer")
        results = []
        for _ in range(max_activations):
            result = self.run_once()
            if result is None:
                break
            results.append(result)
        return results

    def run_forever(self, stop: Event, *, poll_interval=0.1, on_result=None):
        """Resident polling, interruptible while idle; no lease is held while waiting."""
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("poll_interval must be finite and positive")
        while not stop.is_set():
            result = self.run_once()
            if result is not None:
                if on_result is not None:
                    on_result(result)
                elif not result.committed:
                    log.error("Activation failed for %s: %s", result.session_id, result.error)
            if result is None or not result.committed:
                stop.wait(poll_interval)
