"""Producer-side helpers: explicitly addressed sessions and member definitions."""

import textwrap

import pytest

from entourage.runner import Shard
from entourage.session_ingress import ensure_session
from entourage.sessions import LocalSessions


def make_shard(root):
    folder = root / "agents" / "helper"
    (folder / "helper_pkg").mkdir(parents=True)
    (folder / "helper_pkg" / "__init__.py").write_text("SEEN = 'sibling package'\n")
    (folder / "agent.py").write_text(
        "from helper_pkg import SEEN\n"
        "def resume(ctx, state, mail):\n"
        "    return ctx.propose(state, incorporated=[e['event_id'] for e in mail])\n")
    (folder / "agent.yaml").write_text(
        "definition: helper:v1\nprotocol: entourage.activation/v1\n"
        "entrypoint: agent.py:resume\ninbox: session\n")
    (root / "shard.yaml").write_text(textwrap.dedent("""\
        shard: test
        store: sessions.db
        executables:
          helper: {folder: agents/helper}
        """))
    return Shard.from_manifest(root / "shard.yaml")


def test_member_executable_imports_sibling_packages(tmp_path):
    shard = make_shard(tmp_path)
    executable = shard.members["helper"].executable()
    assert executable.definition == "helper:v1"
    assert executable.resume.__module__.startswith("_entourage_executable_")


def test_ensure_session_binds_creates_once_and_keeps_state(tmp_path):
    store = LocalSessions(tmp_path / "sessions.db")
    assert ensure_session(store, "chat:1", "helper:v1", {"v": 1}, {"conversation_id": "chat:1"})
    assert not ensure_session(store, "chat:1", "helper:v1", {"v": 1}, {"conversation_id": "other"})
    snapshot = store.inspect("chat:1")
    assert snapshot["executable"] == "helper:v1"
    assert snapshot["state"] == {"conversation_id": "chat:1"}
    with pytest.raises(ValueError):
        ensure_session(store, "chat:2", "helper:v1", {"v": 2}, {})
