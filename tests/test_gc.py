import pytest

from entourage.runtime import (
    InMemoryReadyQueue,
    QueueRuntime,
    RetentionPolicy,
    collect_terminal_sessions,
)


def terminal_session(store, number):
    session_id = store.create_session("test", {"number": number})
    first = store.add_execution(session_id, "first")
    second = store.add_execution(session_id, "second")
    store.add_edge(session_id, first, second)
    store.complete_session(session_id)
    return session_id, {first, second}


def test_terminal_session_deletion_removes_graph_but_refuses_active(store):
    terminal, execution_ids = terminal_session(store, 1)
    active = store.create_session("test", {})

    with pytest.raises(ValueError, match="not terminal"):
        store.delete_terminal_session(active)

    assert store.delete_terminal_session(terminal) is True
    assert store.get_session(terminal) is None
    assert store.get_session_edges(terminal) == []
    assert all(store.get_execution(exec_id) is None for exec_id in execution_ids)
    assert store.get_session(active)["status"] == "running"


def test_collector_enforces_count_and_batch_oldest_first(store):
    session_ids = [terminal_session(store, number)[0] for number in range(4)]
    policy = RetentionPolicy(
        terminal_ttl_seconds=None,
        max_terminal_sessions=2,
        batch_size=1,
        interval_seconds=0,
    )

    assert collect_terminal_sessions(store, policy) == [session_ids[0]]
    assert collect_terminal_sessions(store, policy) == [session_ids[1]]
    assert collect_terminal_sessions(store, policy) == []
    assert [session["id"] for session in store.get_terminal_sessions()] == session_ids[2:]


def test_runtime_runs_configured_collector(store):
    session_id, _ = terminal_session(store, 1)
    runtime = QueueRuntime(
        store=store,
        queue=InMemoryReadyQueue(),
        retention_policy=RetentionPolicy(
            terminal_ttl_seconds=None,
            max_terminal_sessions=0,
            batch_size=10,
            interval_seconds=0,
        ),
    )

    assert runtime.collect_garbage(force=True) == [session_id]
    assert store.get_session(session_id) is None
