"""
RedisReadyQueue semantics: per-session FIFO, fair-share rotation across
sessions, ack/nack, stale-in-flight reclaim, and the full runtime engine
running on the Redis queue. Skipped when no Redis server is available.
"""

from entourage.flow import Parallel, Sequence
from entourage.runtime import InMemoryGraphStore, QueueRuntime
from entourage.runtime.redis_queue import SYSTEM_KEY


def _exec(session, exec_id):
    return {"type": "execute", "exec_id": exec_id, "session_id": session}


def test_fifo_within_a_session(redis_queue):
    for i in range(5):
        redis_queue.send(_exec("A", f"a{i}"))

    batch = redis_queue.receive(max_messages=10, wait_seconds=0)
    assert [m.payload["exec_id"] for m in batch] == [f"a{i}" for i in range(5)]


def test_fair_share_across_sessions(redis_queue):
    """A deep backlog in one session must not starve other sessions."""
    for i in range(20):
        redis_queue.send(_exec("A", f"a{i}"))
    redis_queue.send(_exec("B", "b0"))
    redis_queue.send(_exec("C", "c0"))

    batch = redis_queue.receive(max_messages=6, wait_seconds=0)
    sessions = [m.payload["session_id"] for m in batch]

    # First rotation pass serves every active session once
    assert set(sessions[:3]) == {"A", "B", "C"}
    # The rest of the batch comes from A's backlog, still in FIFO order
    assert sessions.count("A") == 4
    a_ids = [m.payload["exec_id"] for m in batch if m.payload["session_id"] == "A"]
    assert a_ids == ["a0", "a1", "a2", "a3"]


def test_ack_removes_from_in_flight(redis_queue):
    redis_queue.send(_exec("A", "a0"))
    [msg] = redis_queue.receive(max_messages=1, wait_seconds=0)
    assert redis_queue.in_flight_count() == 1

    msg.ack()
    assert redis_queue.in_flight_count() == 0
    assert redis_queue.receive(max_messages=1, wait_seconds=0) == []


def test_nack_redelivers_first(redis_queue):
    redis_queue.send(_exec("A", "a0"))
    redis_queue.send(_exec("A", "a1"))

    [msg] = redis_queue.receive(max_messages=1, wait_seconds=0)
    assert msg.payload["exec_id"] == "a0"
    msg.nack()

    batch = redis_queue.receive(max_messages=2, wait_seconds=0)
    assert [m.payload["exec_id"] for m in batch] == ["a0", "a1"]
    assert redis_queue.in_flight_count() == 2


def test_reclaim_stale_in_flight(redis_queue):
    """A payload claimed by a dead worker is re-enqueued, not lost."""
    redis_queue.send(_exec("A", "a0"))
    [msg] = redis_queue.receive(max_messages=1, wait_seconds=0)
    # ... the worker dies here: no ack, no nack ...
    assert redis_queue.receive(max_messages=1, wait_seconds=0) == []

    assert redis_queue.reclaim_stale(older_than=-1.0) == 1
    [again] = redis_queue.receive(max_messages=1, wait_seconds=0)
    assert again.payload["exec_id"] == "a0"
    again.ack()
    assert redis_queue.in_flight_count() == 0


def test_triggers_share_the_system_key(redis_queue):
    redis_queue.send({"type": "trigger", "trigger": "t", "state": {}})
    assert redis_queue.active_keys() == [SYSTEM_KEY]

    [msg] = redis_queue.receive(max_messages=1, wait_seconds=0)
    assert msg.payload["trigger"] == "t"


def test_runtime_end_to_end_on_redis(redis_queue):
    calls = []

    def mk(name):
        def node(state):
            calls.append(name)
            return {**state, name: True}, None
        return node

    registry = {n: mk(n) for n in ("a", "b", "c", "d")}
    rt = QueueRuntime(
        node_registry=registry, store=InMemoryGraphStore(), queue=redis_queue
    )
    sid = rt.start_session("t", {"seed": 1}, plan=Sequence("a", Parallel("b", "c"), "d"))
    rt.run(poll_wait=0.05, stop_when_idle=True)

    assert calls[0] == "a" and calls[-1] == "d"
    assert set(calls) == {"a", "b", "c", "d"}
    assert rt.store.get_session(sid)["status"] == "completed"
    assert rt.queue.in_flight_count() == 0
