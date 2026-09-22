"""Reusable SessionBackend behavior; adapters are supplied by make_session_backend."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from entourage.session_backend import (
    Publication, SessionAlreadyExists, Spawn, StaleActivation,
)


@pytest.fixture
def sessions(make_session_backend):
    now = [100.0]
    return make_session_backend(clock=lambda: now[0]), now



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


def test_external_mail_cannot_shadow_a_deadline_event(sessions):
    store, _ = sessions
    park(store)
    with pytest.raises(ValueError, match="reserved"):
        store.append("trip", mail("timer:trip:1"))


def test_definition_binding_survives_reopen_and_rejects_replacement(make_session_backend):
    store = make_session_backend(clock=lambda: 100)
    contract = {"schema": 1, "config": {"a": 1, "b": 2}}
    store.bind_definition("agent:v1", contract)
    fresh = make_session_backend(clock=lambda: 100)
    fresh.bind_definition("agent:v1", {"config": {"b": 2, "a": 1}, "schema": 1})
    with pytest.raises(ValueError):
        fresh.bind_definition("agent:v1", {"schema": 2})
    fresh.bind_definition("agent:v1", contract)


@pytest.mark.parametrize("complete", [False, True])
def test_duplicate_creation_never_overwrites_session(sessions, complete):
    store, _ = sessions
    store.create("trip", "travel:v1", {"saved": [1]})
    activation = store.claim()
    store.commit(activation, activation.state, incorporated=[], complete=complete)
    before = store.inspect("trip")
    with pytest.raises(SessionAlreadyExists):
        store.create("trip", "other:v1", {"saved": [2]})
    assert store.inspect("trip") == before


def test_concurrent_creation_has_one_winner(make_session_backend):
    handles = [make_session_backend(clock=lambda: 100) for _ in range(2)]

    def create(index):
        try:
            handles[index].create("trip", "travel:v1", {"winner": index})
            return index
        except SessionAlreadyExists:
            return None

    with ThreadPoolExecutor(max_workers=2) as workers:
        winners = [result for result in workers.map(create, range(2)) if result is not None]
    assert len(winners) == 1
    assert handles[0].inspect("trip")["state"] == {"winner": winners[0]}


def test_snapshot_and_activation_values_are_detached(sessions):
    store, _ = sessions
    store.create("trip", "travel:v1", {"saved": [1]})
    store.append("trip", {"event_id": "input", "payload": {"value": 1}})
    observed = store.inspect("trip")
    assert observed == {"state": {"saved": [1]}, "executable": "travel:v1",
                        "status": "ready", "deadline": None, "revision": 0}
    observed["state"]["saved"].append(2)
    activation = store.claim()
    assert activation.state == {"saved": [1]}
    activation.state["saved"].append(3)
    activation.events[0]["payload"]["value"] = 9
    assert store.inspect("trip")["state"] == {"saved": [1]}
    store.commit(activation, {"saved": [4]}, incorporated=[])
    assert store.claim().events[0]["payload"] == {"value": 1}


def test_bounded_mail_and_rotation_survive_reopen(make_session_backend):
    store = make_session_backend(clock=lambda: 100)
    store.create("a", "agent:v1", {})
    store.create("b", "agent:v1", {})
    for name in ("one", "two", "three"):
        store.append("a", mail(name))
    first = store.claim(max_events=2)
    assert first.session_id == "a" and first.has_more
    assert [event["event_id"] for event in first.events] == ["one", "two"]
    store.commit(first, {}, incorporated=["one", "two"])
    fresh = make_session_backend(clock=lambda: 100)
    second = fresh.claim(max_events=2)
    assert second.session_id == "b" and not second.has_more
    fresh.commit(second, {}, incorporated=[])
    last = fresh.claim(max_events=2)
    assert last.session_id == "a" and not last.has_more
    assert last.events == [mail("three")]


def test_same_ids_in_separate_backend_namespaces_are_isolated(make_session_backend):
    first = make_session_backend(clock=lambda: 100, name="first")
    second = make_session_backend(clock=lambda: 100, name="second")
    for store in (first, second):
        park(store, state={"saved": True})
    first.append("trip", mail("input"))
    assert first.claim() is not None
    assert second.claim() is None
    assert second.inspect("trip")["state"] == {"saved": True}


def test_commit_token_cannot_be_reused(sessions):
    store, _ = sessions
    store.create("trip", "travel:v1", {})
    activation = store.claim()
    store.commit(activation, {"saved": True}, incorporated=[])
    with pytest.raises(StaleActivation):
        store.commit(activation, {"saved": False}, incorporated=[])
    assert store.inspect("trip")["revision"] == 1
    assert store.inspect("trip")["state"] == {"saved": True}


def test_spawn_creates_child_with_parent_checkpoint_and_delivers_mail(sessions):
    store, _ = sessions
    store.bind_definition("research:v1", {"schema": 1})
    store.create("concierge", "travel:v1", {})
    activation = store.claim()
    store.commit(activation, {"delegated": True}, incorporated=[],
                 spawn=(Spawn("concierge:research:1", "research:v1", {"brief": "Kyoto"}),),
                 publish=(Publication("concierge:research:1", mail("brief")),))
    assert store.inspect("concierge")["state"] == {"delegated": True}
    child = store.claim(executable="research:v1")
    assert child.session_id == "concierge:research:1"
    assert child.state == {"brief": "Kyoto"}
    assert child.events == [mail("brief")]
    assert store.inspect("concierge:research:1")["revision"] == 0


def test_spawn_of_existing_child_rolls_back_the_whole_checkpoint(sessions):
    store, now = sessions
    store.bind_definition("research:v1", {"schema": 1})
    park(store, "concierge:research:1", {"first": True})
    park(store, "sink")
    store.create("concierge", "travel:v1", {"delegated": False})
    activation = store.claim(executable="travel:v1", lease_seconds=5)
    with pytest.raises(SessionAlreadyExists):
        store.commit(activation, {"delegated": True}, incorporated=[],
                     publish=(Publication("sink", mail("side-effect")),),
                     spawn=(Spawn("concierge:research:1", "research:v1", {"second": True}),))
    assert store.inspect("concierge")["state"] == {"delegated": False}
    assert store.inspect("concierge:research:1")["state"] == {"first": True}
    assert store.claim() is None  # parent still leased; sink and child got no mail
    store.commit(activation, {"delegated": "later"}, incorporated=[])
    assert store.inspect("concierge")["revision"] == 1


def test_spawn_requires_bound_definition_distinct_ids_and_live_lease(sessions):
    store, now = sessions
    store.bind_definition("research:v1", {"schema": 1})
    store.create("concierge", "travel:v1", {})
    activation = store.claim(lease_seconds=1)
    with pytest.raises(ValueError, match="unbound"):
        store.commit(activation, {}, incorporated=[],
                     spawn=(Spawn("concierge:x", "missing:v1", {}),))
    with pytest.raises(ValueError, match="distinct"):
        store.commit(activation, {}, incorporated=[],
                     spawn=(Spawn("concierge:x", "research:v1", {}),
                            Spawn("concierge:x", "research:v1", {})))
    with pytest.raises(ValueError, match="parent"):
        store.commit(activation, {}, incorporated=[],
                     spawn=(Spawn("concierge", "research:v1", {}),))
    with pytest.raises(KeyError):
        store.inspect("concierge:x")
    now[0] += 1
    with pytest.raises(StaleActivation):
        store.commit(activation, {}, incorporated=[],
                     spawn=(Spawn("concierge:x", "research:v1", {}),))
    with pytest.raises(KeyError):
        store.inspect("concierge:x")


def test_child_outlives_parent_completion_but_cannot_reply_to_it(sessions):
    store, _ = sessions
    store.bind_definition("research:v1", {"schema": 1})
    store.create("parent", "travel:v1", {})
    activation = store.claim()
    store.commit(activation, {}, incorporated=[], complete=True,
                 spawn=(Spawn("parent:child", "research:v1", {}),))
    child = store.claim()
    assert child.session_id == "parent:child"
    with pytest.raises(ValueError, match="complete"):
        store.commit(child, {}, incorporated=[],
                     publish=(Publication("parent", mail("result")),))
    store.commit(child, {"orphaned": True}, incorporated=[])
    assert store.inspect("parent:child")["status"] == "waiting"


def test_purge_removes_only_old_completed_sessions_and_ends_their_dedup(sessions):
    store, now = sessions

    def finish(session_id):
        store.create(session_id, "travel:v1", {})
        store.append(session_id, mail("input"))
        activation = store.claim()
        store.commit(activation, {}, incorporated=["input"], complete=True)

    finish("early")
    now[0] = 200
    finish("late")
    park(store, "parked")
    assert store.purge(completed_before=100) == 0
    assert store.purge(completed_before=150) == 1
    with pytest.raises(KeyError):
        store.inspect("early")
    with pytest.raises(KeyError):
        store.append("early", mail("input"))
    store.create("early", "travel:v1", {})  # the ID is reusable
    assert store.append("early", mail("input")) is True  # dedup window ended
    assert store.append("late", mail("input")) is False
    assert store.inspect("parked")["status"] == "waiting"
    now[0] = 300
    finish("another")
    assert store.purge(completed_before=400, limit=1) == 1
    assert store.purge(completed_before=400) == 1
    assert store.inspect("parked")["status"] == "waiting"
    with pytest.raises(ValueError):
        store.purge(completed_before=400, limit=0)
