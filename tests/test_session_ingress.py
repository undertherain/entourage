"""Keying decides session lifetime; the store only sees create and append."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from entourage.session_ingress import Member, SessionIngress
from entourage.sessions import LocalSessions


MEMBERS = [
    Member("triage", "triage:v1", "event"),
    Member("chat", "concierge:v1", "conversation", initial_state={"phase": "ready"}),
    Member("concierge", "concierge:v1", "singleton", session="concierge-main",
           initial_state={"phase": "ready"}),
]


@pytest.fixture
def ingress(tmp_path):
    store = LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)
    return SessionIngress(store, MEMBERS), store


def event(event_id):
    return {"event_id": event_id, "kind": "user", "payload": {"text": event_id}}


def test_keying_derives_session_ids_without_storage(ingress):
    router, store = ingress
    assert router.route("triage", event("m1")) == "triage:m1"
    assert router.route("chat", event("m1"), conversation="tg:42") == "chat:tg:42"
    assert router.route("concierge", event("m1")) == "concierge-main"
    with pytest.raises(ValueError, match="conversation"):
        router.route("chat", event("m1"))
    with pytest.raises(KeyError):
        router.route("unknown", event("m1"))
    with pytest.raises(ValueError, match="event_id"):
        router.route("triage", {"kind": "user"})
    assert store.claim() is None


def test_per_event_sessions_run_in_parallel_and_finish_independently(ingress):
    router, store = ingress
    first = router.deliver("triage", event("m1"))
    second = router.deliver("triage", event("m2"))
    assert first.created and first.appended and first.session_id == "triage:m1"
    assert second.session_id == "triage:m2"
    a, b = store.claim(), store.claim()
    assert {a.session_id, b.session_id} == {"triage:m1", "triage:m2"}
    store.commit(a, {}, incorporated=[a.events[0]["event_id"]], complete=True)
    replay = router.deliver("triage", event("m1"))
    assert not replay.created and not replay.appended
    store.commit(b, {}, incorporated=[b.events[0]["event_id"]], complete=True)
    assert store.claim() is None


def test_per_conversation_sessions_are_created_once_and_serialize_their_mail(ingress):
    router, store = ingress
    first = router.deliver("chat", event("m1"), conversation="tg:42")
    second = router.deliver("chat", event("m2"), conversation="tg:42")
    other = router.deliver("chat", event("m3"), conversation="tg:7")
    assert first.created and not second.created and other.created
    assert store.inspect("chat:tg:42")["state"] == {"phase": "ready"}
    activation = store.claim(executable="concierge:v1")
    assert [e["event_id"] for e in activation.events] == ["m1", "m2"]
    assert store.claim(executable="concierge:v1").session_id == "chat:tg:7"


def test_singleton_is_created_on_first_delivery_and_never_reset(ingress):
    router, store = ingress
    assert router.deliver("concierge", event("m1")).session_id == "concierge-main"
    activation = store.claim()
    store.commit(activation, {"phase": "busy"}, incorporated=["m1"])
    delivery = router.deliver("concierge", event("m2"))
    assert not delivery.created and delivery.appended
    assert store.inspect("concierge-main")["state"] == {"phase": "busy"}


def test_completion_closes_a_key_until_purge(ingress):
    router, store = ingress
    router.deliver("chat", event("m1"), conversation="tg:42")
    activation = store.claim()
    store.commit(activation, {}, incorporated=["m1"], complete=True)
    with pytest.raises(ValueError, match="complete"):
        router.deliver("chat", event("m2"), conversation="tg:42")
    assert store.purge(completed_before=200) == 1
    fresh = router.deliver("chat", event("m2"), conversation="tg:42")
    assert fresh.created and store.inspect("chat:tg:42")["revision"] == 0


def test_concurrent_deliveries_for_a_new_key_have_one_creator_and_lose_no_mail(tmp_path):
    path = tmp_path / "sessions.db"
    routers = [SessionIngress(LocalSessions(path), MEMBERS) for _ in range(4)]
    with ThreadPoolExecutor(max_workers=4) as workers:
        deliveries = list(workers.map(
            lambda i: routers[i].deliver("chat", event(f"m{i}"), conversation="tg:42"),
            range(4)))
    assert sum(d.created for d in deliveries) == 1
    assert all(d.appended for d in deliveries)
    activation = LocalSessions(path).claim()
    assert len(activation.events) == 4


def test_ensure_creates_the_destination_without_mail(ingress):
    router, store = ingress
    first = router.ensure("chat", conversation="tg:42")
    again = router.ensure("chat", conversation="tg:42")
    assert first.session_id == "chat:tg:42" and first.created and not first.appended
    assert not again.created
    assert store.inspect("chat:tg:42")["state"] == {"phase": "ready"}
    assert router.deliver("chat", event("m1"), conversation="tg:42").created is False
    assert router.ensure("triage", event=event("m9")).session_id == "triage:m9"


def test_member_validation():
    with pytest.raises(ValueError, match="keying"):
        Member("x", "x:v1", "per-task")
    with pytest.raises(ValueError, match="singleton"):
        Member("x", "x:v1", "event", session="fixed")
    with pytest.raises(ValueError, match="':'"):
        Member("a:b", "x:v1", "event")
    with pytest.raises(ValueError, match="duplicate"):
        SessionIngress(None, [Member("x", "x:v1", "event"), Member("x", "y:v1", "event")])
