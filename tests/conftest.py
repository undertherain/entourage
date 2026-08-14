import os
import uuid

import pytest

# Redis-backed tests need a reachable server. Point ENTOURAGE_TEST_REDIS_URL
# at one (e.g. redis://:password@localhost:6379/15 — prefer a dedicated db
# number); tests only ever touch keys under their own throwaway namespace.
REDIS_URL = os.environ.get(
    "ENTOURAGE_TEST_REDIS_URL",
    os.environ.get("REDIS_URL", "redis://localhost:6379/15"),
)


@pytest.fixture(scope="session")
def redis_client():
    """A client for the test Redis server; skips if unavailable."""
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        client.ping()
    except Exception:
        pytest.skip(f"no Redis server reachable at {REDIS_URL}")
    return client


@pytest.fixture(params=["memory", "sqlite", "redis"])
def store(request, tmp_path):
    """A GraphStore, parametrized over backends — every backend must pass
    the same suites."""
    from entourage.runtime import InMemoryGraphStore, SQLiteGraphStore

    if request.param == "memory":
        s = InMemoryGraphStore()
    elif request.param == "sqlite":
        s = SQLiteGraphStore(tmp_path / "test.db")
    else:
        from entourage.runtime import RedisGraphStore

        client = request.getfixturevalue("redis_client")
        s = RedisGraphStore(
            client=client, namespace=f"entourage-test-graph-{uuid.uuid4().hex[:8]}"
        )
        yield s
        s.purge()
        return
    yield s
    s.close()


@pytest.fixture(params=["memory", "redis"], ids=["qmem", "qredis"])
def make_runtime(request):
    """Runtime factory, parametrized over queue backends — every queue
    backend must pass the same graph-algebra suite as the in-memory one."""
    from entourage.runtime import InMemoryReadyQueue, QueueRuntime

    if request.param == "redis":
        from entourage.runtime.redis_queue import RedisReadyQueue

        client = request.getfixturevalue("redis_client")
        queues = []

        def factory(store, registry):
            q = RedisReadyQueue(
                client=client,
                namespace=f"entourage-test-{uuid.uuid4().hex[:8]}",
                poll_interval=0.005,
            )
            queues.append(q)
            return QueueRuntime(node_registry=registry, store=store, queue=q)

        yield factory
        for q in queues:
            q.purge()
    else:
        yield lambda store, registry: QueueRuntime(
            node_registry=registry, store=store, queue=InMemoryReadyQueue()
        )


@pytest.fixture
def redis_queue(redis_client):
    """A RedisReadyQueue in an isolated, purged-afterwards namespace."""
    from entourage.runtime.redis_queue import RedisReadyQueue

    q = RedisReadyQueue(
        client=redis_client,
        namespace=f"entourage-test-{uuid.uuid4().hex[:8]}",
        poll_interval=0.005,
    )
    yield q
    q.purge()


@pytest.fixture(params=["memory", "redis"])
def mailbox_backend(request):
    """Mailbox contract backend with an isolated Redis namespace when available."""
    if request.param == "memory":
        from entourage.mailbox import InMemoryMailbox

        yield InMemoryMailbox()
        return
    from entourage.redis_mailbox import RedisMailbox

    client = request.getfixturevalue("redis_client")
    mailbox = RedisMailbox(
        client=client,
        namespace=f"entourage-test-mailbox-{uuid.uuid4().hex[:8]}",
        poll_interval=0.005,
    )
    yield mailbox
    mailbox.purge()
