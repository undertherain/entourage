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
