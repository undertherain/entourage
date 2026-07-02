"""Microbenchmark: per-node orchestration overhead of the Entourage runtime
(plan expansion + ready detection + state persistence + queue round-trip),
measured end-to-end with no-op nodes on the in-memory and SQLite backends.

Source of the overhead numbers in the paper's Evaluation section.

Usage: python3 benchmarks/overhead.py
"""

import statistics
import tempfile
import time
from pathlib import Path

from entourage.flow import Sequence
from entourage.runtime import (
    InMemoryGraphStore,
    InMemoryReadyQueue,
    QueueRuntime,
    SQLiteGraphStore,
)

N_NODES = 50
N_RUNS = 20


def make_nodes(n):
    def mk(i):
        def node(state):
            return {**state, f"n{i}": True}, None
        return node
    return {f"n{i}": mk(i) for i in range(n)}


def bench(store_factory, label):
    per_node_times = []
    for run in range(N_RUNS):
        store = store_factory(run)
        registry = make_nodes(N_NODES)
        rt = QueueRuntime(
            node_registry=registry, store=store, queue=InMemoryReadyQueue()
        )
        plan = Sequence(*[f"n{i}" for i in range(N_NODES)])
        t0 = time.perf_counter()
        sid = rt.start_session("bench", {}, plan=plan)
        rt.run(poll_wait=0.001, stop_when_idle=True)
        t1 = time.perf_counter()
        assert store.get_session(sid)["status"] == "completed"
        # N user nodes + the executed END sentinel (HEAD completes for free)
        per_node_times.append((t1 - t0) / (N_NODES + 1) * 1000)
        store.close()
    mean = statistics.mean(per_node_times)
    stdev = statistics.stdev(per_node_times)
    print(f"{label}: {mean:.3f} ms/node (stdev {stdev:.3f}, "
          f"{N_RUNS} runs x {N_NODES} nodes)")


def bench_redis(url):
    """Full-Redis configuration: graph store AND ready queue on one server."""
    import uuid

    import redis as redis_lib

    from entourage.runtime import RedisGraphStore, RedisReadyQueue

    client = redis_lib.Redis.from_url(url, decode_responses=True)
    try:
        client.ping()
    except Exception:
        print(f"redis: skipped (no server at {url})")
        return

    per_node_times = []
    for run in range(N_RUNS):
        ns = f"entourage-bench-{uuid.uuid4().hex[:8]}"
        store = RedisGraphStore(client=client, namespace=f"{ns}:graph")
        queue = RedisReadyQueue(
            client=client, namespace=f"{ns}:queue", poll_interval=0.001
        )
        registry = make_nodes(N_NODES)
        rt = QueueRuntime(node_registry=registry, store=store, queue=queue)
        plan = Sequence(*[f"n{i}" for i in range(N_NODES)])
        t0 = time.perf_counter()
        sid = rt.start_session("bench", {}, plan=plan)
        rt.run(poll_wait=0.001, stop_when_idle=True)
        t1 = time.perf_counter()
        assert store.get_session(sid)["status"] == "completed"
        per_node_times.append((t1 - t0) / (N_NODES + 1) * 1000)
        store.purge()
        queue.purge()
    mean = statistics.mean(per_node_times)
    stdev = statistics.stdev(per_node_times)
    print(f"redis    : {mean:.3f} ms/node (stdev {stdev:.3f}, "
          f"{N_RUNS} runs x {N_NODES} nodes, store+queue on one server)")


if __name__ == "__main__":
    import os

    with tempfile.TemporaryDirectory() as tmp:
        bench(lambda run: InMemoryGraphStore(), "in-memory")
        bench(lambda run: SQLiteGraphStore(Path(tmp) / f"bench_{run}.db"), "sqlite   ")
    bench_redis(os.environ.get("ENTOURAGE_BENCH_REDIS_URL", "redis://localhost:6379/15"))
