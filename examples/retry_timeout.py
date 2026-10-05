"""Attempt limits, backoff and lease expiry on the session dispatcher.

Policy is per definition, not per node: `Dispatcher.register(executable,
lease_seconds=..., max_attempts=...)`. One activation is one step. A step
that raises is released at once with its error and an exponential backoff
(never above the lease); the store counts attempts and parks the session as
`failed` when the limit is reached. A step that outlives its lease loses its
commit authority: the retry wins, the slow attempt's commit is rejected. The
dispatcher cannot kill a blocked Python call; the shard runner kills the
worker process instead.

Run:  python examples/retry_timeout.py
"""

import tempfile
import time
from pathlib import Path

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions


def flaky_api(context, state, mail):
    """A transient outage: the first two attempts raise."""
    if context.attempt <= 2:
        print(f"    flaky_api: attempt {context.attempt} raises ConnectionError (transient)")
        raise ConnectionError("upstream reset the connection")
    print(f"    flaky_api: attempt {context.attempt} succeeds (last error was "
          f"{context.last_error!r})")
    return context.propose({**state, "data": "payload"},
                           incorporated=[e["event_id"] for e in mail], complete=True)


def broken_api(context, state, mail):
    print(f"    broken_api: attempt {context.attempt} raises")
    raise RuntimeError("permanently misconfigured")


def slow_step(context, state, mail):
    """The first attempt outlives its lease; its commit is then rejected."""
    if context.attempt == 1:
        print("    slow_step: attempt 1 is slow and outlives the 0.2s lease...")
        time.sleep(0.3)
    else:
        print(f"    slow_step: attempt {context.attempt} is quick")
    return context.propose({**state, "done_by_attempt": context.attempt},
                           incorporated=[e["event_id"] for e in mail], complete=True)


def drain(dispatcher, store, session, *, timeout=5.0):
    """Run until the session is complete or failed, waiting out backoffs."""
    started = time.time()
    while store.inspect(session)["status"] not in ("complete", "failed"):
        for result in dispatcher.run_until_idle():
            if not result.committed:
                print(f"    [{result.session_id}: not committed: {result.error!r}]")
        if time.time() - started > timeout:
            raise TimeoutError(session)
        time.sleep(0.05)
    snapshot = store.inspect(session)
    print(f"  {session}: {snapshot['status']}, state={snapshot['state']}")


def main():
    with tempfile.TemporaryDirectory() as folder:
        store = LocalSessions(Path(folder) / "sessions.db")
        dispatcher = (Dispatcher(store)
                      .register(Executable("flaky:v1", flaky_api), lease_seconds=0.2, max_attempts=4)
                      .register(Executable("broken:v1", broken_api), lease_seconds=0.2, max_attempts=2)
                      .register(Executable("slow:v1", slow_step), lease_seconds=0.2, max_attempts=3))

        print("═══ 1. flaky API with max_attempts=4: retries absorb the outage ═══")
        dispatcher.create("flaky", "flaky:v1", {})
        drain(dispatcher, store, "flaky")

        print("\n═══ 2. broken API with max_attempts=2: parked as failed ═══")
        dispatcher.create("broken", "broken:v1", {})
        drain(dispatcher, store, "broken")
        print(f"  last error: {store.inspect('broken')['last_error']!r}")

        print("\n═══ 3. slow step with a 0.2s lease: the stale commit is rejected ═══")
        dispatcher.create("slow", "slow:v1", {})
        drain(dispatcher, store, "slow")


if __name__ == "__main__":
    main()
