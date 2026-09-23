"""Worker entrypoint: run one agent folder's dispatcher against a shared store.

    python -m entourage.worker --store state/sessions.db --agent ./concierge

A worker is a process that opens the store, claims ready sessions of the
definitions it loaded, runs them and commits. It decides its own exit: on
SIGTERM/SIGINT, or after --idle-exit seconds without a claim. The store is the
only protocol; nothing is received from the runner after start. This is also
the entrypoint of the runtime container image.
"""

import argparse
import logging
from pathlib import Path
import signal
import sys
from threading import Event

from .executables import Dispatcher, Executable
from .sessions import LocalSessions


def build_parser():
    parser = argparse.ArgumentParser(prog="python -m entourage.worker", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--store", required=True, type=Path, help="SQLite session store")
    parser.add_argument("--agent", type=Path, action="append", default=[],
                        help="agent folder containing agent.yaml (repeatable)")
    parser.add_argument("--manifest", type=Path, action="append", default=[],
                        help="additional executable manifest (repeatable)")
    parser.add_argument("--session", help="serve only this session (pinned worker)")
    parser.add_argument("--worker", help="name recorded on claimed leases")
    parser.add_argument("--idle-exit", type=float, default=None,
                        help="exit after this many idle seconds; default never")
    parser.add_argument("--lease", type=float, default=300,
                        help="lock length and hard limit of one step, seconds")
    parser.add_argument("--max-attempts", type=int, default=3,
                        help="failed attempts before a session is parked as failed")
    parser.add_argument("--poll", type=float, default=0.5, help="idle poll interval, seconds")
    parser.add_argument("--max-events", type=int, default=64)
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    manifests = [folder / "agent.yaml" for folder in args.agent] + args.manifest
    if not manifests:
        build_parser().error("at least one --agent folder or --manifest is required")
    dispatcher = Dispatcher(LocalSessions(args.store), max_events=args.max_events,
                            lease_seconds=args.lease, max_attempts=args.max_attempts,
                            worker=args.worker, session=args.session)
    for manifest in manifests:
        dispatcher.register(Executable.from_manifest(manifest))
    stop = Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    log = logging.getLogger("entourage.worker")
    log.info("worker %s serving %s", dispatcher.worker, ", ".join(sorted(dispatcher.definitions)))
    dispatcher.run_forever(stop, poll_interval=args.poll, idle_exit=args.idle_exit)
    log.info("worker %s exiting (%s)", dispatcher.worker, "stopped" if stop.is_set() else "idle")
    return 0


if __name__ == "__main__":
    sys.exit(main())
