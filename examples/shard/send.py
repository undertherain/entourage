"""Create a session for the demo shard and send it mail; the runner does the rest.

    python examples/shard/send.py hello        # session echo:demo gets one message
    python examples/shard/send.py --show       # print saved state and the ops inbox
"""

import argparse
import json
from pathlib import Path
import uuid

from entourage.session_ingress import Member, SessionIngress
from entourage.sessions import LocalSessions

HERE = Path(__file__).parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", default="hello")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()
    (HERE / "state").mkdir(exist_ok=True)
    store = LocalSessions(HERE / "state/sessions.db")
    try:
        store.create("ops", "ops:v1", {})  # receives runner failure notices
    except ValueError:
        pass
    if args.show:
        print(json.dumps({row["session_id"]: store.inspect(row["session_id"])
                          for row in store.list_sessions()}, indent=2))
        return
    ingress = SessionIngress(store, [Member("echo", "echo:v1", "singleton", session="echo:demo")])
    delivery = ingress.deliver("echo", {"event_id": uuid.uuid4().hex, "kind": "user",
                                        "payload": {"text": args.text}})
    print(f"delivered to {delivery.session_id} (created={delivery.created})")


if __name__ == "__main__":
    main()
