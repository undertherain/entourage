"""Dispatch a tool, keep handling mail, ingest its result on a later pass."""

import asyncio

from .support import mailbox, resident_demo


async def agent(store):
    async for turn in mailbox(store):
        for event in turn.mail:
            if event["kind"] == "user":
                turn.state["pending"] = turn.call(
                    "weather", city=event["city"], key=event["event_id"],
                )
                print("Dispatched weather; continuing the mailbox loop.")
            elif event["kind"] == "note":
                turn.state["note"] = event["content"]
                print(f"Handled other mail: {event['content']}")
            elif event["kind"] == "result":
                turn.state["weather"] = turn.result(turn.state["pending"])
                print(f"Ingested tool result: {turn.state['weather']}")
        turn.save()
        if "weather" in turn.state:
            return  # finite demo; a service would keep receiving


if __name__ == "__main__":
    asyncio.run(resident_demo(agent))
