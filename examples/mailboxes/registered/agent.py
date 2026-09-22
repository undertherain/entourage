"""Application-owned state, mail ingestion and a two-function continuation router."""

from pathlib import Path


def begin(context, state, incorporated):
    if state["task"] == "events":
        context.send(context.config["output"], {"answer": "Events: no upcoming fixtures."},
                     key="answer")
        state["phase"] = "done"
        return context.propose(state, incorporated=incorporated, complete=True)
    state["pending"] = context.request(
        "sources", {"brief": state["brief"]}, key="sources:1")
    state["phase"] = "awaiting_sources"
    return context.propose(state, incorporated=incorporated)


def continue_research(context, state, incorporated):
    if "sources" not in state:
        # A correction wakes us too. Keep the pending request and park again.
        return context.propose(state, incorporated=incorporated)
    prompt = (Path(__file__).parent / "prompt.md").read_text().strip()
    answer = f"{prompt}\nBrief: {state['brief']}\nSources: {state['sources']}"
    context.send(context.config["output"], {"answer": answer}, key="answer")
    state.update(phase="done", answer=answer)
    del state["pending"]
    return context.propose(state, incorporated=incorporated, complete=True)


PHASES = {"ready": begin, "awaiting_sources": continue_research}


def resume(context, state, mail):
    incorporated = []
    for event in mail:
        if event["kind"] == "user":
            state["brief"] = event["payload"]["brief"]
        elif (event["kind"] == "result"
              and event.get("request_id") == state.get("pending")
              and event.get("source") == "sources"):
            state["sources"] = event["payload"]["sources"]
        else:
            raise ValueError(f"unexpected mail: {event['event_id']}")
        incorporated.append(event["event_id"])
    if context.has_more:
        # Persist this batch before deciding to advance using the remaining mail.
        return context.propose(state, incorporated=incorporated)
    return PHASES[state["phase"]](context, state, incorporated)
