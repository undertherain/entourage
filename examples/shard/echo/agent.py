"""Smallest long-lived agent: count the mail it has seen and park."""


def resume(context, state, mail):
    state["seen"] = state.get("seen", 0) + len(mail)
    state["attempt"] = context.attempt
    return context.propose(state, incorporated=[event["event_id"] for event in mail])
