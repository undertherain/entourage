"""Mock tool worker; replies are staged in the same checkpoint as incorporation."""


def resume(context, state, mail):
    for request in mail:
        if request["kind"] != "request":
            raise ValueError("expected a request")
        context.reply(request, {"sources": "Kyoto gardens, museums and walking routes"})
    return context.propose(state, incorporated=[event["event_id"] for event in mail])
