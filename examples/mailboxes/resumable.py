"""Save a continuation and exit. A fresh process routes to the saved step."""

from .support import restart_cli


def prepare(turn):
    turn.state["itinerary"] = ["Walk around Kyoto"]
    turn.state["pending"] = turn.call("weather", city="Kyoto", key="weather-1")
    turn.save(next_step=finish)
    print("Prepared itinerary; saved next=finish and dispatched weather. Exiting.")


def finish(turn):
    weather = turn.result(turn.state["pending"])
    turn.state["itinerary"].append(weather)
    turn.save(complete=True)
    print(f"Resumed finish with saved itinerary: {turn.state['itinerary']}")


if __name__ == "__main__":
    restart_cli([prepare, finish])
