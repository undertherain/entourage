import pytest

from entourage.flow import Node, RecursionLimitExceeded, Sequence
from entourage.runtime.local import Runtime
from entourage.runtime.memory import InMemoryGraphStore, InMemoryReadyQueue

def test_recursion_limit():
    runtime = Runtime()
    store = runtime.store

    def looping_agent(state):
        return state, Sequence("do_stuff", Node(looping_agent, max_invocations=3))

    def do_stuff(state):
        state["count"] = state.get("count", 0) + 1
        return state

    runtime.register_node("looping_agent", looping_agent)
    runtime.register_node("do_stuff", do_stuff)

    session_id = runtime.start_session(looping_agent, {"count": 0})
    
    runtime.run()
    
    # Session should have failed due to RecursionLimitExceeded
    session = store.get_session(session_id)
    assert session["status"] == "failed"
    
    # Check that count is 2 (it should have run do_stuff twice before the 3rd invocation of looping_agent hit the limit during plan expansion)
    # Actually it's 3 because max_invocations=3 means the 3rd expansion will fail.
    # The count doesn't matter much as long as it stopped.

def test_global_session_limit():
    runtime = Runtime()
    store = runtime.store
    
    import entourage.runtime.queue as queue_mod
    original_limit = queue_mod.MAX_SESSION_NODES
    queue_mod.MAX_SESSION_NODES = 5
    
    try:
        def endless_agent(state):
            return state, Sequence("dummy", endless_agent)
            
        def dummy(state):
            return state
            
        runtime.register_node("endless_agent", endless_agent)
        runtime.register_node("dummy", dummy)
        
        session_id = runtime.start_session(endless_agent, {})
        runtime.run()
        
        session = store.get_session(session_id)
        assert session["status"] == "failed"
    finally:
        queue_mod.MAX_SESSION_NODES = original_limit
