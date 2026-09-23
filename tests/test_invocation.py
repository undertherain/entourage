import pytest

from entourage.invocation import ToolInvocation, current_invocation, invocation_context


def test_invocation_context_is_scoped():
    outer = ToolInvocation("concierge", "chat:1")
    inner = ToolInvocation("events", "chat:2", reply_target="delivery:1")
    with invocation_context(outer):
        assert current_invocation() == outer
        with invocation_context(inner):
            assert current_invocation() == inner
        assert current_invocation() == outer
    with pytest.raises(RuntimeError):
        current_invocation()
