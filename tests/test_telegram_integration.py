import pytest

from entourage.integrations.telegram import TelegramListener, TelegramSender
from entourage.integrations.telegram import bot as telegram_bot
from entourage.sessions import LocalSessions
from examples.telegram_group_manager import (
    GroupManager,
    TelegramOutbox,
    TriageAgent,
    build_dispatcher,
    event_messages,
    ingress,
    parse_cli,
    telegram_event,
)


def test_listener_normalizes_allowlisted_message(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    received = []
    listener = TelegramListener(
        received.append,
        bot_token="123:test",
        allowed_chat_ids={"42"},
    )

    listener._process_update({
        "update_id": 7,
        "message": {
            "message_id": 9,
            "date": 1234,
            "chat": {"id": 42},
            "from": {"id": 5, "username": "alex"},
            "text": "hello",
        },
    })

    assert received == [{
        "chat_id": "42",
        "sender": "alex",
        "sender_id": "5",
        "text": "hello",
        "message_id": 9,
        "update_id": 7,
        "timestamp": 1234,
    }]


def test_listener_fails_closed_without_allowlist(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    received = []
    listener = TelegramListener(received.append, bot_token="123:test")
    listener._process_update({
        "update_id": 7,
        "message": {
            "message_id": 9,
            "chat": {"id": 42},
            "from": {"id": 5},
            "text": "hello",
        },
    })
    assert received == []


def test_sender_returns_telegram_message(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    calls = []

    def fake_request(url, payload=None, timeout=35):
        calls.append((url, payload, timeout))
        return {"ok": True, "result": {"message_id": 11}}

    monkeypatch.setattr(telegram_bot, "_request_json", fake_request)
    result = TelegramSender(bot_token="123:test").send("42", "reply")

    assert result["result"]["message_id"] == 11
    assert calls == [(
        "https://api.telegram.org/bot123:test/sendMessage",
        {"chat_id": "42", "text": "reply"},
        15,
    )]


def test_group_demo_normalizes_telegram_into_session_mail():
    message = {"chat_id": "42", "sender": "alex", "sender_id": "5", "text": "remember this",
               "message_id": 9, "update_id": 7, "timestamp": 1234}

    conversation, event = telegram_event(message)

    assert conversation == "telegram:42"
    assert event == {
        "event_id": "telegram:7", "kind": "user", "source": "telegram",
        "payload": {
            "sender": "alex", "sender_id": "5", "content": "remember this", "chat_id": "42",
            "telegram_message_id": 9,
            "reply_target": {"channel": "telegram", "chat_id": "42"}, "created_at": 1234,
        },
    }


def test_group_demo_preserves_event_roles_for_model():
    assert event_messages([
        {"kind": "user", "payload": {"sender": "alex", "content": "question"}},
        {"kind": "ambient", "payload": {"content": "Grafana summary"}},
        {"kind": "subagent", "payload": {"content": "worker update"}},
        {"kind": "assistant", "payload": {"content": "answer"}},
        {"kind": "delivery", "payload": {"content": "not model context"}},
    ]) == [
        {"role": "user", "content": "alex: question"},
        {"role": "system", "content": "[ambient update]\nGrafana summary"},
        {"role": "system", "content": "[subagent update]\nworker update"},
        {"role": "assistant", "content": "answer"},
    ]


CHAT = "group:telegram:42"


class Demo:
    """A group manager, its outbox and a dispatcher over one local store."""

    def __init__(self, tmp_path, triage, answer, **options):
        self.store = LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)
        self.sent, self.output = [], []
        self.manager = GroupManager(triage, answer, output=self.output.append)
        self.outbox = TelegramOutbox(self.send, output=self.output.append)
        self.worker = build_dispatcher(self.store, self.manager, self.outbox, **options)
        assert self.worker.run_once().session_id == "telegram-outbox"  # parks until mail
        self.ingress = ingress(self.store)

    def send(self, chat_id, text):
        self.sent.append((chat_id, text))
        return {"result": {"message_id": len(self.sent)}}

    def publish(self, event):
        return self.ingress.deliver("group", event, conversation="telegram:42")

    def user(self, content="question"):
        return self.publish({"event_id": f"event:{content}", "kind": "user", "source": "telegram",
                             "payload": {"sender": "alex", "content": content, "chat_id": "42",
                                         "reply_target": {"channel": "telegram", "chat_id": "42"}}})

    def run(self):
        for result in self.worker.run_until_idle():
            assert result.committed, result.error

    def events(self):
        return [(e["kind"], e["payload"]["content"])
                for e in self.store.inspect(CHAT)["state"]["events"]]


def test_group_demo_records_and_replies_to_triaged_message(tmp_path):
    demo = Demo(tmp_path, lambda *_: True, lambda *_: "the answer")
    assert demo.user().created

    demo.run()

    assert demo.events() == [("user", "question"), ("delivery", "the answer"),
                             ("assistant", "the answer")]
    assert demo.sent == [("42", "the answer")]
    assert demo.store.inspect(CHAT)["state"]["phase"] == "idle"
    assert demo.store.inspect("telegram-outbox")["state"] == {"delivered": 1}


def test_group_demo_records_chatter_without_replying(tmp_path):
    demo = Demo(tmp_path, lambda *_: False, lambda *_: "must not run")
    demo.user("ordinary chatter")

    demo.run()

    assert demo.sent == []
    assert demo.events() == [("user", "ordinary chatter")]
    assert demo.worker.run_once() is None


def test_mail_arriving_during_triage_joins_the_answer_after_a_restart(tmp_path):
    captured = []
    demo = None

    def triage(_model, _name, messages):
        demo.publish({"event_id": "subagent:1", "kind": "subagent", "source": "worker",
                      "payload": {"content": "found the timeout"}})
        return True

    def answer(_model, _name, messages):
        captured.extend(messages)
        return "timeout found"

    demo = Demo(tmp_path, triage, answer)
    demo.user()
    first = demo.worker.run_once()
    assert first.session_id == CHAT and first.committed
    assert demo.store.inspect(CHAT)["state"]["phase"] == "answer"
    assert captured == []

    fresh = build_dispatcher(demo.store, demo.manager, demo.outbox)  # a new process
    for result in fresh.run_until_idle():
        assert result.committed, result.error

    assert {"role": "system", "content": "[subagent update]\nfound the timeout"} in captured
    assert demo.sent == [("42", "timeout found")]
    assert [kind for kind, _ in demo.events()] == ["user", "subagent", "delivery", "assistant"]


def test_burst_is_coalesced_into_one_triage(tmp_path):
    seen = []
    demo = Demo(tmp_path, lambda _m, _n, messages: seen.append(len(messages)) or False,
                lambda *_: "unused", max_events=1)
    demo.user("first")
    demo.user("second")

    demo.run()

    assert seen == [2]
    assert demo.events() == [("user", "first"), ("user", "second")]


def test_group_demo_mirrors_announcement_and_records_receipt(tmp_path):
    demo = Demo(tmp_path, lambda *_: False, lambda *_: "unused")
    demo.publish(parse_cli("/announce hourly summary", "42"))

    demo.run()

    assert demo.events() == [("ambient", "hourly summary"), ("delivery", "hourly summary")]
    assert demo.sent == [("42", "hourly summary")]


def test_session_triage_labels_forwards_and_the_chat_answers_on_a_trigger(tmp_path):
    store = LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)
    sent, output = [], []
    manager = GroupManager(None, lambda *_: "the answer", output=output.append)
    triage = TriageAgent(lambda _m, _n, text: "Alexander" in text, output=output.append)
    worker = build_dispatcher(store, manager, TelegramOutbox(
        lambda chat_id, text: sent.append((chat_id, text)) or {"result": {}}), triage)
    assert worker.run_once().session_id == "telegram-outbox"
    router = ingress(store)

    def user(content):
        event = {"event_id": f"event:{content}", "kind": "user", "source": "telegram",
                 "payload": {"sender": "alex", "content": content, "chat_id": "42",
                             "reply_target": {"channel": "telegram", "chat_id": "42"}}}
        router.ensure("group", conversation="telegram:42")
        return router.deliver("triage", event)

    assert user("lunch anyone?").session_id == "triage:event:lunch anyone?"
    user("Alexander, what time is it?")
    for result in worker.run_until_idle():
        assert result.committed, result.error

    assert store.inspect("triage:event:lunch anyone?")["status"] == "complete"
    chat = store.inspect(CHAT)["state"]
    assert [(e["kind"], e["payload"].get("trigger")) for e in chat["events"]][:2] == [
        ("user", False), ("user", True)]
    assert [e["kind"] for e in chat["events"]] == ["user", "user", "delivery", "assistant"]
    assert sent == [("42", "the answer")]
    assert "agent: inspecting group context" not in output  # no triage model in the chat

    user("more chatter")
    for result in worker.run_until_idle():
        assert result.committed, result.error
    assert sent == [("42", "the answer")]
    assert [e["kind"] for e in store.inspect(CHAT)["state"]["events"]][-1] == "user"


def test_unknown_mail_fails_the_activation(tmp_path):
    demo = Demo(tmp_path, lambda *_: True, lambda *_: "unused")
    demo.publish({"event_id": "r1", "kind": "result", "payload": {}})
    result = demo.worker.run_once()
    assert not result.committed
    with pytest.raises(ValueError, match="unexpected mail 'r1'"):
        raise result.error
