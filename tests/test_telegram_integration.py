from entourage.integrations.telegram import TelegramListener, TelegramSender
from entourage.integrations.telegram import bot as telegram_bot
from entourage.mailbox import InMemoryMailbox
from entourage.memory import EventHistory
from examples.telegram_group_manager import (
    GroupManager,
    event_messages,
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


def test_group_demo_normalizes_telegram_into_typed_event():
    message = {
        "chat_id": "42",
        "sender": "alex",
        "sender_id": "5",
        "text": "remember this",
        "message_id": 9,
        "update_id": 7,
        "timestamp": 1234,
    }

    conversation_id, event = telegram_event(message)

    assert conversation_id == "telegram:42"
    assert event == {
        "event_id": "telegram:7",
        "kind": "user",
        "source": "telegram",
        "sender": "alex",
        "sender_id": "5",
        "content": "remember this",
        "chat_id": "42",
        "telegram_message_id": 9,
        "reply_target": {"channel": "telegram", "chat_id": "42"},
        "created_at": 1234,
    }


def test_group_demo_preserves_event_roles_for_model():
    assert event_messages([
        {"kind": "user", "sender": "alex", "content": "question"},
        {"kind": "ambient", "content": "Grafana summary"},
        {"kind": "subagent", "content": "worker update"},
        {"kind": "assistant", "content": "answer"},
        {"kind": "delivery", "content": "not model context"},
    ]) == [
        {"role": "user", "content": "alex: question"},
        {"role": "system", "content": "[ambient update]\nGrafana summary"},
        {"role": "system", "content": "[subagent update]\nworker update"},
        {"role": "assistant", "content": "answer"},
    ]


def make_manager(tmp_path, triage, answer, sent, output=None):
    return GroupManager(
        InMemoryMailbox(),
        tmp_path,
        lambda chat_id, text: sent.append((chat_id, text)) or {
            "result": {"message_id": len(sent)}
        },
        triage=triage,
        answer=answer,
        output=(output or []).append,
        step_delay=0,
    )


def publish_user(manager, content="question"):
    manager.publish("telegram:42", {
        "event_id": f"event:{content}",
        "kind": "user",
        "source": "telegram",
        "sender": "alex",
        "content": content,
        "chat_id": "42",
        "reply_target": {"channel": "telegram", "chat_id": "42"},
    })


def test_group_demo_records_and_replies_to_triaged_message(tmp_path):
    sent = []
    manager = make_manager(
        tmp_path,
        lambda *_args: True,
        lambda *_args: "the answer",
        sent,
    )
    publish_user(manager)

    assert manager.process_next() is True
    events = EventHistory("telegram:42", tmp_path).get_events()
    assert [(event["kind"], event["content"]) for event in events] == [
        ("user", "question"),
        ("assistant", "the answer"),
    ]
    assert sent == [("42", "the answer")]


def test_group_demo_records_chatter_without_replying(tmp_path):
    sent = []
    manager = make_manager(
        tmp_path,
        lambda *_args: False,
        lambda *_args: "must not run",
        sent,
    )
    publish_user(manager, "ordinary chatter")

    manager.process_next()

    assert sent == []
    assert EventHistory("telegram:42", tmp_path).get_events()[0]["content"] == "ordinary chatter"


def test_group_demo_ingests_subagent_update_before_answer(tmp_path):
    sent = []
    captured = []
    manager = None

    def triage(*_args):
        manager.publish("telegram:42", {
            "event_id": "subagent:1",
            "kind": "subagent",
            "source": "worker",
            "content": "found the timeout",
        })
        return True

    def answer(_model, _name, messages):
        captured.extend(messages)
        return "timeout found"

    manager = make_manager(tmp_path, triage, answer, sent)
    publish_user(manager)

    manager.process_next()

    assert {"role": "system", "content": "[subagent update]\nfound the timeout"} in captured
    assert sent == [("42", "timeout found")]


def test_group_demo_mirrors_announcement_and_records_receipt(tmp_path):
    sent = []
    manager = make_manager(
        tmp_path,
        lambda *_args: False,
        lambda *_args: "unused",
        sent,
    )
    manager.publish("telegram:42", parse_cli("/announce hourly summary", "42"))

    manager.process_next()

    events = EventHistory("telegram:42", tmp_path).get_events()
    assert [event["kind"] for event in events] == ["ambient", "delivery"]
    assert sent == [("42", "hourly summary")]
