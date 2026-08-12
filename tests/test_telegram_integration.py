from entourage.integrations.telegram import TelegramListener, TelegramSender
from entourage.integrations.telegram import bot as telegram_bot
from examples import telegram_simple


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


def test_demo_records_inbound_and_archives_on_new(tmp_path, monkeypatch):
    monkeypatch.setattr(telegram_simple, "HISTORY_DIR", tmp_path / "history")
    message = {
        "chat_id": "42",
        "sender": "alex",
        "text": "remember this",
        "message_id": 9,
        "update_id": 7,
    }
    telegram_simple.record_inbound(message)

    stored = telegram_simple._history("42").get_messages()
    assert stored[0]["content"] == "remember this"
    assert stored[0]["telegram_update_id"] == 7

    telegram_simple.record_inbound({**message, "text": "/new", "update_id": 8})
    assert telegram_simple._history("42").get_messages() == []
    assert len(list((tmp_path / "history" / "archive").glob("42-*.json"))) == 1
