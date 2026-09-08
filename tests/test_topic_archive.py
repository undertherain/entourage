from entourage.memory import TopicMemory


def test_archive_record_hides_storage_convention(tmp_path, monkeypatch):
    memory = TopicMemory(tmp_path, "unused")
    monkeypatch.setattr(memory, "summarize", lambda _messages: "durable summary")

    archive = memory.archive_record([{"role": "user", "content": "hello"}])

    assert archive.summary == "durable summary"
    assert archive.summary_path.read_text() == "durable summary"
    assert archive.transcript_path.is_file()
