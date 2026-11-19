"""Checkpoint inbox delivery (G1): drain_into_steer semantics and fail-open guards."""
import pytest

from hermes_cli import coordination as co
from agent.coordination_inbox import drain_into_steer


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


class _StubAgent:
    def __init__(self, session_id: str | None = "sess-r", persist_disabled=False):
        self.session_id = session_id
        self._persist_disabled = persist_disabled
        self.steered: list[str] = []

    def steer(self, text, **kw):
        self.steered.append(text)
        return True


class TestDrainIntoSteer:
    def test_delivers_once_and_acks(self):
        co.send_message("sess-s", "sess-r", "finding: X")
        agent = _StubAgent()
        assert drain_into_steer(agent) == 1
        assert "finding: X" in agent.steered[0]
        assert "sess-s" in agent.steered[0]
        assert drain_into_steer(agent) == 0  # already delivered
        assert co.sent_status("sess-s")[0]["acked_at"] is not None
        assert co.inbox_history("sess-s") == []  # no ack recursion

    def test_persist_disabled_fork_skipped(self):
        co.send_message("sess-s", "sess-r", "mail")
        agent = _StubAgent(persist_disabled=True)
        assert drain_into_steer(agent) == 0
        assert agent.steered == []
        # parent still gets it
        assert drain_into_steer(_StubAgent()) == 1

    def test_no_session_id_skipped(self):
        agent = _StubAgent(session_id=None)
        assert drain_into_steer(agent) == 0

    def test_disabled_coordination_skipped(self, monkeypatch):
        monkeypatch.setattr(co, "coordination_config", lambda: {"enabled": False})
        co.send_message("sess-s", "sess-r", "mail")
        assert drain_into_steer(_StubAgent()) == 0

    def test_fail_open_on_registry_error(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("db down")
        monkeypatch.setattr(co, "drain_inbox", boom)
        assert drain_into_steer(_StubAgent()) == 0

    def test_no_steer_method_skipped(self):
        co.send_message("sess-s", "sess-r", "mail")

        class NoSteer:
            session_id = "sess-r"

        assert drain_into_steer(NoSteer()) == 0
        # undelivered mail stays queued for a capable surface
        assert len(co.drain_inbox("sess-r")) == 1
