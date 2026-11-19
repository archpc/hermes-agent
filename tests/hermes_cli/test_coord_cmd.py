"""``hermes coord`` read-only view (G3)."""
import argparse
import json

import pytest

from hermes_cli import coordination as co
from hermes_cli.subcommands.coord import build_coord_parser, cmd_coord


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


def _args(**kw):
    ns = argparse.Namespace(json=False, messages=20)
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


class TestCoordCmd:
    def test_json_snapshot(self, _home, capsys):
        co.claim("path:/shared", "path", "sess-a")
        co.send_message("sess-a", "sess-b", "hello")
        assert cmd_coord(_args(json=True)) == 0
        snap = json.loads(capsys.readouterr().out)
        assert snap["enabled"] is True
        assert len(snap["claims"]) == 1
        assert len(snap["messages"]) == 1
        assert "active_sessions" in snap and "goals" in snap

    def test_human_render_sections(self, _home, capsys):
        co.claim("path:/shared", "path", "sess-a", meta={"note": "mine"})
        co.send_message("sess-a", "sess-b", "hello")
        co.register_dependency("sess-b", co.integration_key(str(_home), "main"), "abc123")
        assert cmd_coord(_args()) == 0
        out = capsys.readouterr().out
        for section in ("ACTIVE SESSIONS", "ACTIVE GOALS", "RESOURCE CLAIMS",
                        "INTEGRATION DEPENDENCIES", "RECENT MESSAGES", "RECENT CLAIM EVENTS"):
            assert section in out
        assert "sess-a" in out and "QUEUED" in out and "path:/shared" in out

    def test_empty_store_renders(self, _home, capsys):
        assert cmd_coord(_args()) == 0
        out = capsys.readouterr().out
        assert "(none)" in out

    def test_parser_wiring(self):
        p = argparse.ArgumentParser()
        sub = p.add_subparsers(dest="cmd")
        build_coord_parser(sub)
        ns = p.parse_args(["coord", "--json", "--messages", "5"])
        assert ns.json is True and ns.messages == 5 and ns.func is cmd_coord

    def test_read_only_no_mutation(self, _home):
        co.claim("path:/shared", "path", "sess-a")
        co.send_message("sess-a", "sess-b", "hello")
        before_claims = co.active_claims()
        before_inbox = co.inbox_history("sess-b")
        cmd_coord(_args(json=True))
        assert co.active_claims() == before_claims
        after = co.inbox_history("sess-b")
        assert after == before_inbox
        assert after[0]["delivered_at"] is None  # viewing never delivers
