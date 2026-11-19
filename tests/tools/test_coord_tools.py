"""Tool-boundary coordination tests: coord_* handlers, file-write and terminal-cwd
enforcement through the REAL tool entry points (not just the registry helpers).
"""
import json
import os
import subprocess

import pytest

from hermes_cli import coordination as co
from tools import coord_tools  # registers the coord_* tools at import
from tools.registry import registry


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    co._WT_CACHE.clear()
    yield tmp_path


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL, check=True)


@pytest.fixture
def worktree(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "f.txt").write_text("base\n")
    _git(repo, "add", "f.txt")
    _git(repo, "commit", "-qm", "base")
    wt = tmp_path / "wt"
    _git(repo, "worktree", "add", "-q", str(wt), "-b", "side")
    return repo, wt


def _call(name, args, session):
    r = registry.dispatch(name, args, session_id=session, task_id=f"t-{session}")
    return r if isinstance(r, dict) else json.loads(r)


class TestCoordTools:
    def test_claim_send_status_flow(self, _home):
        r = _call("coord_claim", {"resource": str(_home / "shared"), "note": "mine"}, "sess-a")
        assert r["ok"]
        conflict = _call("coord_claim", {"resource": str(_home / "shared")}, "sess-b")
        assert not conflict["ok"] and conflict["conflict"]["owner_session"] == "sess-a"
        sent = _call("coord_send", {"recipient": "sess-b", "body": "shared dir is mine",
                                    "dedup_key": "k"}, "sess-a")
        assert sent["ok"]
        again = _call("coord_send", {"recipient": "sess-b", "body": "retry", "dedup_key": "k"}, "sess-a")
        assert again["duplicate"] and again["id"] == sent["id"]
        status = _call("coord_status", {}, "sess-a")
        assert status["ok"] and len(status["claims"]) == 1
        assert status["sent"][0]["delivered_at"] is None  # not drained yet
        inbox = _call("coord_inbox", {}, "sess-b")
        assert inbox["messages"][0]["body"] == "shared dir is mine"
        rel = _call("coord_release", {"resource": str(_home / "shared")}, "sess-a")
        assert rel["ok"]
        assert _call("coord_claim", {"resource": str(_home / "shared")}, "sess-b")["ok"]

    def test_sessions_discovery_lists_claims(self, _home):
        _call("coord_claim", {"resource": str(_home / "x")}, "sess-a")
        r = _call("coord_sessions", {}, "sess-b")
        assert r["ok"]
        assert any(c["owner_session"] == "sess-a" for c in r["claims"])

    def test_integration_release_requires_evidence_via_tool(self, worktree):
        repo, _wt = worktree
        resource = f"repo:{repo}#integrate:main"
        assert _call("coord_claim", {"resource": resource}, "sess-a")["ok"]
        bad = _call("coord_release", {"resource": resource, "outcome": "merged"}, "sess-a")
        assert not bad["ok"]
        ok = _call("coord_release", {"resource": resource, "outcome": "merged",
                                     "evidence": {"gate_command": "true", "exit_code": 0}}, "sess-a")
        assert ok["ok"]

    def test_depend_registration(self, worktree):
        repo, _wt = worktree
        resource = f"repo:{repo}#integrate:main"
        head = _git(repo, "rev-parse", "main").stdout.strip()
        r = _call("coord_depend", {"resource": resource, "base_sha": head}, "sess-dep")
        assert r["ok"]
        bad = _call("coord_depend", {"resource": str(repo), "base_sha": head}, "sess-dep")
        assert not bad["ok"]

    def test_gate_disables_tools(self, monkeypatch):
        monkeypatch.setattr(co, "coordination_config", lambda: {"enabled": False})
        assert coord_tools._gate() is False

    def test_toolset_membership(self):
        import toolsets
        names = [t for t in toolsets._HERMES_CORE_TOOLS if t.startswith("coord_")]
        assert set(names) == {"coord_claim", "coord_release", "coord_status", "coord_send",
                              "coord_inbox", "coord_sessions", "coord_depend"}
        assert toolsets.TOOLSETS["coordination"]["tools"] == names


class TestFileWriteEnforcement:
    def test_write_file_second_session_refused(self, worktree):
        from tools.file_tools import write_file_tool
        _repo, wt = worktree
        first = json.loads(write_file_tool(str(wt / "a.txt"), "one", task_id="t-a", session_id="sess-a"))
        assert "error" not in first, first
        second = json.loads(write_file_tool(str(wt / "b.txt"), "two", task_id="t-b", session_id="sess-b"))
        assert "error" in second and "sess-a" in second["error"]
        assert not (wt / "b.txt").exists()
        # owner unaffected
        third = json.loads(write_file_tool(str(wt / "c.txt"), "three", task_id="t-a", session_id="sess-a"))
        assert "error" not in third, third

    def test_patch_second_session_refused(self, worktree):
        from tools.file_tools import write_file_tool, patch_tool
        _repo, wt = worktree
        write_file_tool(str(wt / "a.txt"), "one", task_id="t-a", session_id="sess-a")
        r = json.loads(patch_tool(mode="replace", path=str(wt / "a.txt"),
                                  old_string="one", new_string="two",
                                  task_id="t-b", session_id="sess-b"))
        assert "error" in r and "sess-a" in r["error"]
        assert (wt / "a.txt").read_text() == "one"

    def test_canonical_checkout_shared_writes_allowed(self, worktree):
        from tools.file_tools import write_file_tool
        repo, _wt = worktree
        a = json.loads(write_file_tool(str(repo / "a.txt"), "one", task_id="t-a", session_id="sess-a"))
        b = json.loads(write_file_tool(str(repo / "b.txt"), "two", task_id="t-b", session_id="sess-b"))
        assert "error" not in a and "error" not in b
        assert co.active_claims() == []  # never auto-claimed


class TestTerminalEnforcement:
    def test_workdir_owned_by_other_session_refused(self, worktree):
        from tools.terminal_tool import terminal_tool
        _repo, wt = worktree
        co.claim(co.worktree_key(str(wt)), "worktree", "sess-a")
        r = json.loads(terminal_tool("echo hi", task_id="t-b", session_id="sess-b",
                                     workdir=str(wt), timeout=30))
        assert r.get("error") and "sess-a" in r["error"]
        ok = json.loads(terminal_tool("echo hi", task_id="t-a", session_id="sess-a",
                                      workdir=str(wt), timeout=30))
        assert ok.get("exit_code") == 0, ok
