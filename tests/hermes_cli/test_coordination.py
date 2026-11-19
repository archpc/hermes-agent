"""Core coordination registry tests (G1/G2/G4): claims, messages, dependencies.

Single-process semantics here; real multiprocess races live in
test_coordination_multiprocess.py.
"""
import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid

import pytest

from hermes_cli import coordination as co


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    co._WT_CACHE.clear()
    yield tmp_path


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL, check=True)


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "t@t")
    _git(root, "config", "user.name", "t")
    (root / "f.txt").write_text("base\n")
    _git(root, "add", "f.txt")
    _git(root, "commit", "-qm", "base")
    return root


@pytest.fixture
def worktree(repo, tmp_path):
    wt = tmp_path / "wt"
    _git(repo, "worktree", "add", "-q", str(wt), "-b", "side")
    return wt


# ── claims ─────────────────────────────────────────────────────────────

class TestClaims:
    def test_claim_then_conflict(self):
        a = co.claim("path:/x", "path", "sess-a")
        assert a["ok"] and not a["renewed"]
        b = co.claim("path:/x", "path", "sess-b")
        assert not b["ok"]
        assert b["conflict"]["owner_session"] == "sess-a"
        assert b["conflict"]["reason"] == "owner-live"

    def test_same_owner_renews_idempotently(self):
        a = co.claim("path:/x", "path", "sess-a")
        b = co.claim("path:/x", "path", "sess-a")
        assert b["ok"] and b["renewed"]
        assert b["claim_lock"] == a["claim_lock"]

    def test_release_cas_and_double_release(self):
        co.claim("path:/x", "path", "sess-a")
        wrong = co.release("path:/x", "sess-b")
        assert not wrong["ok"] and wrong["error"] == "not the owner"
        held = co.get_claim("path:/x")
        assert held and held["owner_session"] == "sess-a"
        ok = co.release("path:/x", "sess-a")
        assert ok["ok"]
        again = co.release("path:/x", "sess-a")
        assert not again["ok"] and again["error"] == "no active claim"
        # released resource is claimable by anyone
        assert co.claim("path:/x", "path", "sess-b")["ok"]

    def test_expired_claim_reclaimed(self):
        co.claim("path:/x", "path", "sess-a", ttl_seconds=0.05)
        time.sleep(0.1)
        res = co.claim("path:/x", "path", "sess-b")
        assert res["ok"]
        assert res["reclaimed_from"]["reason"] == "expired"

    def test_heartbeat_extends(self):
        co.claim("path:/x", "path", "sess-a", ttl_seconds=0.3)
        time.sleep(0.2)
        assert co.heartbeat("path:/x", "sess-a", ttl_seconds=0.5)
        time.sleep(0.2)  # past the original TTL, inside the extended one
        assert not co.claim("path:/x", "path", "sess-b")["ok"]
        assert not co.heartbeat("path:/x", "sess-b")

    def test_dead_owner_reclaimed_before_ttl(self):
        co.claim("path:/x", "path", "sess-a", ttl_seconds=900)
        # Simulate a crashed owner: pid that cannot exist, start fingerprint mismatch.
        c = co._connect()
        try:
            c.execute("UPDATE claims SET owner_pid=?, owner_start=? WHERE resource_key=?",
                      (2**22 + 12345, 1.0, "path:/x"))
            c.commit()
        finally:
            c.close()
        res = co.claim("path:/x", "path", "sess-b")
        assert res["ok"]
        assert res["reclaimed_from"]["reason"] == "owner-dead"

    def test_unverifiable_owner_survives_until_ttl(self):
        # pid alive but start time unknown must NOT be treated as dead (fail closed).
        co.claim("path:/x", "path", "sess-a", ttl_seconds=900)
        c = co._connect()
        try:
            c.execute("UPDATE claims SET owner_pid=?, owner_start=? WHERE resource_key=?",
                      (os.getpid(), 0.0, "path:/x"))
            c.commit()
        finally:
            c.close()
        res = co.claim("path:/x", "path", "sess-b")
        assert not res["ok"]

    def test_events_recorded(self):
        co.claim("path:/x", "path", "sess-a")
        co.claim("path:/x", "path", "sess-a")
        co.release("path:/x", "sess-a")
        snap = co.snapshot()
        events = [e["event"] for e in snap["recent_events"] if e["resource_key"] == "path:/x"]
        assert events == ["released", "renewed", "claimed"]


# ── enforcement boundaries ─────────────────────────────────────────────

class TestBoundaries:
    def test_linked_worktree_first_writer_claims(self, worktree):
        target = str(worktree / "a.txt")
        assert co.check_write_boundary(target, "sess-a") is None
        cl = co.get_claim(co.worktree_key(str(worktree)))
        assert cl and cl["owner_session"] == "sess-a" and cl["kind"] == "worktree"
        # second session refused, fail closed
        refusal = co.check_write_boundary(target, "sess-b")
        assert refusal and "sess-a" in refusal
        # owner keeps writing
        assert co.check_write_boundary(target, "sess-a") is None

    def test_canonical_checkout_not_auto_claimed(self, repo):
        target = str(repo / "a.txt")
        assert co.check_write_boundary(target, "sess-a") is None
        assert co.check_write_boundary(target, "sess-b") is None
        assert co.active_claims() == []

    def test_explicit_path_claim_blocks_subtree(self, tmp_path):
        base = tmp_path / "shared"
        base.mkdir()
        co.claim(co.path_key(str(base)), "path", "sess-a")
        assert co.check_write_boundary(str(base / "sub" / "f.txt"), "sess-b")
        assert co.check_write_boundary(str(base), "sess-b")
        assert co.check_write_boundary(str(base / "f.txt"), "sess-a") is None
        assert co.check_write_boundary(str(tmp_path / "elsewhere.txt"), "sess-b") is None

    def test_cwd_boundary(self, worktree):
        co.claim(co.worktree_key(str(worktree)), "worktree", "sess-a")
        assert co.check_cwd_boundary(str(worktree), "sess-b")
        assert co.check_cwd_boundary(str(worktree), "sess-a") is None

    def test_disabled_gate_allows_everything(self, worktree, monkeypatch):
        monkeypatch.setattr(co, "coordination_config", lambda: {"enabled": False})
        co.claim(co.worktree_key(str(worktree)), "worktree", "sess-a")
        assert co.check_write_boundary(str(worktree / "f.txt"), "sess-b") is None
        assert co.check_cwd_boundary(str(worktree), "sess-b") is None

    def test_dead_worktree_owner_reclaimed_by_writer(self, worktree):
        co.claim(co.worktree_key(str(worktree)), "worktree", "sess-a")
        c = co._connect()
        try:
            c.execute("UPDATE claims SET owner_pid=? WHERE resource_key=?",
                      (2**22 + 999, co.worktree_key(str(worktree))))
            c.commit()
        finally:
            c.close()
        assert co.check_write_boundary(str(worktree / "f.txt"), "sess-b") is None
        reclaimed = co.get_claim(co.worktree_key(str(worktree)))
        assert reclaimed and reclaimed["owner_session"] == "sess-b"


# ── messages ───────────────────────────────────────────────────────────

class TestMessages:
    def test_send_drain_ack(self):
        res = co.send_message("sess-a", "sess-b", "hello")
        assert res["ok"] and not res["duplicate"]
        taken = co.drain_inbox("sess-b")
        assert len(taken) == 1 and taken[0]["body"] == "hello"
        assert taken[0]["acked_at"] is not None  # delivery IS the ack
        assert co.drain_inbox("sess-b") == []
        sent = co.sent_status("sess-a")
        assert sent[0]["delivered_at"] is not None and sent[0]["acked_at"] is not None
        # no ack recursion: draining created no new messages
        assert co.inbox_history("sess-a") == []

    def test_dedup_key(self):
        a = co.send_message("s", "r", "one", dedup_key="k1")
        b = co.send_message("s", "r", "one again", dedup_key="k1")
        assert b["duplicate"] and b["id"] == a["id"]
        assert len(co.drain_inbox("r")) == 1

    def test_self_send_refused(self):
        assert not co.send_message("s", "s", "x")["ok"]

    def test_concurrent_drains_partition(self):
        for i in range(20):
            co.send_message("s", "r", f"m{i}")
        taken: list[list] = []
        lock = threading.Lock()

        def worker():
            got = co.drain_inbox("r", limit=50)
            with lock:
                taken.append(got)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        ids = [m["id"] for batch in taken for m in batch]
        assert len(ids) == 20 and len(set(ids)) == 20

    def test_persist_across_connections(self):
        co.send_message("s", "r", "durable")
        # "restart": brand-new connection sees the queued message
        c = co._connect()
        try:
            rows = c.execute("SELECT count(*) n FROM messages WHERE delivered_at IS NULL").fetchone()
            assert rows["n"] == 1
        finally:
            c.close()
        assert len(co.drain_inbox("r")) == 1

    def test_format_block_attributes_sender(self):
        co.send_message("sess-a", "sess-b", "finding: X")
        block = co.format_inbox_block(co.drain_inbox("sess-b"))
        assert "sess-a" in block and "finding: X" in block
        assert "not the user" in block


# ── integration evidence + dependencies (G4) ───────────────────────────

class TestIntegration:
    def _claim_int(self, repo, owner="sess-a"):
        key = co.integration_key(str(repo), "main")
        assert co.claim(key, "integration", owner)["ok"]
        return key

    def test_merged_requires_evidence(self, repo):
        key = self._claim_int(repo)
        assert not co.release(key, "sess-a", outcome="merged")["ok"]
        assert not co.release(key, "sess-a", outcome="merged",
                              evidence={"gate_command": "make test"})["ok"]
        bad = co.release(key, "sess-a", outcome="merged",
                         evidence={"gate_command": "make test", "exit_code": 1})
        assert not bad["ok"] and "exit_code" in bad["error"]
        assert co.get_claim(key) is not None  # still held: fail closed
        ok = co.release(key, "sess-a", outcome="merged",
                        evidence={"gate_command": "make test", "exit_code": 0,
                                  "testbench_run_id": "tb_x", "review": "PR#1"})
        assert ok["ok"]
        snap = co.snapshot()
        released = [e for e in snap["recent_events"] if e["event"] == "released"]
        assert released and json.loads(released[0]["detail"])["evidence"]["exit_code"] == 0

    def test_non_merged_release_needs_no_evidence(self, repo):
        key = self._claim_int(repo)
        assert co.release(key, "sess-a", outcome="abandoned")["ok"]

    def test_dependents_notified_once(self, repo):
        key = self._claim_int(repo)
        co.register_dependency("sess-dep", key, "abc123")
        co.register_dependency("sess-dep2", key, "abc123")
        res = co.release(key, "sess-a", outcome="merged",
                         evidence={"gate_command": "g", "exit_code": 0})
        assert sorted(res["notified_dependents"]) == ["sess-dep", "sess-dep2"]
        dep_mail = co.drain_inbox("sess-dep")
        assert len(dep_mail) == 1 and dep_mail[0]["kind"] == "integration_landed"
        assert "revalidate" in dep_mail[0]["body"].lower()
        # integrator does not notify itself
        assert co.drain_inbox("sess-a") == []

    def test_revalidation_sha_gate(self, repo):
        key = co.integration_key(str(repo), "main")
        head = _git(repo, "rev-parse", "main").stdout.strip()
        co.register_dependency("sess-dep", key, head)
        assert co.check_integration_revalidation(key, "sess-dep") is None
        # target advances
        (repo / "f.txt").write_text("next\n")
        _git(repo, "commit", "-aqm", "next")
        refusal = co.check_integration_revalidation(key, "sess-dep")
        assert refusal and "revalidat" in refusal.lower()
        # re-register after rebase -> allowed
        new_head = _git(repo, "rev-parse", "main").stdout.strip()
        co.register_dependency("sess-dep", key, new_head)
        assert co.check_integration_revalidation(key, "sess-dep") is None
        # sessions without a registered dependency are not gated
        assert co.check_integration_revalidation(key, "sess-other") is None

    def test_integration_claim_serializes(self, repo):
        key = self._claim_int(repo)
        res = co.claim(key, "integration", "sess-b")
        assert not res["ok"] and res["conflict"]["owner_session"] == "sess-a"


# ── snapshot / config ──────────────────────────────────────────────────

class TestSnapshot:
    def test_shape_and_readonly(self, repo):
        co.claim(co.path_key(str(repo)), "path", "sess-a")
        co.send_message("sess-a", "sess-b", "hi")
        snap = co.snapshot()
        assert snap["enabled"] is True
        assert isinstance(snap["active_sessions"], list)
        assert isinstance(snap["goals"], list)
        assert len(snap["claims"]) == 1
        assert len(snap["messages"]) == 1
        assert "body" not in snap["messages"][0]  # metadata-only listing

    def test_ttl_config_honored(self, monkeypatch):
        monkeypatch.setattr(co, "coordination_config", lambda: {"claim_ttl_seconds": 7})
        res = co.claim("path:/x", "path", "s")
        assert abs(res["expires_at"] - time.time() - 7) < 2
