"""Real multiprocess coordination tests: races, crashes, restarts, stale ownership.

Every child is a separate OS process with its own SQLite connection against the
same isolated HERMES_HOME — no mocked single-process shortcuts.
"""
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from hermes_cli import coordination as co

REPO_ROOT = Path(__file__).resolve().parents[2]

_CLAIM_DRIVER = """
import json, sys
from hermes_cli import coordination as co
res = co.claim(sys.argv[1], "path", sys.argv[2])
print(json.dumps(res))
"""

_SEND_DRIVER = """
import json, sys
from hermes_cli import coordination as co
res = co.send_message(sys.argv[1], sys.argv[2], sys.argv[3], dedup_key=sys.argv[4])
print(json.dumps(res))
"""

_DRAIN_DRIVER = """
import json, sys
from hermes_cli import coordination as co
print(json.dumps(co.drain_inbox(sys.argv[1], limit=100)))
"""

_CLAIM_HOLD_DRIVER = """
import json, sys, time
from hermes_cli import coordination as co
res = co.claim(sys.argv[1], "path", sys.argv[2])
print(json.dumps(res), flush=True)
time.sleep(120)
"""

_RACE_DRIVER = """
import json, sys, time
from hermes_cli import coordination as co
res = co.claim(sys.argv[1], "path", sys.argv[2])
print(json.dumps(res), flush=True)
time.sleep(60)
"""


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


def _spawn(code, home, *args):
    env = dict(os.environ)
    env["HERMES_HOME"] = str(home)
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen([sys.executable, "-c", code, *args],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env=env, cwd=str(REPO_ROOT))


def _json_out(proc, timeout=60):
    out, err = proc.communicate(timeout=timeout)
    assert proc.returncode == 0, f"child failed: {err[-2000:]}"
    return json.loads(out.strip().splitlines()[-1])


class TestClaimRaces:
    def test_eight_processes_one_winner(self, _home):
        # Holders stay alive for the whole race: a claim-and-exit driver would
        # legitimately cascade dead-owner reclaims and prove nothing about CAS.
        procs = [_spawn(_RACE_DRIVER, _home, "path:/race", f"sess-{i}") for i in range(8)]
        try:
            results = []
            for p in procs:
                assert p.stdout is not None
                results.append(json.loads(p.stdout.readline()))
            winners = [r for r in results if r.get("ok")]
            losers = [r for r in results if not r.get("ok")]
            assert len(winners) == 1, results
            assert len(losers) == 7
            held = co.get_claim("path:/race")
            assert held is not None
            owner = held["owner_session"]
            assert all(r["conflict"]["owner_session"] == owner for r in losers)
            assert all(r["conflict"]["reason"] == "owner-live" for r in losers)
        finally:
            for p in procs:
                p.kill()
            for p in procs:
                p.wait(timeout=10)

    def test_crashed_owner_reclaimed(self, _home):
        child = _spawn(_CLAIM_DRIVER, _home, "path:/crash", "sess-dead")
        assert _json_out(child)["ok"]
        # child exited: its pid is gone
        res = co.claim("path:/crash", "path", "sess-new")
        assert res["ok"]
        assert res["reclaimed_from"]["reason"] == "owner-dead"

    def test_killed_owner_reclaimed(self, _home):
        child = _spawn(_CLAIM_HOLD_DRIVER, _home, "path:/kill", "sess-killed")
        assert child.stdout is not None
        first = json.loads(child.stdout.readline())
        assert first["ok"]
        os.kill(child.pid, signal.SIGKILL)
        child.wait(timeout=10)
        res = co.claim("path:/kill", "path", "sess-new")
        assert res["ok"]
        assert res["reclaimed_from"]["reason"] == "owner-dead"

    def test_live_owner_not_reclaimed(self, _home):
        child = _spawn(_CLAIM_HOLD_DRIVER, _home, "path:/live", "sess-live")
        assert child.stdout is not None
        try:
            assert json.loads(child.stdout.readline())["ok"]
            res = co.claim("path:/live", "path", "sess-other")
            assert not res["ok"] and res["conflict"]["reason"] == "owner-live"
        finally:
            child.kill()
            child.wait(timeout=10)


class TestMessageRaces:
    def test_concurrent_dedup_send_single_row(self, _home):
        procs = [_spawn(_SEND_DRIVER, _home, "s", "r", f"body-{i}", "same-key")
                 for i in range(6)]
        results = [_json_out(p) for p in procs]
        assert all(r["ok"] for r in results)
        ids = {r["id"] for r in results}
        assert len(ids) == 1
        assert sum(1 for r in results if r["duplicate"]) == 5
        assert len(co.drain_inbox("r")) == 1

    def test_concurrent_drains_partition_exactly_once(self, _home):
        for i in range(30):
            co.send_message("s", "r", f"m{i}")
        procs = [_spawn(_DRAIN_DRIVER, _home, "r") for _ in range(5)]
        batches = [_json_out(p) for p in procs]
        ids = [m["id"] for batch in batches for m in batch]
        assert len(ids) == 30
        assert len(set(ids)) == 30
        assert co.drain_inbox("r") == []

    def test_restart_recovery_delivers_queued_mail(self, _home):
        # recipient "down": nothing drains; mail must survive for the next process
        co.send_message("s", "r", "while-you-were-out")
        child = _spawn(_DRAIN_DRIVER, _home, "r")
        taken = _json_out(child)
        assert len(taken) == 1 and taken[0]["body"] == "while-you-were-out"
        assert taken[0]["acked_at"] is not None
