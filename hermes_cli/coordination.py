"""Cross-session coordination: resource claims, addressed messages, dependencies.

One SQLite file (``coordination.db`` under HERMES_HOME), WAL + ``BEGIN IMMEDIATE``
compare-and-swap — the same claim pattern as ``hermes_cli/kanban_db.py``, scoped to
resources instead of tasks. No daemon: every operation is a short transaction in the
calling session's own process; stale ownership is recovered from pid+start-time
fingerprints and TTL expiry, never from a background sweeper.

Scope (honest boundaries):
- Claims are cooperative Hermes-side ownership. Enforcement lives at the file-write
  and terminal-cwd tool boundaries and at explicit integration releases. Arbitrary
  shell commands, direct SQLite writers, and processes outside Hermes are NOT
  confined; this is not OS isolation.
- Messages are durable and addressed to a session id. Delivery happens when the
  recipient session next drains its inbox at a turn checkpoint (at-least-once with
  recipient-side CAS dedup). An idle session that never takes another turn keeps its
  mail queued; nothing is lost, nothing is pushed.
- Acknowledgment is a state transition on the message row (delivered/acked), never a
  return message — there is no acknowledgment recursion.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_CLAIM_TTL_SECONDS = 900.0
# A claim whose owner pid is alive but whose heartbeat is older than its TTL is
# treated as wedged and reclaimable (kanban #29747 pattern): expires_at already
# encodes heartbeat + TTL, so plain expiry is the wedge test.

_SCHEMA = """
CREATE TABLE IF NOT EXISTS claims (
    resource_key    TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    owner_session   TEXT NOT NULL,
    owner_pid       INTEGER NOT NULL,
    owner_start     REAL NOT NULL,
    claim_lock      TEXT NOT NULL,
    acquired_at     REAL NOT NULL,
    heartbeat_at    REAL NOT NULL,
    expires_at      REAL NOT NULL,
    meta            TEXT NOT NULL DEFAULT '{}',
    released_at     REAL,
    release_outcome TEXT
);
CREATE TABLE IF NOT EXISTS claim_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_key  TEXT NOT NULL,
    event         TEXT NOT NULL,
    actor_session TEXT,
    detail        TEXT,
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claim_events_key ON claim_events(resource_key, id);
CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    dedup_key       TEXT UNIQUE,
    sender_session  TEXT NOT NULL,
    sender_pid      INTEGER,
    recipient_session TEXT NOT NULL,
    kind            TEXT NOT NULL DEFAULT 'message',
    body            TEXT NOT NULL,
    in_reply_to     TEXT,
    created_at      REAL NOT NULL,
    delivered_at    REAL,
    acked_at        REAL
);
CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_session, delivered_at, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_session, created_at);
CREATE TABLE IF NOT EXISTS dependencies (
    id                TEXT PRIMARY KEY,
    dependent_session TEXT NOT NULL,
    integration_key   TEXT NOT NULL,
    base_sha          TEXT NOT NULL,
    note              TEXT,
    created_at        REAL NOT NULL,
    notified_at       REAL,
    UNIQUE(dependent_session, integration_key)
);
"""


# ── configuration ──────────────────────────────────────────────────────

def coordination_config() -> dict[str, Any]:
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly() or {}
        section = cfg.get("coordination") or {}
        return section if isinstance(section, dict) else {}
    except Exception:
        return {}


def enabled() -> bool:
    """Master gate (config coordination.enabled, default True). Fail-open on config
    errors: a broken config read must not wedge tool execution."""
    try:
        return bool(coordination_config().get("enabled", True))
    except Exception:
        return True


def claim_ttl_seconds() -> float:
    try:
        raw = coordination_config().get("claim_ttl_seconds", DEFAULT_CLAIM_TTL_SECONDS)
        ttl = float(raw)
        return ttl if ttl > 0 else DEFAULT_CLAIM_TTL_SECONDS
    except Exception:
        return DEFAULT_CLAIM_TTL_SECONDS


# ── storage ────────────────────────────────────────────────────────────

def db_path() -> Path:
    from hermes_constants import get_hermes_home
    return Path(get_hermes_home()) / "coordination.db"


def _connect(path: Optional[Path] = None) -> sqlite3.Connection:
    target = Path(path) if path else db_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    return conn


# ── identity / liveness ────────────────────────────────────────────────

def process_start_time(pid: int) -> Optional[float]:
    try:
        import psutil  # type: ignore
        return float(psutil.Process(pid).create_time())
    except Exception:
        return None


def own_identity() -> tuple[int, float]:
    pid = os.getpid()
    start = process_start_time(pid)
    return pid, (start if start is not None else 0.0)


def _owner_alive(pid: int, start: float) -> Optional[bool]:
    """True/False when provable, None when liveness cannot be established.
    A recycled pid (start-time mismatch) is dead. Unknown is NOT dead: claims
    with unverifiable owners survive until TTL expiry (fail closed)."""
    if not pid or pid <= 0:
        return False
    try:
        import psutil  # type: ignore
        proc = psutil.Process(pid)
        live_start = float(proc.create_time())
    except Exception:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return None
        except OSError:
            return False
        return None  # alive but start time unknown
    if start and abs(live_start - start) > 1.0:
        return False
    return True


def _claimer_lock() -> str:
    import socket
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"


def session_identity(session_id: Optional[str] = None) -> str:
    """Stable owner id for a calling surface. Falls back to a process-unique id
    when the surface has no durable session id."""
    sid = (session_id or "").strip()
    return sid or f"pid:{os.getpid()}"


# ── resource keys ──────────────────────────────────────────────────────

def worktree_key(root: str) -> str:
    return "wt:" + os.path.realpath(root)


def path_key(path: str) -> str:
    return "path:" + os.path.realpath(path)


def integration_key(repo_root: str, branch: str) -> str:
    return "int:" + os.path.realpath(repo_root) + ":" + branch.strip()


def parse_integration_key(key: str) -> Optional[tuple[str, str]]:
    if not key.startswith("int:"):
        return None
    rest = key[4:]
    repo, sep, branch = rest.rpartition(":")
    if not sep or not repo or not branch:
        return None
    return repo, branch


# ── claims ─────────────────────────────────────────────────────────────

def _event(conn: sqlite3.Connection, key: str, event: str, actor: str,
           detail: Optional[dict] = None) -> None:
    conn.execute(
        "INSERT INTO claim_events (resource_key, event, actor_session, detail, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (key, event, actor, json.dumps(detail or {}, ensure_ascii=False), time.time()))


def _row_live(row: sqlite3.Row, now: float) -> tuple[bool, str]:
    """(conflicts, reason) for an unreleased claim row."""
    alive = _owner_alive(int(row["owner_pid"]), float(row["owner_start"]))
    if alive is False:
        return False, "owner-dead"
    if now >= float(row["expires_at"]):
        return False, "expired"
    return True, ("owner-live" if alive else "owner-unverified")


def claim(resource_key: str, kind: str, owner_session: str, *,
          meta: Optional[dict] = None, ttl_seconds: Optional[float] = None,
          conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    """Atomically claim a resource. Same-owner re-claim renews (idempotent).
    Returns {ok, renewed?, claim_lock?, conflict?: {owner_session, reason, ...}}."""
    if not resource_key or not owner_session:
        return {"ok": False, "error": "resource_key and owner_session are required"}
    ttl = float(ttl_seconds) if ttl_seconds and ttl_seconds > 0 else claim_ttl_seconds()
    pid, start = own_identity()
    now = time.time()
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM claims WHERE resource_key = ?", (resource_key,)).fetchone()
        if row is not None and row["released_at"] is None:
            if row["owner_session"] == owner_session:
                lock = row["claim_lock"]
                c.execute(
                    "UPDATE claims SET heartbeat_at=?, expires_at=?, meta=?, owner_pid=?, owner_start=? "
                    "WHERE resource_key=? AND released_at IS NULL AND owner_session=?",
                    (now, now + ttl, json.dumps(meta or json.loads(row["meta"] or "{}"), ensure_ascii=False),
                     pid, start, resource_key, owner_session))
                _event(c, resource_key, "renewed", owner_session)
                c.commit()
                return {"ok": True, "renewed": True, "claim_lock": lock,
                        "expires_at": now + ttl}
            conflicts, reason = _row_live(row, now)
            if conflicts:
                c.rollback()
                return {"ok": False, "conflict": {
                    "resource_key": resource_key,
                    "owner_session": row["owner_session"],
                    "owner_pid": int(row["owner_pid"]),
                    "kind": row["kind"],
                    "acquired_at": float(row["acquired_at"]),
                    "expires_at": float(row["expires_at"]),
                    "reason": reason,
                }}
            prior = {"owner_session": row["owner_session"], "reason": reason}
        else:
            prior = None
        lock = _claimer_lock()
        c.execute(
            "INSERT INTO claims (resource_key, kind, owner_session, owner_pid, owner_start, "
            "claim_lock, acquired_at, heartbeat_at, expires_at, meta, released_at, release_outcome) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL) "
            "ON CONFLICT(resource_key) DO UPDATE SET kind=excluded.kind, "
            "owner_session=excluded.owner_session, owner_pid=excluded.owner_pid, "
            "owner_start=excluded.owner_start, claim_lock=excluded.claim_lock, "
            "acquired_at=excluded.acquired_at, heartbeat_at=excluded.heartbeat_at, "
            "expires_at=excluded.expires_at, meta=excluded.meta, "
            "released_at=NULL, release_outcome=NULL",
            (resource_key, kind, owner_session, pid, start, lock, now, now, now + ttl,
             json.dumps(meta or {}, ensure_ascii=False)))
        _event(c, resource_key, "reclaimed" if prior else "claimed", owner_session, prior)
        c.commit()
        return {"ok": True, "renewed": False, "claim_lock": lock, "expires_at": now + ttl,
                **({"reclaimed_from": prior} if prior else {})}
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            c.close()


def heartbeat(resource_key: str, owner_session: str, *,
              ttl_seconds: Optional[float] = None,
              conn: Optional[sqlite3.Connection] = None) -> bool:
    """Extend a claim we own. False when we don't own it (or it is gone)."""
    ttl = float(ttl_seconds) if ttl_seconds and ttl_seconds > 0 else claim_ttl_seconds()
    now = time.time()
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        cur = c.execute(
            "UPDATE claims SET heartbeat_at=?, expires_at=? "
            "WHERE resource_key=? AND released_at IS NULL AND owner_session=?",
            (now, now + ttl, resource_key, owner_session))
        c.commit()
        return cur.rowcount > 0
    finally:
        if own_conn:
            c.close()


def release(resource_key: str, owner_session: str, *, outcome: str = "released",
            evidence: Optional[dict] = None,
            conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    """Release a claim we own (CAS on owner). Integration claims released with
    outcome='merged' require gate evidence: a gate command that exited 0.
    Dependent sessions are notified in the same transaction (G4)."""
    now = time.time()
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM claims WHERE resource_key=? AND released_at IS NULL",
                        (resource_key,)).fetchone()
        if row is None:
            c.rollback()
            return {"ok": False, "error": "no active claim", "resource_key": resource_key}
        if row["owner_session"] != owner_session:
            c.rollback()
            return {"ok": False, "error": "not the owner",
                    "owner_session": row["owner_session"], "resource_key": resource_key}
        if row["kind"] == "integration" and outcome == "merged":
            err = _validate_merge_evidence(evidence)
            if err:
                c.rollback()
                return {"ok": False, "error": err, "resource_key": resource_key}
        meta = json.loads(row["meta"] or "{}")
        if evidence:
            meta["evidence"] = evidence
        c.execute(
            "UPDATE claims SET released_at=?, release_outcome=?, meta=? "
            "WHERE resource_key=? AND released_at IS NULL AND owner_session=?",
            (now, outcome, json.dumps(meta, ensure_ascii=False), resource_key, owner_session))
        _event(c, resource_key, "released", owner_session,
               {"outcome": outcome, "evidence": evidence or {}})
        notified: list[str] = []
        if row["kind"] == "integration" and outcome == "merged":
            notified = _notify_dependents(c, resource_key, owner_session, now, evidence or {})
        c.commit()
        return {"ok": True, "resource_key": resource_key, "outcome": outcome,
                "notified_dependents": notified}
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            c.close()


def _validate_merge_evidence(evidence: Optional[dict]) -> Optional[str]:
    """Fail closed: a merged integration must carry a passing gate receipt."""
    if not isinstance(evidence, dict):
        return ("integration release with outcome='merged' requires evidence: "
                "{gate_command, exit_code: 0, ...}")
    gate = str(evidence.get("gate_command") or "").strip()
    if not gate:
        return "evidence.gate_command is required (the verification command that ran)"
    raw_exit = evidence.get("exit_code")
    if raw_exit is None:
        return "evidence.exit_code is required and must be an integer"
    try:
        exit_code = int(raw_exit)
    except (TypeError, ValueError):
        return "evidence.exit_code is required and must be an integer"
    if exit_code != 0:
        return f"evidence.exit_code must be 0 for outcome='merged' (got {exit_code})"
    return None


def active_claims(conn: Optional[sqlite3.Connection] = None) -> list[dict[str, Any]]:
    own_conn = conn is None
    c = conn or _connect()
    try:
        rows = c.execute(
            "SELECT * FROM claims WHERE released_at IS NULL ORDER BY acquired_at").fetchall()
        now = time.time()
        out = []
        for r in rows:
            d = dict(r)
            d["meta"] = json.loads(d.get("meta") or "{}")
            conflicts, reason = _row_live(r, now)
            d["live"] = conflicts
            d["liveness"] = reason
            out.append(d)
        return out
    finally:
        if own_conn:
            c.close()


def get_claim(resource_key: str, conn: Optional[sqlite3.Connection] = None) -> Optional[dict]:
    own_conn = conn is None
    c = conn or _connect()
    try:
        r = c.execute("SELECT * FROM claims WHERE resource_key=? AND released_at IS NULL",
                      (resource_key,)).fetchone()
        if r is None:
            return None
        d = dict(r)
        d["meta"] = json.loads(d.get("meta") or "{}")
        return d
    finally:
        if own_conn:
            c.close()


# ── enforcement helpers (tool boundaries) ──────────────────────────────

_WT_CACHE: dict[str, tuple[Optional[str], float]] = {}
_WT_CACHE_TTL = 60.0
_WT_CACHE_MAX = 4096


def _worktree_root(path: str) -> Optional[str]:
    """The linked git worktree root containing *path*, or None. Canonical
    checkouts (git-dir == common-dir) return None: they are shared by design and
    are only governed by explicit claims, never auto-claimed.

    Results are cached per directory for 60s: write-boundary checks run on every
    file write and three git spawns per write would be a real tax. A worktree's
    identity does not change while it exists; a new worktree at a cached path is
    picked up when the entry expires."""
    import subprocess
    try:
        candidate = path if os.path.isdir(path) else os.path.dirname(path)
        if not candidate or not os.path.isdir(candidate):
            return None
        now = time.time()
        hit = _WT_CACHE.get(candidate)
        if hit is not None and now - hit[1] < _WT_CACHE_TTL:
            return hit[0]
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=candidate,
                             capture_output=True, text=True, timeout=5,
                             stdin=subprocess.DEVNULL, check=False)
        root: Optional[str] = None
        if top.returncode == 0:
            cand_root = os.path.realpath(top.stdout.strip())
            gitdir = subprocess.run(["git", "rev-parse", "--absolute-git-dir"], cwd=cand_root,
                                    capture_output=True, text=True, timeout=5,
                                    stdin=subprocess.DEVNULL, check=False)
            common = subprocess.run(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
                                    cwd=cand_root, capture_output=True, text=True, timeout=5,
                                    stdin=subprocess.DEVNULL, check=False)
            if (gitdir.returncode == 0 and common.returncode == 0
                    and os.path.realpath(gitdir.stdout.strip()) != os.path.realpath(common.stdout.strip())):
                root = cand_root
        if len(_WT_CACHE) >= _WT_CACHE_MAX:
            _WT_CACHE.clear()
        _WT_CACHE[candidate] = (root, now)
        return root
    except Exception:
        return None


def check_write_boundary(path: str, session_id: Optional[str], *,
                         auto_claim_worktrees: bool = True) -> Optional[str]:
    """Refusal reason when *path* is owned by another live session, else None.

    Order: explicit path claims covering the target, then the linked-worktree
    owner. An unclaimed linked worktree is auto-claimed by the first writer
    (one writable worktree per writer). Unknown/unprovable states fail OPEN for
    access (no claim known) but claims themselves fail CLOSED on conflict.
    """
    if not enabled():
        return None
    owner = session_identity(session_id)
    try:
        target = os.path.realpath(path)
    except Exception:
        return None
    try:
        claims = active_claims()
    except Exception:
        logger.debug("coordination: claim read failed; allowing write", exc_info=True)
        return None
    now = time.time()
    covering: list[dict] = []
    for cl in claims:
        if cl["kind"] == "path" and cl["resource_key"].startswith("path:"):
            base = cl["resource_key"][5:]
            if target == base or target.startswith(base + os.sep):
                covering.append(cl)
    wt_root = _worktree_root(target)
    if wt_root:
        wt_cl = next((cl for cl in claims
                      if cl["kind"] == "worktree" and cl["resource_key"] == worktree_key(wt_root)), None)
        if wt_cl:
            covering.append(wt_cl)
    for cl in covering:
        if cl["owner_session"] == owner:
            if now - float(cl["heartbeat_at"]) > claim_ttl_seconds() / 4:
                try:
                    heartbeat(cl["resource_key"], owner)
                except Exception:
                    pass
            continue
        if not cl["live"]:
            continue  # dead/expired: reclaimable below, not a conflict
        return (f"resource owned by another live session: {cl['resource_key']} "
                f"(owner {cl['owner_session']}, pid {cl['owner_pid']}, "
                f"expires {cl['expires_at']:.0f}). Coordinate via coord_send, wait for "
                f"release, or work in your own worktree. Not os-level confinement: "
                f"this refusal is Hermes tool-boundary enforcement.")
    if auto_claim_worktrees and wt_root:
        try:
            res = claim(worktree_key(wt_root), "worktree", owner,
                        meta={"auto": True, "first_writer": True})
            if not res.get("ok") and res.get("conflict"):
                other = res["conflict"]
                return (f"worktree {wt_root} was just claimed by {other['owner_session']} "
                        f"(pid {other['owner_pid']}). Work in your own worktree.")
        except Exception:
            logger.debug("coordination: auto-claim failed; allowing write", exc_info=True)
    return None


def check_cwd_boundary(cwd: str, session_id: Optional[str]) -> Optional[str]:
    """Terminal working-directory gate: same ownership rules as writes."""
    if not enabled() or not cwd:
        return None
    return check_write_boundary(cwd, session_id, auto_claim_worktrees=False)


# ── messages (G1) ──────────────────────────────────────────────────────

def send_message(sender_session: str, recipient_session: str, body: str, *,
                 kind: str = "message", dedup_key: Optional[str] = None,
                 in_reply_to: Optional[str] = None,
                 conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    """Durable addressed message. Unique id; optional sender idempotency key.
    Returns {ok, id, duplicate}."""
    if not sender_session or not recipient_session or not str(body).strip():
        return {"ok": False, "error": "sender, recipient and non-empty body are required"}
    if sender_session == recipient_session:
        return {"ok": False, "error": "cannot message yourself"}
    mid = uuid.uuid4().hex
    now = time.time()
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        if dedup_key:
            prior = c.execute("SELECT id FROM messages WHERE dedup_key=?", (dedup_key,)).fetchone()
            if prior:
                c.rollback()
                return {"ok": True, "id": prior["id"], "duplicate": True}
        c.execute(
            "INSERT INTO messages (id, dedup_key, sender_session, sender_pid, recipient_session, "
            "kind, body, in_reply_to, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mid, dedup_key, sender_session, os.getpid(), recipient_session, kind,
             str(body), in_reply_to, now))
        c.commit()
        return {"ok": True, "id": mid, "duplicate": False}
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            c.close()


def drain_inbox(recipient_session: str, *, limit: int = 50,
                conn: Optional[sqlite3.Connection] = None) -> list[dict[str, Any]]:
    """Atomically take delivery of undelivered messages (CAS delivered_at).
    Concurrent drains never hand the same message to two contexts. Delivery is
    acknowledgment: acked_at is set with delivered_at; no reply message is
    generated (no ack recursion)."""
    if not recipient_session:
        return []
    now = time.time()
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        rows = c.execute(
            "SELECT * FROM messages WHERE recipient_session=? AND delivered_at IS NULL "
            "ORDER BY created_at LIMIT ?", (recipient_session, int(limit))).fetchall()
        taken = []
        for r in rows:
            cur = c.execute(
                "UPDATE messages SET delivered_at=?, acked_at=? "
                "WHERE id=? AND delivered_at IS NULL", (now, now, r["id"]))
            if cur.rowcount:
                d = dict(r)
                d["delivered_at"] = now  # the SELECT predates the CAS update
                d["acked_at"] = now
                taken.append(d)
        c.commit()
        return taken
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        if own_conn:
            c.close()


def inbox_history(recipient_session: str, *, limit: int = 50,
                  conn: Optional[sqlite3.Connection] = None) -> list[dict[str, Any]]:
    own_conn = conn is None
    c = conn or _connect()
    try:
        rows = c.execute(
            "SELECT * FROM messages WHERE recipient_session=? ORDER BY created_at DESC LIMIT ?",
            (recipient_session, int(limit))).fetchall()
        return [dict(r) for r in rows]
    finally:
        if own_conn:
            c.close()


def sent_status(sender_session: str, *, limit: int = 50,
                conn: Optional[sqlite3.Connection] = None) -> list[dict[str, Any]]:
    own_conn = conn is None
    c = conn or _connect()
    try:
        rows = c.execute(
            "SELECT id, recipient_session, kind, created_at, delivered_at, acked_at, in_reply_to "
            "FROM messages WHERE sender_session=? ORDER BY created_at DESC LIMIT ?",
            (sender_session, int(limit))).fetchall()
        return [dict(r) for r in rows]
    finally:
        if own_conn:
            c.close()


def format_inbox_block(messages: list[dict[str, Any]]) -> str:
    """The checkpoint-injection framing: attributed, id'd, explicitly not the user."""
    lines = ["[COORDINATION INBOX — messages from other Hermes sessions, not the user]"]
    for m in messages:
        lines.append(f"- id={m['id']} from={m['sender_session']} kind={m['kind']} "
                     f"at={m['created_at']:.0f}: {m['body']}")
    lines.append("These were delivered to your session inbox. Acknowledge by acting on them; "
                 "do not send acknowledgment-only replies (no ack recursion).")
    return "\n".join(lines)


# ── dependencies + integration notification (G4) ───────────────────────

def register_dependency(dependent_session: str, key: str, base_sha: str, *,
                        note: str = "", conn: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
    if not dependent_session or not key or not base_sha:
        return {"ok": False, "error": "dependent_session, integration_key and base_sha are required"}
    if parse_integration_key(key) is None:
        return {"ok": False, "error": f"not an integration key: {key}"}
    own_conn = conn is None
    c = conn or _connect()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "INSERT INTO dependencies (id, dependent_session, integration_key, base_sha, note, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(dependent_session, integration_key) DO UPDATE SET "
            "base_sha=excluded.base_sha, note=excluded.note, created_at=excluded.created_at, "
            "notified_at=NULL",
            (uuid.uuid4().hex, dependent_session, key, base_sha, note, time.time()))
        c.commit()
        return {"ok": True, "integration_key": key, "base_sha": base_sha}
    finally:
        if own_conn:
            c.close()


def _notify_dependents(c: sqlite3.Connection, key: str, integrator: str,
                       now: float, evidence: dict) -> list[str]:
    rows = c.execute(
        "SELECT * FROM dependencies WHERE integration_key=? AND dependent_session != ?",
        (key, integrator)).fetchall()
    notified = []
    for r in rows:
        dep = r["dependent_session"]
        dedup = f"integration-landed:{key}:{now:.6f}:{dep}"
        body = (f"Integration landed on {key} by {integrator}. Your dependency was registered "
                f"at base_sha {r['base_sha']}. Revalidate before continuing: rebase onto the new "
                f"target head, rerun your gates, then re-register the dependency with the new "
                f"base_sha. Gate evidence: {json.dumps(evidence, ensure_ascii=False)}")
        c.execute(
            "INSERT OR IGNORE INTO messages (id, dedup_key, sender_session, sender_pid, "
            "recipient_session, kind, body, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (uuid.uuid4().hex, dedup, integrator, os.getpid(), dep,
             "integration_landed", body, now))
        c.execute("UPDATE dependencies SET notified_at=? WHERE id=?", (now, r["id"]))
        notified.append(dep)
    return notified


def check_integration_revalidation(key: str, claimant_session: str) -> Optional[str]:
    """A claimant with a registered dependency must have revalidated onto the
    current target head (base_sha match) before claiming the integration."""
    parsed = parse_integration_key(key)
    if parsed is None:
        return None
    repo, branch = parsed
    import subprocess
    try:
        head = subprocess.run(["git", "rev-parse", "--verify", branch], cwd=repo,
                              capture_output=True, text=True, timeout=10,
                              stdin=subprocess.DEVNULL, check=False)
        current = head.stdout.strip() if head.returncode == 0 else ""
    except Exception:
        return None  # cannot prove a mismatch: do not invent one
    if not current:
        return None
    c = _connect()
    try:
        row = c.execute(
            "SELECT base_sha FROM dependencies WHERE dependent_session=? AND integration_key=?",
            (claimant_session, key)).fetchone()
    finally:
        c.close()
    if row is None:
        return None
    if row["base_sha"] != current:
        return (f"dependent branch not revalidated: dependency registered at {row['base_sha']} "
                f"but {branch} is now {current}. Rebase, rerun gates, then coord_depend again "
                f"with the new base_sha before integrating.")
    return None


# ── read-only snapshot (G3) ────────────────────────────────────────────

def snapshot(*, include_messages: bool = True, message_limit: int = 20) -> dict[str, Any]:
    """Everything the coord view renders. Read-only; never mutates."""
    out: dict[str, Any] = {"generated_at": time.time(), "enabled": enabled()}
    try:
        from hermes_cli.active_sessions import active_session_registry_snapshot
        out["active_sessions"] = active_session_registry_snapshot()
    except Exception as exc:
        out["active_sessions"] = []
        out["active_sessions_error"] = str(exc)
    try:
        out["goals"] = _active_goals()
    except Exception as exc:
        out["goals"] = []
        out["goals_error"] = str(exc)
    try:
        c = _connect()
        try:
            out["claims"] = active_claims(conn=c)
            out["dependencies"] = [dict(r) for r in c.execute(
                "SELECT * FROM dependencies ORDER BY created_at").fetchall()]
            out["recent_events"] = [dict(r) for r in c.execute(
                "SELECT * FROM claim_events ORDER BY id DESC LIMIT 50").fetchall()]
            if include_messages:
                out["messages"] = [dict(r) for r in c.execute(
                    "SELECT id, sender_session, recipient_session, kind, created_at, "
                    "delivered_at, acked_at FROM messages ORDER BY created_at DESC LIMIT ?",
                    (int(message_limit),)).fetchall()]
        finally:
            c.close()
    except Exception as exc:
        out["coordination_error"] = str(exc)
    return out


def _active_goals() -> list[dict[str, Any]]:
    from hermes_constants import get_hermes_home
    db = Path(get_hermes_home()) / "state.db"
    if not db.exists():
        return []
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = conn.execute(
            "SELECT key, value FROM state_meta WHERE key LIKE 'goal:%'").fetchall()
    finally:
        conn.close()
    goals = []
    for key, raw in rows:
        try:
            state = json.loads(raw)
        except Exception:
            continue
        if state.get("status") != "active":
            continue
        goals.append({
            "session_id": key[5:],
            "goal": state.get("goal", ""),
            "turns_used": state.get("turns_used", 0),
            "max_turns": state.get("max_turns", 0),
            "waiting_on_pid": state.get("waiting_on_pid"),
            "waiting_reason": state.get("waiting_reason"),
        })
    return goals
