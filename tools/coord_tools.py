"""Model-facing coordination tools (G1/G2/G4): claims, addressed messages, dependencies.

Backed by hermes_cli/coordination.py (one SQLite file, CAS claims, durable inbox).
These tools are cooperative Hermes-side coordination, not OS confinement: they
govern Hermes tool boundaries and explicit integration releases, nothing else.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

from tools.registry import registry

logger = logging.getLogger(__name__)


def _gate() -> bool:
    try:
        from hermes_cli import coordination
        return coordination.enabled()
    except Exception:
        return False


def _owner(kw: dict) -> str:
    from hermes_cli.coordination import session_identity
    return session_identity(kw.get("session_id"))


def _parse_resource(resource: str) -> tuple[str, str]:
    """'repo:<path>#integrate:<branch>' -> integration key; anything else -> path key."""
    from hermes_cli import coordination
    text = (resource or "").strip()
    if text.startswith("repo:") and "#integrate:" in text:
        repo, branch = text[5:].split("#integrate:", 1)
        return coordination.integration_key(os.path.expanduser(repo), branch), "integration"
    return coordination.path_key(os.path.expanduser(text)), "path"


def _coord_claim(args: dict, **kw) -> str:
    from hermes_cli import coordination
    resource = str(args.get("resource") or "").strip()
    if not resource:
        return json.dumps({"ok": False, "error": "resource is required (a path, or repo:<path>#integrate:<branch>)"})
    key, kind = _parse_resource(resource)
    owner = _owner(kw)
    if kind == "integration":
        if refusal := coordination.check_integration_revalidation(key, owner):
            return json.dumps({"ok": False, "error": refusal})
    note = str(args.get("note") or "")
    res = coordination.claim(key, kind, owner, meta={"note": note} if note else {})
    res["resource_key"] = key
    return json.dumps(res, ensure_ascii=False)


def _coord_release(args: dict, **kw) -> str:
    from hermes_cli import coordination
    resource = str(args.get("resource") or "").strip()
    if not resource:
        return json.dumps({"ok": False, "error": "resource is required"})
    key, _kind = _parse_resource(resource)
    outcome = str(args.get("outcome") or "released").strip() or "released"
    evidence = args.get("evidence")
    if evidence is not None and not isinstance(evidence, dict):
        return json.dumps({"ok": False, "error": "evidence must be an object"})
    res = coordination.release(key, _owner(kw), outcome=outcome, evidence=evidence)
    return json.dumps(res, ensure_ascii=False)


def _coord_status(args: dict, **kw) -> str:
    from hermes_cli import coordination
    owner = _owner(kw)
    claims = [c for c in coordination.active_claims() if c["owner_session"] == owner]
    inbox = coordination.inbox_history(owner, limit=10)
    pending = [m for m in inbox if m["delivered_at"] is None]
    return json.dumps({
        "ok": True, "session": owner,
        "claims": claims,
        "sent": coordination.sent_status(owner, limit=20),
        "inbox_pending": len(pending),
        "inbox_recent": [{k: m[k] for k in ("id", "sender_session", "kind", "created_at", "delivered_at")}
                         for m in inbox],
    }, ensure_ascii=False)


def _coord_send(args: dict, **kw) -> str:
    from hermes_cli import coordination
    recipient = str(args.get("recipient") or "").strip()
    body = str(args.get("body") or "")
    if not recipient or not body.strip():
        return json.dumps({"ok": False, "error": "recipient (session id) and body are required"})
    res = coordination.send_message(
        _owner(kw), recipient, body,
        kind=str(args.get("kind") or "message"),
        dedup_key=(str(args.get("dedup_key")) if args.get("dedup_key") else None),
        in_reply_to=(str(args.get("in_reply_to")) if args.get("in_reply_to") else None))
    if res.get("ok") and not res.get("duplicate"):
        res["note"] = ("queued durably; delivered when the recipient session next drains its "
                       "inbox at a turn checkpoint. Delivery is the acknowledgment — do not "
                       "expect or send ack-only replies.")
    return json.dumps(res, ensure_ascii=False)


def _coord_inbox(args: dict, **kw) -> str:
    from hermes_cli import coordination
    owner = _owner(kw)
    limit = int(args.get("limit") or 20)
    history = coordination.inbox_history(owner, limit=max(1, min(limit, 100)))
    return json.dumps({"ok": True, "session": owner, "messages": history}, ensure_ascii=False)


def _coord_sessions(args: dict, **kw) -> str:
    from hermes_cli import coordination
    snap = coordination.snapshot(include_messages=False)
    return json.dumps({
        "ok": True,
        "active_sessions": snap.get("active_sessions", []),
        "active_goals": snap.get("goals", []),
        "claims": snap.get("claims", []),
        "note": "session ids here are valid coord_send recipients within this profile",
    }, ensure_ascii=False)


def _coord_depend(args: dict, **kw) -> str:
    from hermes_cli import coordination
    resource = str(args.get("resource") or "").strip()
    base_sha = str(args.get("base_sha") or "").strip()
    if not resource or not base_sha:
        return json.dumps({"ok": False, "error": "resource (repo:<path>#integrate:<branch>) and base_sha are required"})
    key, kind = _parse_resource(resource)
    if kind != "integration":
        return json.dumps({"ok": False, "error": "dependencies attach to integration resources: repo:<path>#integrate:<branch>"})
    res = coordination.register_dependency(_owner(kw), key, base_sha, note=str(args.get("note") or ""))
    return json.dumps(res, ensure_ascii=False)


def _schema(name: str, description: str, properties: dict, required: list[str]) -> dict:
    return {"name": name, "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required}}


registry.register(
    name="coord_claim", toolset="coordination",
    schema=_schema(
        "coord_claim",
        "Atomically claim exclusive ownership of a writable resource before working on it: "
        "a filesystem path (kind=path) or an integration target 'repo:<path>#integrate:<branch>' "
        "(kind=integration, serialized merges). Conflicts with another LIVE session's claim are "
        "refused (fail closed) with the owner's identity; dead or expired owners are reclaimed. "
        "Re-claiming your own resource renews it. Linked git worktrees are auto-claimed by the "
        "first writer, so normally you only claim shared paths and integration targets explicitly. "
        "Cooperative Hermes-side ownership — not OS confinement.",
        {"resource": {"type": "string", "description": "absolute path, or repo:<path>#integrate:<branch>"},
         "note": {"type": "string", "description": "what you intend to do (visible to other sessions)"}},
        ["resource"]),
    handler=_coord_claim, check_fn=_gate, emoji="🔒")

registry.register(
    name="coord_release", toolset="coordination",
    schema=_schema(
        "coord_release",
        "Release a resource claim you own. For integration claims released with outcome='merged', "
        "evidence is REQUIRED and fail-closed: {gate_command: the verification command that ran, "
        "exit_code: 0, testbench_run_id/review optional but recorded}. Releasing a merged integration "
        "notifies registered dependent sessions to revalidate. Never bypass repo review/CI gates: "
        "the evidence must come from actually running them.",
        {"resource": {"type": "string"},
         "outcome": {"type": "string", "description": "released (default) | merged | abandoned"},
         "evidence": {"type": "object", "description": "{gate_command, exit_code, testbench_run_id?, review?}"}},
        ["resource"]),
    handler=_coord_release, check_fn=_gate, emoji="🔓")

registry.register(
    name="coord_status", toolset="coordination",
    schema=_schema("coord_status",
                   "Your coordination state: active claims you own, sent-message delivery/ack status, "
                   "and inbox summary. Read-only.",
                   {}, []),
    handler=_coord_status, check_fn=_gate, emoji="📋")

registry.register(
    name="coord_send", toolset="coordination",
    schema=_schema(
        "coord_send",
        "Send a durable addressed message to another Hermes session in this profile (recipient = "
        "session id from coord_sessions). Unique message id; optional dedup_key makes retries "
        "idempotent. Delivered into the recipient's context at its next turn checkpoint; the "
        "delivery itself is the acknowledgment (no ack replies). Use for findings, handoffs, "
        "dependency notices — not for work that belongs on the kanban board.",
        {"recipient": {"type": "string", "description": "target session id"},
         "body": {"type": "string", "description": "the message you composed"},
         "dedup_key": {"type": "string", "description": "idempotency key; same key sends once"},
         "in_reply_to": {"type": "string", "description": "message id this replies to"}},
        ["recipient", "body"]),
    handler=_coord_send, check_fn=_gate, emoji="✉️")

registry.register(
    name="coord_inbox", toolset="coordination",
    schema=_schema("coord_inbox",
                   "Read messages addressed to your session (history incl. delivery state). New "
                   "messages are also injected automatically at turn checkpoints; this tool is the "
                   "read-only view, e.g. after a restart.",
                   {"limit": {"type": "integer", "description": "max messages (default 20)"}},
                   []),
    handler=_coord_inbox, check_fn=_gate, emoji="📥")

registry.register(
    name="coord_sessions", toolset="coordination",
    schema=_schema("coord_sessions",
                   "Discover live sessions and active goals in this profile (lease registry + goal "
                   "state), plus current resource claims. Read-only; session ids are coord_send "
                   "recipients.",
                   {}, []),
    handler=_coord_sessions, check_fn=_gate, emoji="🧭")

registry.register(
    name="coord_depend", toolset="coordination",
    schema=_schema(
        "coord_depend",
        "Register that your session's work depends on an integration target "
        "('repo:<path>#integrate:<branch>') at base_sha (the target head you rebased onto and "
        "tested against). When that integration lands you are notified to revalidate; claiming the "
        "integration yourself later requires base_sha to match the current target head (fail "
        "closed on stale revalidation).",
        {"resource": {"type": "string", "description": "repo:<path>#integrate:<branch>"},
         "base_sha": {"type": "string", "description": "target branch head you revalidated against"},
         "note": {"type": "string"}},
        ["resource", "base_sha"]),
    handler=_coord_depend, check_fn=_gate, emoji="🔗")
