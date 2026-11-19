"""``hermes coord`` — read-only global coordination view (G3).

Renders active sessions, goals, resource claims, messages, dependencies and
recent claim events from the existing stores (active-session lease registry,
state.db goal meta, coordination.db). No new service, no mutations.
"""
from __future__ import annotations

import argparse
import json
import time
from typing import Any


def build_coord_parser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "coord", help="Read-only view of cross-session coordination",
        description="Active sessions, goals, resource claims, addressed messages, "
                    "integration dependencies and recent claim events. Read-only; "
                    "renders the same data the coord_* tools use.")
    parser.add_argument("--json", action="store_true", help="machine-readable snapshot")
    parser.add_argument("--messages", type=int, default=20,
                        help="max recent messages to include (default 20)")
    parser.set_defaults(func=cmd_coord)
    return parser


def _age(ts: Any, now: float) -> str:
    try:
        secs = max(0.0, now - float(ts))
    except (TypeError, ValueError):
        return "?"
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs / 60:.0f}m"
    if secs < 86400:
        return f"{secs / 3600:.1f}h"
    return f"{secs / 86400:.1f}d"


def _render(snap: dict[str, Any]) -> str:
    now = float(snap.get("generated_at") or time.time())
    lines: list[str] = []
    lines.append(f"coordination: {'enabled' if snap.get('enabled') else 'DISABLED'}")

    sessions = snap.get("active_sessions") or []
    lines.append(f"\nACTIVE SESSIONS ({len(sessions)})")
    for e in sorted(sessions, key=lambda x: x.get("started_at") or 0):
        lines.append(f"  {e.get('surface') or '?':<14} {e.get('session_id') or '?'} "
                     f"pid={e.get('pid')} age={_age(e.get('started_at'), now)}")
    if snap.get("active_sessions_error"):
        lines.append(f"  (registry error: {snap['active_sessions_error']})")

    goals = snap.get("goals") or []
    lines.append(f"\nACTIVE GOALS ({len(goals)})")
    for g in goals:
        goal = str(g.get("goal") or "").replace("\n", " ")
        if len(goal) > 72:
            goal = goal[:72] + "…"
        parked = f" parked:{g.get('waiting_reason') or g.get('waiting_on_pid')}" if g.get("waiting_on_pid") or g.get("waiting_reason") else ""
        lines.append(f"  {g.get('session_id')} [{g.get('turns_used')}/{g.get('max_turns')}]{parked} {goal}")

    claims = snap.get("claims") or []
    lines.append(f"\nRESOURCE CLAIMS ({len(claims)})")
    for c in claims:
        state = "live" if c.get("live") else f"STALE({c.get('liveness')})"
        lines.append(f"  {c.get('resource_key')}  owner={c.get('owner_session')} "
                     f"pid={c.get('owner_pid')} {state} expires_in={_age(now, float(c.get('expires_at') or now))}")
    if not claims:
        lines.append("  (none)")

    deps = snap.get("dependencies") or []
    lines.append(f"\nINTEGRATION DEPENDENCIES ({len(deps)})")
    for d in deps:
        notified = "notified" if d.get("notified_at") else "waiting"
        lines.append(f"  {d.get('dependent_session')} -> {d.get('integration_key')} "
                     f"base={str(d.get('base_sha'))[:12]} {notified}")
    if not deps:
        lines.append("  (none)")

    msgs = snap.get("messages") or []
    lines.append(f"\nRECENT MESSAGES ({len(msgs)})")
    for m in msgs:
        state = "acked" if m.get("acked_at") else ("delivered" if m.get("delivered_at") else "QUEUED")
        lines.append(f"  {str(m.get('id'))[:8]} {m.get('sender_session')} -> "
                     f"{m.get('recipient_session')} [{m.get('kind')}] {state} "
                     f"age={_age(m.get('created_at'), now)}")
    if not msgs:
        lines.append("  (none)")

    events = snap.get("recent_events") or []
    conflicts = [e for e in events if e.get("event") in ("reclaimed",)]
    lines.append(f"\nRECENT CLAIM EVENTS ({len(events)}; {len(conflicts)} reclaims)")
    for e in events[:10]:
        lines.append(f"  {e.get('event'):<10} {e.get('resource_key')} "
                     f"actor={e.get('actor_session')} age={_age(e.get('created_at'), now)}")
    if snap.get("coordination_error"):
        lines.append(f"\n(coordination store error: {snap['coordination_error']})")
    return "\n".join(lines)


def cmd_coord(args: argparse.Namespace) -> int:
    from hermes_cli import coordination
    snap = coordination.snapshot(message_limit=max(0, int(getattr(args, "messages", 20) or 20)))
    if getattr(args, "json", False):
        print(json.dumps(snap, indent=2, ensure_ascii=False, default=str))
    else:
        print(_render(snap))
    return 0
