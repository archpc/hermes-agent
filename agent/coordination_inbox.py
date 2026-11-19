"""Checkpoint inbox delivery for cross-session coordination (G1).

Drains the durable inbox (hermes_cli/coordination.py) into the agent's existing
steer buffer, so messages ride the proven out-of-band delivery path: a standalone
user row after the current tool batch, or the next-turn fallback. Delivery is the
acknowledgment (the drain CAS-marks delivered/acked); no reply message is created,
so there is no acknowledgment recursion.

Fail-open everywhere: coordination must never break a turn. Known limitation: a
hard interrupt that wipes the steer buffer after a drain can drop the notice from
context; the message remains visible via coord_inbox (marked delivered).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def drain_into_steer(agent) -> int:
    """Take delivery of pending inbox messages and queue them as steer text.
    Returns the number of messages delivered (0 when none/undeliverable)."""
    try:
        # Persistence-disabled forks share their parent's session id; draining
        # there would steal the parent's mail.
        if getattr(agent, "_persist_disabled", False):
            return 0
        sid = getattr(agent, "session_id", None)
        if not sid:
            return 0
        steer = getattr(agent, "steer", None)
        if not callable(steer):
            return 0
        from hermes_cli import coordination
        if not coordination.enabled():
            return 0
        messages = coordination.drain_inbox(str(sid))
        if not messages:
            return 0
        if steer(coordination.format_inbox_block(messages)):
            return len(messages)
        return 0
    except Exception:
        logger.debug("coordination inbox drain failed (fail-open)", exc_info=True)
        return 0
