"""Regression: ``/topup`` → Auto-reload must reach the PATCH, not print "Cancelled.".

The slash command runs on the ``process_loop`` daemon thread. When the amount prompts came back
``None`` from the #23185 stdin guard, the wizard printed ``cli.billing.cancelled_yellow`` before
either field was shown and nothing was configured. This drives the real flow from a worker thread
with a live app loop.
"""

import asyncio
import threading
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch


def _start_loop():
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()

    threading.Thread(target=_run, name="fake-app-loop", daemon=True).start()
    assert ready.wait(5), "fake app loop did not start"
    return loop


def _make_cli(loop):
    import cli as cli_mod

    obj = object.__new__(cli_mod.HermesCLI)
    obj._app = SimpleNamespace(loop=loop, invalidate=lambda: None, is_running=True)
    obj._status_bar_visible = True
    obj._invalidate = lambda *a, **k: None
    obj._block_header = lambda *a, **k: None
    obj._dim = lambda *a, **k: None
    obj._billing_portal_hint = lambda *a, **k: None
    obj._billing_require_admin = lambda state, **k: True
    obj._modal_choice = lambda *a, **k: "agree"
    captured = {}

    def _patch(state, **kwargs):
        captured.update(kwargs)
        return True

    obj._billing_patch_auto_top_up = _patch
    return obj, captured


def _state():
    return SimpleNamespace(
        card=SimpleNamespace(masked="mastercard ····7638"),
        auto_reload=SimpleNamespace(enabled=False, threshold_usd=None, reload_to_usd=None),
        min_usd=Decimal(5),
        max_usd=Decimal(500),
        portal_url="https://portal.example/billing")


def test_auto_reload_flow_from_worker_thread_configures_and_does_not_cancel(capsys):
    cli, captured = _make_cli(_start_loop())
    state = _state()
    errors = []

    async def fake_run_in_terminal(func):
        return func()

    def worker():
        try:
            with patch("prompt_toolkit.application.run_in_terminal", fake_run_in_terminal), \
                 patch("builtins.input", side_effect=["25", "50"]):
                cli._billing_auto_reload_flow(state)
        except BaseException as exc:  # surfaced below, not swallowed
            errors.append(exc)

    t = threading.Thread(target=worker, name="process_loop", daemon=True)
    t.start()
    t.join(timeout=15)

    out = capsys.readouterr().out
    assert not t.is_alive(), "billing wizard blocked forever"
    assert not errors, errors
    assert captured == {"enabled": True, "threshold": 25.0, "top_up_amount": 50.0}
    assert "Cancelled." not in out
