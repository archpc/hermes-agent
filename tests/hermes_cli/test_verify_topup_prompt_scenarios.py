"""Scenario coverage for the free-text prompt paths (c01e455b645 + the Ctrl+C cancel fix).

Exercises cancellation, timeout, application shutdown, thread leaks, and terminal-state
restoration around ``_prompt_text_input``. Run it from a checkout outside the Hermes home —
``tests/home_io_guard.py`` refuses the ``manifest.json`` read when the repo lives inside
``~/.hermes``, which is why CI-style ``pytest`` from a default install errors out here.

Never touches billing: the auto-top-up PATCH is always mocked.
"""

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import cli as cli_mod


def _make_cli(app=None):
    obj = object.__new__(cli_mod.HermesCLI)
    obj._app = app
    obj._status_bar_visible = True
    obj._invalidate = lambda *a, **k: None
    return obj


def _start_loop():
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()

    threading.Thread(target=_run, name="scenario-app-loop", daemon=True).start()
    assert ready.wait(5)
    return loop


def _app(loop):
    return SimpleNamespace(loop=loop, invalidate=lambda: None, is_running=True)


def _run_in_thread(fn, timeout=10.0):
    """Run fn in a worker thread; returns (finished, result, exception)."""
    box = {}

    def _wrap():
        try:
            box["result"] = fn()
        except BaseException as exc:
            box["exc"] = exc

    t = threading.Thread(target=_wrap, name="scenario-worker", daemon=True)
    t.start()
    t.join(timeout)
    return (not t.is_alive(), box.get("result"), box.get("exc"))


async def _never_returns(func):
    await asyncio.sleep(3600)


def test_encoded_ctrl_c_cancels_instead_of_poisoning_the_answer():
    """tmux `extended-keys` delivers Ctrl+C as CSI 27;5;99~; it must cancel, not join the amount."""
    cli = _make_cli(_app(_start_loop()))

    async def _rit(func):
        return func()

    def _call():
        with patch("prompt_toolkit.application.run_in_terminal", _rit), \
             patch("builtins.input", return_value="\x1b[27;5;99~25"):
            return cli._prompt_text_input("Threshold: ")

    finished, value, exc = _run_in_thread(_call)
    assert finished, "prompt hung on encoded Ctrl+C"
    assert exc is None
    assert value is None, f"escape bytes leaked into the answer: {value!r}"


def test_billing_wizard_encoded_ctrl_c_sends_no_patch(capsys):
    cli, captured = _billing_cli(_start_loop())
    state = _state()

    async def _rit(func):
        return func()

    finished, _value, exc = _run_in_thread(
        lambda: _flow(cli, state, _rit, ["\x1b[27;5;99~", "50"]))
    out = capsys.readouterr().out
    assert finished and exc is None
    assert captured == {}, f"auto-reload mutated on interrupt: {captured}"
    assert "Cancelled." in out


def test_keyboard_interrupt_cancels_without_hanging():
    cli = _make_cli(_app(_start_loop()))

    async def _rit(func):
        return func()

    def _call():
        with patch("prompt_toolkit.application.run_in_terminal", _rit), \
             patch("builtins.input", side_effect=KeyboardInterrupt()):
            return cli._prompt_text_input("Threshold: ")

    finished, value, exc = _run_in_thread(_call)
    assert finished, "prompt hung on KeyboardInterrupt"
    assert exc is None and value is None
    assert cli._status_bar_visible is True


def test_prompt_timeout_returns_none_and_unblocks():
    cli = _make_cli(_app(_start_loop()))
    started = time.monotonic()
    with patch("prompt_toolkit.application.run_in_terminal", _never_returns):
        value = cli._prompt_text_input_via_app_loop("Threshold: ", timeout=0.5)
    elapsed = time.monotonic() - started
    assert value is None
    assert elapsed < 5, f"timeout path took {elapsed:.1f}s"
    assert cli._status_bar_visible is True


def test_closed_loop_falls_back_to_cancel():
    loop = asyncio.new_event_loop()
    loop.close()
    cli = _make_cli(_app(loop))
    with patch("builtins.input", side_effect=AssertionError("input() must not be called")):
        assert cli._prompt_text_input("Threshold: ") is None


def test_app_shutdown_mid_prompt_does_not_crash_the_worker():
    """Loop stops while the prompt is pending: worker must exit, not raise or hang."""
    loop = _start_loop()
    cli = _make_cli(_app(loop))
    box = {}

    async def _pending(func):
        await asyncio.sleep(3600)

    def _worker():
        try:
            with patch("prompt_toolkit.application.run_in_terminal", _pending):
                box["value"] = cli._prompt_text_input_via_app_loop("Threshold: ", timeout=5)
        except BaseException as exc:
            box["exc"] = exc

    t = threading.Thread(target=_worker, name="scenario-shutdown", daemon=True)
    t.start()
    time.sleep(0.3)
    loop.call_soon_threadsafe(loop.stop)
    t.join(8)

    assert not t.is_alive(), "worker hung after app shutdown"
    assert "exc" not in box, f"worker raised during app shutdown: {box.get('exc')!r}"
    assert box.get("value") is None


def test_no_thread_leak_across_ok_cancel_and_timeout():
    cli = _make_cli(_app(_start_loop()))
    baseline = {t.ident for t in threading.enumerate()}

    async def _ok(func):
        return func()

    def _exercise():
        with patch("prompt_toolkit.application.run_in_terminal", _ok), \
             patch("builtins.input", return_value="10"):
            first = cli._prompt_text_input("Threshold: ")
        with patch("prompt_toolkit.application.run_in_terminal", _never_returns):
            second = cli._prompt_text_input_via_app_loop("Threshold: ", timeout=0.3)
        return first, second

    finished, value, exc = _run_in_thread(_exercise)
    assert finished, "prompt worker hung"
    assert exc is None, exc
    assert value == ("10", None), value

    time.sleep(0.2)
    leaked = [t for t in threading.enumerate() if t.ident not in baseline and t.is_alive()]
    assert not leaked, f"leaked threads: {[(t.name, t.daemon) for t in leaked]}"


def _billing_cli(loop):
    cli = _make_cli(_app(loop))
    cli._block_header = lambda *a, **k: None
    cli._dim = lambda *a, **k: None
    cli._billing_portal_hint = lambda *a, **k: None
    cli._billing_require_admin = lambda state, **k: True
    cli._modal_choice = lambda *a, **k: "agree"
    captured = {}

    def _patch_auto_top_up(state, **kwargs):
        captured.update(kwargs)
        return True

    cli._billing_patch_auto_top_up = _patch_auto_top_up
    return cli, captured


def _state():
    from decimal import Decimal
    return SimpleNamespace(
        card=SimpleNamespace(masked="mastercard ····7638"),
        auto_reload=SimpleNamespace(enabled=False, threshold_usd=None, reload_to_usd=None),
        min_usd=Decimal(5),
        max_usd=Decimal(500),
        portal_url="https://portal.example/billing")


def test_billing_wizard_user_cancel_keeps_autoreload_off(capsys):
    """EOF at the second field: wizard prints Cancelled., sends no PATCH."""
    cli, captured = _billing_cli(_start_loop())
    state = _state()

    async def _rit(func):
        return func()

    finished, _value, exc = _run_in_thread(lambda: _flow(cli, state, _rit, ["25", EOFError()]))
    out = capsys.readouterr().out
    assert finished, "billing wizard hung on EOF"
    assert exc is None
    assert captured == {}, f"auto-reload was mutated on cancel: {captured}"
    assert "Cancelled." in out


def test_billing_wizard_interrupt_at_first_field_sends_no_patch():
    cli, captured = _billing_cli(_start_loop())
    state = _state()

    async def _rit(func):
        return func()

    finished, _value, exc = _run_in_thread(lambda: _flow(cli, state, _rit, [KeyboardInterrupt()]))
    assert finished, "billing wizard hung on interrupt"
    assert exc is None
    assert captured == {}


def _flow(cli, state, rit, answers):
    with patch("prompt_toolkit.application.run_in_terminal", rit), \
         patch("builtins.input", side_effect=answers):
        cli._billing_auto_reload_flow(state)
