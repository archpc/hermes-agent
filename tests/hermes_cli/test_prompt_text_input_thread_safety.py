"""Tests for ``HermesCLI._prompt_text_input`` thread-safe input dispatch.

Raw ``input()`` prompts can race with prompt_toolkit when called from the TUI.
The normal slash confirmations now use a prompt_toolkit-native modal, but
``_prompt_text_input`` remains the free-text path (billing amounts) and as a
fallback for non-interactive calls. From the ``process_loop`` worker it must
route through the app loop rather than cancelling — the #23185 guard used to
return ``None``, so ``/topup`` → Auto-reload printed "Cancelled." without ever
showing a field.
"""

import asyncio
import threading
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _make_cli():
    """Minimal HermesCLI shell exposing prompt fallback helpers."""
    import cli as cli_mod

    obj = object.__new__(cli_mod.HermesCLI)
    obj._app = MagicMock()
    obj._status_bar_visible = True
    obj._invalidate = lambda *a, **k: None
    return obj


def _start_loop():
    """Run an asyncio loop on a background thread (stand-in for the prompt_toolkit app loop)."""
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _run():
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()

    threading.Thread(target=_run, name="fake-app-loop", daemon=True).start()
    assert ready.wait(5), "fake app loop did not start"
    return loop


class TestPromptTextInputThreadSafety:

    def test_background_thread_cancels_instead_of_hanging(self):
        """With an app but no usable event loop, cancel cleanly (return None).

        stdin is owned by the prompt_toolkit event loop / JSON-RPC pipe on the
        non-main (process_loop / slash-worker) thread, so a bare input() there
        would block until the worker's timeout (#23185 / billing auto-reload
        hang). No live loop to schedule onto → cancel; never call input().
        """
        cli = _make_cli()

        result_holder = {}

        def run_on_daemon():
            with patch("prompt_toolkit.application.run_in_terminal") as mock_rit, \
                 patch("builtins.input", side_effect=AssertionError("input() must not be called off-main-thread")) as mock_input:
                result_holder["value"] = cli._prompt_text_input("Choice [1/2/3]: ")
                result_holder["rit_called"] = mock_rit.called
                result_holder["input_called"] = mock_input.called

        t = threading.Thread(target=run_on_daemon, daemon=True)
        t.start()
        t.join(timeout=2.0)
        assert not t.is_alive(), "daemon thread hung — guard did not cancel cleanly"

        # Cancelled cleanly: None returned, neither run_in_terminal nor input() called.
        assert result_holder["value"] is None
        assert result_holder["rit_called"] is False
        assert result_holder["input_called"] is False

    def test_no_app_uses_direct_input(self):
        """Without an active prompt_toolkit app, always call input() directly."""
        cli = _make_cli()
        cli._app = None

        with patch("builtins.input", return_value="cancel") as mock_input:
            result = cli._prompt_text_input("Choice: ")

        assert mock_input.called
        assert result == "cancel"

    def test_run_in_terminal_exception_falls_back(self):
        """If run_in_terminal raises (WSL / Warp edge cases), fall back to input()."""
        cli = _make_cli()

        with patch(
            "prompt_toolkit.application.run_in_terminal",
            side_effect=RuntimeError("event loop dropped the coroutine"),
        ), patch("builtins.input", return_value="3") as mock_input:
            result = cli._prompt_text_input("Choice: ")

        assert mock_input.called
        assert result == "3"

    def test_eof_returns_none(self):
        """EOFError from input() yields None, not an unhandled exception."""
        cli = _make_cli()
        cli._app = None

        with patch("builtins.input", side_effect=EOFError()):
            result = cli._prompt_text_input("Choice: ")

        assert result is None


class TestPromptViaAppLoop:
    """A worker thread must get real input, not an instant ``None`` (#23185 follow-up)."""

    def test_background_thread_prompts_through_the_app_loop(self):
        cli = _make_cli()
        cli._app.loop = _start_loop()
        ran_on = {}
        seen = {}

        async def fake_run_in_terminal(func):
            ran_on["thread"] = threading.current_thread().name
            return func()

        def worker():
            with patch("prompt_toolkit.application.run_in_terminal", fake_run_in_terminal), \
                 patch("builtins.input", return_value=" 25 ") as mock_input:
                seen["value"] = cli._prompt_text_input("When balance falls below (USD): ")
                seen["input_called"] = mock_input.called

        t = threading.Thread(target=worker, name="process_loop", daemon=True)
        t.start()
        t.join(timeout=10)

        assert not t.is_alive(), "worker blocked forever on the prompt"
        assert seen["input_called"] is True
        assert seen["value"] == "25"
        assert ran_on.get("thread") == "fake-app-loop", "prompt did not run on the app loop"

    def test_empty_answer_returns_none_not_a_cancel_sentinel(self):
        cli = _make_cli()
        cli._app.loop = _start_loop()

        async def fake_run_in_terminal(func):
            return func()

        def worker():
            with patch("prompt_toolkit.application.run_in_terminal", fake_run_in_terminal), \
                 patch("builtins.input", return_value="   "):
                assert cli._prompt_text_input("Threshold: ") is None

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive()
