import threading
from unittest.mock import patch

import pytest

from src.threads.abstract_thread import AbstractThread


# ── Concrete stubs ───────────────────────────────────────────────────────────


class SuccessThread(AbstractThread):
    """handle() completes normally."""

    def __init__(self):
        self.call_count = 0

    def handle(self):
        self.call_count += 1


class AlwaysCrashThread(AbstractThread):
    """handle() always raises ValueError."""

    def handle(self):
        raise ValueError("boom")


class RuntimeErrorThread(AbstractThread):
    """handle() always raises RuntimeError."""

    def handle(self):
        raise RuntimeError("runtime boom")


class BlockingThread(AbstractThread):
    """Blocks inside handle() until stop_event is set."""

    def __init__(self, stop_event: threading.Event):
        self.stop_event = stop_event

    def handle(self):
        self.stop_event.wait(timeout=5)


# ── Helpers ──────────────────────────────────────────────────────────────────


def start_thread(cls, *args):
    """Mirror of manager.start_thread: instantiate cls, run via run()."""
    instance = cls(*args)
    t = threading.Thread(target=instance.run)
    t.start()
    return t


def watchdog_check(active_threads):
    """One iteration of the manager watchdog loop (without the sleep).
    Returns list of classes whose threads were restarted."""
    restarted = []
    for cls_key, t in list(active_threads.items()):
        if not t.is_alive():
            cls, args = cls_key
            active_threads[cls_key] = start_thread(cls, *args)
            restarted.append(cls)
    return restarted


@pytest.fixture
def stop_event():
    """Provides a threading.Event and guarantees it is set after the test."""
    event = threading.Event()
    yield event
    event.set()


# ── AbstractThread.run() ─────────────────────────────────────────────────────


class TestAbstractThreadRun:
    def test_run_calls_handle(self):
        th = SuccessThread()
        th.run()
        assert th.call_count == 1

    def test_run_does_not_raise_on_success(self):
        th = SuccessThread()
        th.run()  # must not raise

    def test_run_reraises_value_error(self):
        th = AlwaysCrashThread()
        with pytest.raises(ValueError, match="boom"):
            th.run()

    def test_run_reraises_runtime_error(self):
        th = RuntimeErrorThread()
        with pytest.raises(RuntimeError, match="runtime boom"):
            th.run()

    def test_run_logs_error_on_crash(self):
        th = AlwaysCrashThread()
        with patch("src.threads.abstract_thread.logger") as mock_logger:
            with pytest.raises(ValueError):
                th.run()
        mock_logger.error.assert_called_once()

    def test_run_logs_class_name_in_message(self):
        th = AlwaysCrashThread()
        with patch("src.threads.abstract_thread.logger") as mock_logger:
            with pytest.raises(ValueError):
                th.run()
        logged_msg = mock_logger.error.call_args[0][0]
        assert "AlwaysCrashThread" in logged_msg

    def test_run_logs_exception_object_in_extra(self):
        th = AlwaysCrashThread()
        with patch("src.threads.abstract_thread.logger") as mock_logger:
            with pytest.raises(ValueError):
                th.run()
        extra = mock_logger.error.call_args[1]["extra"]
        assert str(extra["exception"]) == "boom"

    def test_run_logs_traceback_in_extra(self):
        th = AlwaysCrashThread()
        with patch("src.threads.abstract_thread.logger") as mock_logger:
            with pytest.raises(ValueError):
                th.run()
        extra = mock_logger.error.call_args[1]["extra"]
        assert "traceback" in extra
        # traceback must mention the exception type and the raising method
        assert "ValueError" in extra["traceback"]
        assert "handle" in extra["traceback"]

    def test_run_logs_before_reraising(self):
        """Log must be written even though the exception propagates out."""
        logged = []
        th = AlwaysCrashThread()

        with patch("src.threads.abstract_thread.logger") as mock_logger:
            mock_logger.error.side_effect = lambda *a, **kw: logged.append(True)
            with pytest.raises(ValueError):
                th.run()

        assert len(logged) == 1

    def test_run_does_not_log_on_success(self):
        th = SuccessThread()
        with patch("src.threads.abstract_thread.logger") as mock_logger:
            th.run()
        mock_logger.error.assert_not_called()

    def test_run_preserves_exception_type_for_different_errors(self):
        """run() re-raises the original exception type, not a wrapped one."""

        class KeyThread(AbstractThread):
            def handle(self):
                raise KeyError("missing_key")

        th = KeyThread()
        with pytest.raises(KeyError, match="missing_key"):
            th.run()


# ── Thread crash detection (is_alive behaviour) ──────────────────────────────


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestThreadCrashDetection:
    def test_crashed_thread_is_not_alive(self):
        t = start_thread(AlwaysCrashThread)
        t.join(timeout=2)
        assert not t.is_alive()

    def test_completed_thread_is_not_alive(self):
        """handle() returning normally also makes is_alive() False."""
        t = start_thread(SuccessThread)
        t.join(timeout=2)
        assert not t.is_alive()

    def test_running_thread_is_alive(self, stop_event):
        t = start_thread(BlockingThread, stop_event)
        # give the thread a moment to start
        assert t.is_alive()

    def test_thread_dies_after_crash_not_before(self):
        """is_alive() is True while handle() is executing, False after crash."""
        started = threading.Event()
        crashed = threading.Event()

        class SlowCrashThread(AbstractThread):
            def handle(self):
                started.set()
                crashed.wait(timeout=2)
                raise ValueError("delayed crash")

        t = threading.Thread(target=SlowCrashThread().run)
        t.start()
        started.wait(timeout=2)
        assert t.is_alive()  # still running before we trigger the crash
        crashed.set()
        t.join(timeout=2)
        assert not t.is_alive()


# ── Watchdog restart pattern ─────────────────────────────────────────────────


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestWatchdogRestartPattern:
    def _make_key(self, cls, *args):
        return (cls, args)

    def test_watchdog_detects_dead_thread(self):
        key = self._make_key(AlwaysCrashThread)
        active_threads = {key: start_thread(AlwaysCrashThread)}
        active_threads[key].join(timeout=2)

        restarted = watchdog_check(active_threads)

        assert AlwaysCrashThread in restarted

    def test_watchdog_replaces_thread_object(self):
        key = self._make_key(AlwaysCrashThread)
        active_threads = {key: start_thread(AlwaysCrashThread)}
        original = active_threads[key]
        original.join(timeout=2)

        watchdog_check(active_threads)

        assert active_threads[key] is not original

    def test_restarted_thread_is_a_thread_object(self):
        key = self._make_key(AlwaysCrashThread)
        active_threads = {key: start_thread(AlwaysCrashThread)}
        active_threads[key].join(timeout=2)

        watchdog_check(active_threads)

        assert isinstance(active_threads[key], threading.Thread)

    def test_watchdog_does_not_restart_running_thread(self, stop_event):
        key = self._make_key(BlockingThread, stop_event)
        active_threads = {key: start_thread(BlockingThread, stop_event)}
        original = active_threads[key]

        restarted = watchdog_check(active_threads)

        assert BlockingThread not in restarted
        assert active_threads[key] is original

    def test_watchdog_only_restarts_dead_threads_among_mixed(self, stop_event):
        """Dead thread restarted; healthy thread left untouched."""
        crash_key = self._make_key(AlwaysCrashThread)
        block_key = self._make_key(BlockingThread, stop_event)
        active_threads = {
            crash_key: start_thread(AlwaysCrashThread),
            block_key: start_thread(BlockingThread, stop_event),
        }
        original_block = active_threads[block_key]
        active_threads[crash_key].join(timeout=2)

        restarted = watchdog_check(active_threads)

        assert AlwaysCrashThread in restarted
        assert BlockingThread not in restarted
        assert active_threads[block_key] is original_block

    def test_watchdog_can_restart_multiple_times(self):
        key = self._make_key(AlwaysCrashThread)
        active_threads = {key: start_thread(AlwaysCrashThread)}

        for _ in range(3):
            active_threads[key].join(timeout=2)
            watchdog_check(active_threads)

        # After 3 restarts the latest thread has also crashed
        active_threads[key].join(timeout=2)
        assert not active_threads[key].is_alive()

    def test_watchdog_restarts_calls_handle_on_new_instance(self):
        """Each restart creates a fresh instance; call_count starts from 0."""
        instances = []

        class TrackingThread(AbstractThread):
            def __init__(self):
                self.call_count = 0
                instances.append(self)

            def handle(self):
                self.call_count += 1
                raise ValueError("always crash after one call")

        key = self._make_key(TrackingThread)
        active_threads = {key: start_thread(TrackingThread)}
        active_threads[key].join(timeout=2)

        watchdog_check(active_threads)
        active_threads[key].join(timeout=2)

        assert len(instances) == 2
        assert all(i.call_count == 1 for i in instances)

    def test_no_restart_when_all_threads_healthy(self, stop_event):
        key = self._make_key(BlockingThread, stop_event)
        active_threads = {key: start_thread(BlockingThread, stop_event)}

        restarted = watchdog_check(active_threads)

        assert restarted == []
