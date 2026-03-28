import threading
from datetime import datetime
from unittest.mock import patch

import pytest

from src.threads.abstract_thread import AbstractThread


# ── Concrete stubs ───────────────────────────────────────────────────────────


class SuccessThread(AbstractThread):
    """handle() completes normally."""

    def __init__(self):
        super().__init__()
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
        super().__init__()
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


# ── AbstractThread state (start_time / last_error_time / _record_error) ──────


class TestAbstractThreadState:
    def test_init_sets_start_time(self):
        before = datetime.now()
        th = AlwaysCrashThread()
        after = datetime.now()
        assert before <= th.start_time <= after

    def test_init_sets_last_error_time_none(self):
        th = AlwaysCrashThread()
        assert th.last_error_time is None

    def test_init_sets_error_count_zero(self):
        th = AlwaysCrashThread()
        assert th.error_count == 0

    def test_record_error_increments_error_count(self):
        th = AlwaysCrashThread()
        th._record_error()
        assert th.error_count == 1

    def test_record_error_accumulates_on_repeated_calls(self):
        th = AlwaysCrashThread()
        th._record_error()
        th._record_error()
        assert th.error_count == 2

    def test_run_does_not_increment_error_count_on_success(self):
        th = SuccessThread()
        th.run()
        assert th.error_count == 0

    def test_run_does_not_increment_error_count_on_crash(self):
        th = AlwaysCrashThread()
        with pytest.raises(ValueError):
            th.run()
        assert th.error_count == 0

    def test_record_error_sets_last_error_time(self):
        th = AlwaysCrashThread()
        before = datetime.now()
        th._record_error()
        after = datetime.now()
        assert before <= th.last_error_time <= after

    def test_record_error_updates_on_repeated_calls(self):
        th = AlwaysCrashThread()
        th._record_error()
        first = th.last_error_time
        th._record_error()
        assert th.last_error_time >= first

    def test_run_does_not_set_last_error_time_on_crash(self):
        th = AlwaysCrashThread()
        with pytest.raises(ValueError):
            th.run()
        assert th.last_error_time is None

    def test_run_does_not_set_last_error_time_on_success(self):
        th = SuccessThread()
        th.run()
        assert th.last_error_time is None

    def test_subclass_with_custom_init_has_start_time_when_super_called(self):
        th = SuccessThread()
        assert isinstance(th.start_time, datetime)

    def test_subclass_with_custom_init_has_last_error_time_none_when_super_called(self):
        th = SuccessThread()
        assert th.last_error_time is None

    def test_record_error_does_not_affect_start_time(self):
        th = AlwaysCrashThread()
        original_start = th.start_time
        th._record_error()
        assert th.start_time == original_start


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
                super().__init__()
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


# ── Crash counted on new instance (manager pattern) ──────────────────────────


def manager_start_thread(cls):
    """Mirror of manager.py start_thread: returns (t, instance)."""
    instance = cls()
    t = threading.Thread(target=instance.run)
    t.start()
    return t, instance


def manager_watchdog_step(active_threads):
    """One watchdog iteration mirroring manager.py: calls _record_error on the new instance."""
    for cls, (t, instance) in list(active_threads.items()):
        if not t.is_alive():
            new_t, new_instance = manager_start_thread(cls)
            new_instance._record_error()
            active_threads[cls] = new_t, new_instance


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
class TestCrashCountedOnRestart:
    """Verify the manager counts a crash as an error on the replacement instance."""

    def test_new_instance_has_error_count_one_after_crash(self):
        t, instance = manager_start_thread(AlwaysCrashThread)
        t.join(timeout=2)

        active_threads = {AlwaysCrashThread: (t, instance)}
        manager_watchdog_step(active_threads)

        _, new_instance = active_threads[AlwaysCrashThread]
        assert new_instance.error_count == 1

    def test_new_instance_does_not_inherit_old_error_count(self):
        """Error count resets on restart — no carryover from previous run."""
        instance = AlwaysCrashThread()
        instance.error_count = 99  # simulate many prior errors on old instance
        t = threading.Thread(target=instance.run)
        t.start()
        t.join(timeout=2)

        active_threads = {AlwaysCrashThread: (t, instance)}
        manager_watchdog_step(active_threads)

        _, new_instance = active_threads[AlwaysCrashThread]
        assert new_instance.error_count == 1  # only the crash, not 100
