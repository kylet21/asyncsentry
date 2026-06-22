"""
Core blocking-detection monitor. Runs a background thread that polls the event
loop at a fixed interval; if the loop has not responded within `threshold`
seconds it is considered blocked.

All events are emitted as structured JSON lines to the Python `logging`
infrastructure so they are captured by any log aggregator (stdout in a
container, Loki, CloudWatch, etc.).
"""

from __future__ import annotations

import asyncio
import functools
import gc
import json
import linecache
import logging
import os
import threading
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Coroutine, Dict, List, Optional, TypeVar

import psutil

logger = logging.getLogger("asyncsentry")

F = TypeVar("F")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class BlockEvent:
    event_type: str                   # "block" | "slow_async" | "gc"
    timestamp: float
    duration: float
    thread_id: int
    task_name: Optional[str]
    culprit: Optional[str]            # "file:line in function" of top app frame
    stack_frames: List[Dict[str, Any]]
    cpu_percent: float
    memory_mb: float
    gc_collections: Dict[str, int]
    extra: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SKIP_MODULES = frozenset({
    "asyncsentry",
    "asyncio",
    "threading",
    "concurrent",
    "_bootstrap",
    "linecache",
    "importlib",
})

def _is_app_frame(frame) -> bool:
    module = frame.f_globals.get("__name__", "")
    for skip in _SKIP_MODULES:
        if module.startswith(skip):
            return False
    filename = frame.f_code.co_filename
    return not (
        filename.startswith("<")
        or "site-packages/asyncio" in filename
        or "site-packages/threading" in filename
    )


def _extract_frames(frame, max_frames: int = 20) -> List[Dict[str, Any]]:
    frames: List[Dict[str, Any]] = []
    current = frame
    while current is not None and len(frames) < max_frames:
        co = current.f_code
        filename = co.co_filename
        lineno = current.f_lineno
        source_line = linecache.getline(filename, lineno).strip()
        frames.append({
            "filename": filename,
            "lineno": lineno,
            "function": co.co_name,
            "source": source_line,
            "is_app_frame": _is_app_frame(current),
        })
        current = current.f_back
    return frames


def _find_culprit(frames: List[Dict[str, Any]]) -> Optional[str]:
    """Return the innermost application-level frame as a readable culprit string."""
    for f in frames:
        if f.get("is_app_frame"):
            return f"{f['filename']}:{f['lineno']} in {f['function']}"
    return None


def _gc_stats() -> Dict[str, int]:
    return {f"gen{i}": gc.get_count()[i] for i in range(len(gc.get_count()))}


def _memory_mb() -> float:
    try:
        proc = psutil.Process(os.getpid())
        return proc.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def _cpu_percent() -> float:
    try:
        proc = psutil.Process(os.getpid())
        return proc.cpu_percent(interval=None)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Log emitter
# ---------------------------------------------------------------------------

_LOG_LEVEL_MAP = {
    "block":      logging.WARNING,
    "slow_async": logging.WARNING,
    "gc":         logging.INFO,
    "start":      logging.INFO,
    "stop":       logging.INFO,
}


def _emit(event: BlockEvent) -> None:
    """Serialize an event to a structured JSON log line."""
    payload = {
        "asyncsentry": True,
        "event_type": event.event_type,
        "timestamp": event.timestamp,
        "duration_s": round(event.duration, 4),
        "task_name": event.task_name,
        "culprit": event.culprit,
        "cpu_percent": round(event.cpu_percent, 2),
        "memory_mb": round(event.memory_mb, 2),
        "gc_collections": event.gc_collections,
        "stack_frames": event.stack_frames,
    }
    if event.extra:
        payload.update(event.extra)

    level = _LOG_LEVEL_MAP.get(event.event_type, logging.WARNING)
    logger.log(level, json.dumps(payload, default=str))


def _emit_lifecycle(event_type: str, extra: Optional[Dict] = None) -> None:
    payload: Dict[str, Any] = {
        "asyncsentry": True,
        "event_type": event_type,
        "timestamp": time.time(),
    }
    if extra:
        payload.update(extra)
    logger.info(json.dumps(payload))


# ---------------------------------------------------------------------------
# Async task tracker
# ---------------------------------------------------------------------------

class _TaskTracker:
    """
    Wraps asyncio's task factory to track when tasks start and finish.
    Detects slow coroutines by comparing actual wall-time against
    `async_threshold`.

    Also exposes `watch(coro, name)` so that directly-awaited coroutines
    (not wrapped in create_task) can be timed without spawning a Task.
    """

    def __init__(self, threshold: float) -> None:
        self._threshold = threshold
        self._start_times: Dict[int, float] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Task-factory hook (covers asyncio.create_task / ensure_future)
    # ------------------------------------------------------------------

    def install(self, loop: asyncio.AbstractEventLoop) -> None:
        original_factory = loop.get_task_factory()

        def factory(loop, coro, **kwargs):
            if original_factory is None:
                task = asyncio.Task(coro, loop=loop, **kwargs)
            else:
                task = original_factory(loop, coro, **kwargs)
            self._attach(task)
            return task

        loop.set_task_factory(factory)

    def uninstall(self, loop: asyncio.AbstractEventLoop) -> None:
        loop.set_task_factory(None)

    def _attach(self, task: asyncio.Task) -> None:
        task_id = id(task)
        with self._lock:
            self._start_times[task_id] = time.monotonic()

        def _done_cb(t: asyncio.Task) -> None:
            now = time.monotonic()
            with self._lock:
                start = self._start_times.pop(id(t), None)
            if start is None:
                return
            elapsed = now - start
            if elapsed >= self._threshold:
                _emit(BlockEvent(
                    event_type="slow_async",
                    timestamp=time.time(),
                    duration=elapsed,
                    thread_id=threading.get_ident(),
                    task_name=t.get_name() if hasattr(t, "get_name") else str(t),
                    culprit=None,
                    stack_frames=[],
                    cpu_percent=_cpu_percent(),
                    memory_mb=_memory_mb(),
                    gc_collections=_gc_stats(),
                    extra={"cancelled": t.cancelled()},
                ))

        task.add_done_callback(_done_cb)

    # ------------------------------------------------------------------
    # Direct-await wrapper (covers bare `await coro()` calls)
    # ------------------------------------------------------------------

    async def watch(self, coro: Coroutine, name: Optional[str] = None) -> Any:
        """
        Await `coro` while timing it.  Emits a slow_async event if it
        exceeds the threshold.  Use as a drop-in for direct awaits:

            await sentry.watch(my_coro(), name="my_coro")
        """
        label = name or getattr(coro, "__qualname__", repr(coro))
        start = time.monotonic()
        try:
            return await coro
        finally:
            elapsed = time.monotonic() - start
            if elapsed >= self._threshold:
                _emit(BlockEvent(
                    event_type="slow_async",
                    timestamp=time.time(),
                    duration=elapsed,
                    thread_id=threading.get_ident(),
                    task_name=label,
                    culprit=None,
                    stack_frames=[],
                    cpu_percent=_cpu_percent(),
                    memory_mb=_memory_mb(),
                    gc_collections=_gc_stats(),
                    extra={"cancelled": False},
                ))


# ---------------------------------------------------------------------------
# Main monitor
# ---------------------------------------------------------------------------

class AsyncSentry:
    """
    Drop-in event-loop blocker detector that logs to stdout/stderr only.

    Parameters
    ----------
    threshold : float
        Seconds of event-loop silence that constitute a "block" (default 0.1).
    async_threshold : float | None
        Seconds before a slow async task is flagged. Defaults to `threshold`.
    poll_interval : float
        How often (seconds) the watchdog thread checks the loop (default 0.05).
    detect_async_bottlenecks : bool
        Whether to track slow async tasks via the task factory (default True).
    capture_args : bool
        Whether to include local variable snapshots in stack frames (default False).
    log_gc : bool
        Whether to log GC collection events (default False).
    max_stack_frames : int
        Maximum number of frames captured per event (default 20).
    """

    def __init__(
        self,
        threshold: float = 0.1,
        async_threshold: Optional[float] = None,
        poll_interval: float = 0.05,
        detect_async_bottlenecks: bool = True,
        capture_args: bool = False,
        log_gc: bool = False,
        max_stack_frames: int = 20,
    ) -> None:
        self.threshold = threshold
        self.async_threshold = async_threshold if async_threshold is not None else threshold
        self.poll_interval = poll_interval
        self.detect_async_bottlenecks = detect_async_bottlenecks
        self.capture_args = capture_args
        self.log_gc = log_gc
        self.max_stack_frames = max_stack_frames

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._task_tracker: Optional[_TaskTracker] = None
        self._gc_callbacks_registered = False

        # Shared heartbeat between loop and watchdog thread
        self._last_tick = time.monotonic()
        self._tick_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Start the sentry. Call from within a running async context."""
        self._loop = loop or asyncio.get_event_loop()
        self._stop_event.clear()

        # Schedule a recurring heartbeat coroutine on the event loop
        self._loop.call_soon(self._schedule_heartbeat)

        if self.detect_async_bottlenecks:
            self._task_tracker = _TaskTracker(self.async_threshold)
            self._task_tracker.install(self._loop)

        if self.log_gc:
            self._register_gc_callbacks()

        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="asyncsentry-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

        _emit_lifecycle("start", {
            "threshold_s": self.threshold,
            "async_threshold_s": self.async_threshold,
            "poll_interval_s": self.poll_interval,
            "detect_async_bottlenecks": self.detect_async_bottlenecks,
            "capture_args": self.capture_args,
        })

    def stop(self) -> None:
        """Stop the sentry and clean up."""
        self._stop_event.set()
        if self._watchdog_thread:
            self._watchdog_thread.join(timeout=2.0)
            self._watchdog_thread = None

        if self._task_tracker and self._loop:
            self._task_tracker.uninstall(self._loop)
            self._task_tracker = None

        if self._gc_callbacks_registered:
            self._unregister_gc_callbacks()

        _emit_lifecycle("stop")

    # ------------------------------------------------------------------
    # Slow-async convenience wrapper
    # ------------------------------------------------------------------

    async def watch(self, coro: Coroutine, name: Optional[str] = None) -> Any:
        """
        Time a directly-awaited coroutine and emit a slow_async event if it
        exceeds `async_threshold`.

        Use when you await a coroutine directly (not via create_task) and want
        it covered by slow-async detection::

            result = await sentry.watch(my_coro(), name="my_coro")
        """
        if self._task_tracker is None:
            return await coro
        return await self._task_tracker.watch(coro, name=name)

    def monitor(self, fn: Callable[..., Coroutine]) -> Callable:
        """
        Decorator that automatically wraps every call of an async function
        with `watch`, so you never have to change call sites::

            @sentry.monitor
            async def slow_async_task():
                await asyncio.sleep(2.5)

            await slow_async_task()   # automatically timed
        """
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            return await self.watch(fn(*args, **kwargs), name=fn.__qualname__)
        return wrapper

    # ------------------------------------------------------------------
    # Context-manager interface (sync)
    # ------------------------------------------------------------------

    def __enter__(self) -> "AsyncSentry":
        self.start()
        return self

    def __exit__(self, *args) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Async context-manager interface
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "AsyncSentry":
        self.start()
        return self

    async def __aexit__(self, *args) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Heartbeat (runs on the event loop thread)
    # ------------------------------------------------------------------

    def _schedule_heartbeat(self) -> None:
        """Update the tick timestamp from within the event loop."""
        with self._tick_lock:
            self._last_tick = time.monotonic()
        if not self._stop_event.is_set() and self._loop and self._loop.is_running():
            self._loop.call_later(self.poll_interval / 2, self._schedule_heartbeat)

    # ------------------------------------------------------------------
    # Watchdog loop (runs in a background thread)
    # ------------------------------------------------------------------

    def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(self.poll_interval)
            if self._stop_event.is_set():
                break

            with self._tick_lock:
                last = self._last_tick
            elapsed = time.monotonic() - last

            if elapsed >= self.threshold:
                self._report_block(elapsed)

    def _report_block(self, elapsed: float) -> None:
        """Capture stack frames from all threads and emit a block event."""
        import sys

        all_frames = sys._current_frames()
        loop_thread_id = None

        # Identify the loop thread via CPython internal
        if self._loop and hasattr(self._loop, "_thread_id"):
            loop_thread_id = self._loop._thread_id

        # Fall back: find a thread running asyncio code
        if loop_thread_id is None:
            for tid, frame in all_frames.items():
                module = frame.f_globals.get("__name__", "")
                if "asyncio" in module:
                    loop_thread_id = tid
                    break

        target_frame = None
        if loop_thread_id and loop_thread_id in all_frames:
            target_frame = all_frames[loop_thread_id]
        elif all_frames:
            watchdog_id = threading.get_ident()
            for tid, frame in all_frames.items():
                if tid != watchdog_id:
                    target_frame = frame
                    break

        frames: List[Dict[str, Any]] = []
        if target_frame:
            frames = _extract_frames(target_frame, self.max_stack_frames)
            if self.capture_args:
                for f_info, raw_frame in zip(frames, self._iter_frames(target_frame)):
                    try:
                        f_info["locals"] = {
                            k: repr(v)[:200]
                            for k, v in raw_frame.f_locals.items()
                        }
                    except Exception:
                        pass

        culprit = _find_culprit(frames)

        task_name: Optional[str] = None
        try:
            if self._loop:
                current = asyncio.current_task() if self._loop.is_running() else None
                if current:
                    task_name = current.get_name()
        except RuntimeError:
            pass

        event = BlockEvent(
            event_type="block",
            timestamp=time.time(),
            duration=elapsed,
            thread_id=loop_thread_id or 0,
            task_name=task_name,
            culprit=culprit,
            stack_frames=frames,
            cpu_percent=_cpu_percent(),
            memory_mb=_memory_mb(),
            gc_collections=_gc_stats(),
        )
        _emit(event)

    @staticmethod
    def _iter_frames(frame):
        current = frame
        while current is not None:
            yield current
            current = current.f_back

    # ------------------------------------------------------------------
    # GC callback support
    # ------------------------------------------------------------------

    def _gc_callback(self, phase: str, info: Dict[str, Any]) -> None:
        if phase == "stop":
            _emit(BlockEvent(
                event_type="gc",
                timestamp=time.time(),
                duration=0.0,
                thread_id=threading.get_ident(),
                task_name=None,
                culprit=None,
                stack_frames=[],
                cpu_percent=_cpu_percent(),
                memory_mb=_memory_mb(),
                gc_collections=_gc_stats(),
                extra={"gc_generation": info.get("generation"), "gc_collected": info.get("collected")},
            ))

    def _register_gc_callbacks(self) -> None:
        gc.callbacks.append(self._gc_callback)
        self._gc_callbacks_registered = True

    def _unregister_gc_callbacks(self) -> None:
        try:
            gc.callbacks.remove(self._gc_callback)
        except ValueError:
            pass
        self._gc_callbacks_registered = False


# ---------------------------------------------------------------------------
# FastAPI / lifespan helper
# ---------------------------------------------------------------------------

@asynccontextmanager
async def asyncsentry_lifespan(**kwargs):
    """
    Async context manager for use inside FastAPI lifespan functions.

    Usage::

        from asyncsentry import asyncsentry_lifespan

        @asynccontextmanager
        async def lifespan(app):
            async with asyncsentry_lifespan(threshold=0.1, async_threshold=2.0):
                yield
    """
    sentry = AsyncSentry(**kwargs)
    async with sentry:
        yield sentry
