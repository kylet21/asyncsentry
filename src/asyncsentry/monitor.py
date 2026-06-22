"""
asyncsentry.monitor
~~~~~~~~~~~~~~~~~~~
Core blocking-detection monitor. Runs a background thread that polls the event
loop at a fixed interval; if the loop has not responded within `threshold`
seconds it is considered blocked.

All events are emitted as structured JSON lines to the Python `logging`
infrastructure so they are captured by any log aggregator (stdout in a
container, Loki, CloudWatch, etc.).

No files are written.
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
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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
    culprit: Optional[str]
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


def _extract_task_frames(task: asyncio.Task, max_frames: int = 20) -> List[Dict[str, Any]]:
    """Extract stack frames directly from a live asyncio Task's coroutine stack."""
    try:
        # get_stack() returns frames from outermost to innermost; we reverse to
        # match the innermost-first convention used elsewhere in this module.
        raw_frames = task.get_stack(limit=max_frames)
        result = []
        for f in reversed(raw_frames):
            co = f.f_code
            filename = co.co_filename
            lineno = f.f_lineno
            source_line = linecache.getline(filename, lineno).strip()
            result.append({
                "filename": filename,
                "lineno": lineno,
                "function": co.co_name,
                "source": source_line,
                "is_app_frame": _is_app_frame(f),
            })
        return result
    except Exception:
        return []


def _find_culprit(frames: List[Dict[str, Any]]) -> Optional[str]:
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
# Task registry
# ---------------------------------------------------------------------------

class _TaskRegistry:
    """
    Tracks live asyncio Tasks by hooking the task factory.

    The watchdog thread reads _start_times to find tasks that have been
    running longer than async_threshold and samples their stacks while they
    are still alive.  The done-callback only handles cleanup.
    """

    def __init__(self) -> None:
        # task_id -> (start_monotonic, task_ref)
        self._tasks: Dict[int, tuple[float, asyncio.Task]] = {}
        self._lock = threading.Lock()

    def install(self, loop: asyncio.AbstractEventLoop) -> None:
        original_factory = loop.get_task_factory()

        def factory(loop, coro, **kwargs):
            if original_factory is None:
                task = asyncio.Task(coro, loop=loop, **kwargs)
            else:
                task = original_factory(loop, coro, **kwargs)
            self._register(task)
            return task

        loop.set_task_factory(factory)

    def uninstall(self, loop: asyncio.AbstractEventLoop) -> None:
        loop.set_task_factory(None)
        with self._lock:
            self._tasks.clear()

    def _register(self, task: asyncio.Task) -> None:
        task_id = id(task)
        with self._lock:
            self._tasks[task_id] = (time.monotonic(), task)

        def _done(t: asyncio.Task) -> None:
            with self._lock:
                self._tasks.pop(id(t), None)

        task.add_done_callback(_done)

    def snapshot(self) -> List[tuple[float, asyncio.Task]]:
        """Return a snapshot of (start_time, task) for all live tasks."""
        with self._lock:
            return list(self._tasks.values())


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
        Whether to include local variable snapshots in block stack frames (default False).
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
        self._registry: Optional[_TaskRegistry] = None
        self._gc_callbacks_registered = False

        self._last_tick = time.monotonic()
        self._tick_lock = threading.Lock()

        # Tracks which tasks have already had a slow_async event emitted so we
        # don't spam the same task on every watchdog poll.
        self._alerted_tasks: set[int] = set()
        self._alerted_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._loop = loop or asyncio.get_event_loop()
        self._stop_event.clear()

        self._loop.call_soon(self._schedule_heartbeat)

        if self.detect_async_bottlenecks:
            self._registry = _TaskRegistry()
            self._registry.install(self._loop)

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
        self._stop_event.set()
        if self._watchdog_thread:
            self._watchdog_thread.join(timeout=2.0)
            self._watchdog_thread = None

        if self._registry and self._loop:
            self._registry.uninstall(self._loop)
            self._registry = None

        if self._gc_callbacks_registered:
            self._unregister_gc_callbacks()

        _emit_lifecycle("stop")

    # ------------------------------------------------------------------
    # sentry.watch() — time a directly-awaited coroutine
    # ------------------------------------------------------------------

    async def watch(self, coro: Coroutine, name: Optional[str] = None) -> Any:
        """
        Time a directly-awaited coroutine and emit a slow_async event with a
        live stack if it exceeds `async_threshold`.

            result = await sentry.watch(my_coro(), name="my_coro")
        """
        label = name or getattr(coro, "__qualname__", repr(coro))
        start = time.monotonic()
        try:
            return await coro
        finally:
            elapsed = time.monotonic() - start
            if elapsed >= self.async_threshold:
                # Stack is gone by this point (coroutine finished), so we
                # emit without frames — the label/name is the identifier.
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
                    extra={"cancelled": False, "note": "use create_task for live stack capture"},
                ))

    # ------------------------------------------------------------------
    # track_request() — for middleware; wraps handler in a real Task
    # ------------------------------------------------------------------

    async def track_request(self, coro: Coroutine, name: str) -> Any:
        """
        Run `coro` as a named asyncio Task so the watchdog can sample its
        stack mid-flight via task.get_stack().  Use this in HTTP middleware
        instead of watch() so slow requests produce stack frames.

            response = await sentry.track_request(call_next(request), name=label)
        """
        task = asyncio.ensure_future(coro)
        task.set_name(name)
        return await task

    # ------------------------------------------------------------------
    # @sentry.monitor — decorator for zero call-site changes
    # ------------------------------------------------------------------

    def monitor(self, fn: Callable[..., Coroutine]) -> Callable:
        """
        Decorator that wraps every call of an async function with watch().

            @sentry.monitor
            async def slow_task():
                ...

            await slow_task()   # automatically timed
        """
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            return await self.watch(fn(*args, **kwargs), name=fn.__qualname__)
        return wrapper

    # ------------------------------------------------------------------
    # Context managers
    # ------------------------------------------------------------------

    def __enter__(self) -> "AsyncSentry":
        self.start()
        return self

    def __exit__(self, *args) -> None:
        self.stop()

    async def __aenter__(self) -> "AsyncSentry":
        self.start()
        return self

    async def __aexit__(self, *args) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _schedule_heartbeat(self) -> None:
        with self._tick_lock:
            self._last_tick = time.monotonic()
        if not self._stop_event.is_set() and self._loop and self._loop.is_running():
            self._loop.call_later(self.poll_interval / 2, self._schedule_heartbeat)

    # ------------------------------------------------------------------
    # Watchdog loop
    # ------------------------------------------------------------------

    def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(self.poll_interval)
            if self._stop_event.is_set():
                break

            now = time.monotonic()

            # 1. Check for event-loop blocks
            with self._tick_lock:
                last = self._last_tick
            elapsed = now - last
            if elapsed >= self.threshold:
                self._report_block(elapsed)

            # 2. Check for slow async tasks (mid-flight, stack still live)
            if self._registry is not None:
                self._check_slow_tasks(now)

    def _check_slow_tasks(self, now: float) -> None:
        """
        Inspect all live tasks. For any that have been running longer than
        async_threshold and haven't been reported yet, capture their stack
        via task.get_stack() (which works on live coroutines) and emit.
        """
        for start_time, task in self._registry.snapshot():
            task_id = id(task)
            elapsed = now - start_time

            if elapsed < self.async_threshold:
                continue

            # Only emit once per task, not on every poll tick
            with self._alerted_lock:
                if task_id in self._alerted_tasks:
                    continue
                self._alerted_tasks.add(task_id)

            # Clean up the alert record when the task finishes
            def _clear_alert(t: asyncio.Task, tid: int = task_id) -> None:
                with self._alerted_lock:
                    self._alerted_tasks.discard(tid)

            try:
                task.add_done_callback(_clear_alert)
            except Exception:
                pass

            task_name = task.get_name() if hasattr(task, "get_name") else str(task)

            # get_stack() returns live frames while the coroutine is suspended
            frames = _extract_task_frames(task, self.max_stack_frames)
            culprit = _find_culprit(frames)

            _emit(BlockEvent(
                event_type="slow_async",
                timestamp=time.time(),
                duration=elapsed,
                thread_id=threading.get_ident(),
                task_name=task_name,
                culprit=culprit,
                stack_frames=frames,
                cpu_percent=_cpu_percent(),
                memory_mb=_memory_mb(),
                gc_collections=_gc_stats(),
                extra={"cancelled": task.cancelled() if task.done() else False},
            ))

    def _report_block(self, elapsed: float) -> None:
        import sys

        all_frames = sys._current_frames()
        loop_thread_id = None

        if self._loop and hasattr(self._loop, "_thread_id"):
            loop_thread_id = self._loop._thread_id

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

        _emit(BlockEvent(
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
        ))

    @staticmethod
    def _iter_frames(frame):
        current = frame
        while current is not None:
            yield current
            current = current.f_back

    # ------------------------------------------------------------------
    # GC callbacks
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

        @asynccontextmanager
        async def lifespan(app):
            async with asyncsentry_lifespan(threshold=0.1, async_threshold=2.0):
                yield
    """
    sentry = AsyncSentry(**kwargs)
    async with sentry:
        yield sentry
