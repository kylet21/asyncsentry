"""
examples/demo.py
~~~~~~~~~~~~~~~~
Demonstrates asyncsentry in a plain asyncio app that deliberately triggers
blocking and slow-async patterns.

Run:
    python examples/demo.py
"""

import asyncio
import json
import logging
import sys
import time

# ── Logging setup ────────────────────────────────────────────────────────────

class _PrettyJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            payload = json.loads(record.getMessage())
            et = payload.get("event_type", "?")
            dur = payload.get("duration_s", 0)
            culprit = payload.get("culprit") or payload.get("task_name") or "-"
            cpu = payload.get("cpu_percent", 0)
            mem = payload.get("memory_mb", 0)
            prefix = {
                "block":      "🔴 BLOCK     ",
                "slow_async": "🟠 SLOW_ASYNC",
                "gc":         "🔵 GC        ",
                "start":      "🟢 START     ",
                "stop":       "⚫ STOP      ",
            }.get(et, "❓ UNKNOWN   ")
            return (
                f"{prefix} | dur={dur:.3f}s | culprit={culprit} "
                f"| cpu={cpu:.1f}% | mem={mem:.1f}MB"
            )
        except Exception:
            return record.getMessage()

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(_PrettyJsonFormatter())
logging.getLogger("asyncsentry").addHandler(handler)
logging.getLogger("asyncsentry").setLevel(logging.DEBUG)

from monitor import AsyncSentry  # noqa: E402


# ── Coroutines ────────────────────────────────────────────────────────────────

async def clean_task():
    """A well-behaved async function."""
    await asyncio.sleep(0.05)


async def blocking_task():
    """Simulates a blocking synchronous call inside an async context."""
    time.sleep(0.5)  # blocks the event loop for 500 ms


async def slow_async_task():
    """A coroutine that is genuinely slow but non-blocking."""
    await asyncio.sleep(1.5)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    async with AsyncSentry(
        threshold=0.1,
        async_threshold=1.0,
        detect_async_bottlenecks=True,
        capture_args=True,
        log_gc=False,
    ) as sentry:

        # 1. Clean coroutine — no events expected
        print("\n[demo] Running clean task …")
        await clean_task()

        # 2. Blocking call — triggers BLOCK events from the watchdog thread
        print("[demo] Running blocking task (expect BLOCK events) …")
        await blocking_task()

        # 3a. Slow coroutine via create_task — task factory hook fires on completion
        print("[demo] Running slow task via create_task (expect SLOW_ASYNC) …")
        task = asyncio.create_task(slow_async_task(), name="slow-via-create_task")
        await task

        # 3b. Slow coroutine awaited directly — use sentry.watch() to time it
        print("[demo] Running slow task via sentry.watch() (expect SLOW_ASYNC) …")
        await sentry.watch(slow_async_task(), name="slow-via-watch")

        # 3c. Use the @sentry.monitor decorator so call sites need no changes
        print("[demo] Running slow task via @sentry.monitor (expect SLOW_ASYNC) …")

        @sentry.monitor
        async def slow_decorated():
            await asyncio.sleep(1.5)

        await slow_decorated()

        print("\n[demo] Done.\n")


if __name__ == "__main__":
    asyncio.run(main())
