# asyncsentry

**Async Event Loop Blocker Detector — logs-only, Kubernetes-native**

Detects blocking calls and slow async tasks in `asyncio` applications. Unlike similar tools, `asyncsentry` never writes to disk; all events are emitted as **structured JSON log lines** via Python's `logging` module so they integrate naturally with any log aggregator (stdout/stderr, Loki, CloudWatch, Datadog, etc.).

## Installation

```bash
pip install asyncsentry
# or
uv add asyncsentry
```

## Quick start

```python
import asyncio
from asyncsentry import AsyncSentry

async def main():
    async with AsyncSentry(threshold=0.1):
        # ... your application code
        await asyncio.sleep(10)

asyncio.run(main())
```

## Configuration

```python
sentry = AsyncSentry(
    threshold=0.1,                  # seconds before a block is flagged
    async_threshold=1.0,            # seconds before a slow async task is flagged
    poll_interval=0.05,             # how often the watchdog checks (seconds)
    detect_async_bottlenecks=True,  # track slow async tasks via task factory
    capture_args=False,             # include local variables in stack frames
    log_gc=False,                   # log GC collection events
    max_stack_frames=20,            # max frames captured per event
)
```

| Parameter                  | Default           | Description |
|----------------------------|-------------------|-------------|
| `threshold`                | `0.1`             | Seconds of loop silence that constitute a block |
| `async_threshold`          | Same as threshold | Slow async task cutoff |
| `poll_interval`            | `0.05`            | Watchdog polling interval |
| `detect_async_bottlenecks` | `True`            | Track slow coroutines via task factory |
| `capture_args`             | `False`           | Capture local variables in stack frames |
| `log_gc`                   | `False`           | Emit events on GC collection |
| `max_stack_frames`         | `20`              | Cap on captured frames |

## Usage patterns

### Plain asyncio

```python
import asyncio, logging
from asyncsentry import AsyncSentry

logging.basicConfig(level=logging.INFO)  # logs go to stdout

async def main():
    async with AsyncSentry(threshold=0.1, detect_async_bottlenecks=True):
        await your_app()

asyncio.run(main())
```

### FastAPI / Uvicorn

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from asyncsentry import asyncsentry_lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with asyncsentry_lifespan(
        threshold=0.1,
        async_threshold=2.0,
        capture_args=True,
        detect_async_bottlenecks=True,
    ):
        yield

app = FastAPI(lifespan=lifespan)
```

### Manual start / stop

```python
sentry = AsyncSentry(threshold=0.05)
sentry.start()
# ... run your app ...
sentry.stop()
```

## Log output

Every event is a single JSON line emitted at `WARNING` (blocks / slow tasks) or `INFO` (lifecycle / GC) level through the `asyncsentry` logger.

**Block event example:**
```json
{
  "asyncsentry": true,
  "event_type": "block",
  "timestamp": 1718000000.123,
  "duration_s": 0.312,
  "task_name": "Task-3",
  "culprit": "/app/routes/users.py:42 in get_users",
  "cpu_percent": 94.2,
  "memory_mb": 128.4,
  "gc_collections": {"gen0": 12, "gen1": 3, "gen2": 0},
  "stack_frames": [
    {
      "filename": "/app/routes/users.py",
      "lineno": 42,
      "function": "get_users",
      "source": "rows = db.execute(query).fetchall()",
      "is_app_frame": true
    }
  ]
}
```

**Slow async task event example:**
```json
{
  "asyncsentry": true,
  "event_type": "slow_async",
  "timestamp": 1718000001.456,
  "duration_s": 2.81,
  "task_name": "Task-7",
  "culprit": null,
  "cpu_percent": 12.1,
  "memory_mb": 130.0,
  "gc_collections": {"gen0": 13, "gen1": 3, "gen2": 0},
  "stack_frames": [],
  "cancelled": false
}
```

## What is detected

| Pattern | Type | Example |
|---------|------|---------|
| Blocking sleep | `block` | `time.sleep()` in async context |
| Sync HTTP | `block` | `requests.get()` instead of aiohttp/httpx |
| Sync DB calls | `block` | PyMongo / sqlite3 sync ops |
| CPU-bound loops | `block` | Tight loops without yielding |
| Slow coroutines | `slow_async` | Tasks exceeding `async_threshold` |
| GC collections | `gc` | (optional, `log_gc=True`) |

## Kubernetes / container setup

Configure `logging` to emit JSON to stdout and you're done — no volumes, no sidecars, no file rotation:

```python
import logging, json, sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(base)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logging.getLogger("asyncsentry").addHandler(handler)
logging.getLogger("asyncsentry").setLevel(logging.INFO)
```

Then ship logs with your existing stack (Fluentd, Promtail, Fluent Bit, etc.).

## FastAPI middleware example

```python
@app.middleware("http")
async def request_timing_middleware(request, call_next):
    if _sentry is None:
        return await call_next(request)
    label = f"HTTP {request.method} {request.url.path}"
    return await _sentry.watch(call_next(request), name=label)
```
