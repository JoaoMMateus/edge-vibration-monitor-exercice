"""Small function-call/output trace helper for the repo's runnable scripts."""

import functools
import inspect
import os
from datetime import datetime
from typing import Any, Callable


TRACE_LOG_FILE = os.environ.get("EDGE_TRACE_LOG_FILE", "edge_trace.log")

FUNCTION_SCRIPT_MAP = {
    "VibrationSensor.__init__": "edge/sensor.py",
    "VibrationSensor.step": "edge/sensor.py",
    "StreamingProcessor.process_sample": "edge/processor.py",
    "CloudSync.add_window": "edge/sync.py",
    "CloudSync.flush": "edge/sync.py",
    "CloudSync.shutdown": "edge/sync.py",
    "build_demo_pipeline": "run_sync.py",
    "main": "run_sync.py",
    "create_sensor_config": "run_processor.py",
    "run_benchmark_mode": "run_processor.py",
    "run_realtime_mode": "run_processor.py",
    "run_interactive_mode": "run_processor.py",
}


def _stamp() -> str:
    """Return a timestamp with a compact date:hour style prefix."""
    return datetime.now().strftime("%Y-%m-%d:%H:%M:%S")


def _caller_script() -> str:
    """Return a fallback script filename from the immediate caller stack."""
    frame = inspect.currentframe()
    try:
        current = frame
        while current is not None:
            current = current.f_back
            if current is None:
                break
            filename = current.f_code.co_filename
            if filename.endswith("trace.py"):
                continue
            if filename.startswith("<"):
                continue
            return os.path.basename(filename)
    except Exception:
        return "unknown_script"
    finally:
        del frame

    return "unknown_script"


def _script_for(function_name: str) -> str:
    """Prefer edge module script names for edge-owned functions, else use run scripts."""
    if function_name in FUNCTION_SCRIPT_MAP:
        return FUNCTION_SCRIPT_MAP[function_name]
    return _caller_script()


def log_line(message: str) -> None:
    """Append one trace line to the configured log file."""
    with open(TRACE_LOG_FILE, "a", encoding="utf-8") as handle:
        handle.write(message.rstrip() + "\n")


def log_call(function_name: str) -> None:
    """Record that a function has been called, including the source script."""
    script = _script_for(function_name)
    log_line(f"[LOG] {_stamp()} script={script} function {function_name} called")


def log_output(function_name: str, output: Any) -> None:
    """Record the normalized function return payload, including the source script."""
    script = _script_for(function_name)
    safe_output = repr(output)
    log_line(f"[INFO] {_stamp()} script={script} function {function_name} output {safe_output}")


def trace_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that records calls and function return values to the log file."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        log_call(func.__name__)
        try:
            result = func(*args, **kwargs)
            log_output(func.__name__, result)
            return result
        except Exception as exc:
            log_output(func.__name__, f"ERROR: {type(exc).__name__}: {exc}")
            raise

    return wrapper
