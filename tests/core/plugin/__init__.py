"""Stub of ``core.plugin`` — only what midflight/main.py imports."""
from enum import IntEnum


class Priority(IntEnum):
    """Mirrors core/plugin/plugin_handlers.py (SYS_LOW/SYS_HIGH are system-level)."""

    SYS_LOW = -100
    LOW = -50
    MEDIUM = 0
    HIGH = 50
    SYS_HIGH = 100


class _Logger:
    """Collects log lines instead of printing them (tests stay readable)."""

    def __init__(self):
        self.records = []

    def _emit(self, level, msg):
        self.records.append((level, str(msg)))

    def info(self, msg, *a, **k):
        self._emit("INFO", msg)

    def debug(self, msg, *a, **k):
        self._emit("DEBUG", msg)

    def warning(self, msg, *a, **k):
        self._emit("WARN", msg)

    def error(self, msg, *a, **k):
        self._emit("ERROR", msg)

    def exception(self, msg, *a, **k):
        self._emit("EXC", msg)

    def lines(self, level=None):
        return [m for lv, m in self.records if level is None or lv == level]


logger = _Logger()


class _On:
    """Hook decorators: verified by the tests to be no-ops (hooks are called directly)."""

    def _deco(self, *_a, **_k):
        def inner(func):
            return func
        return inner

    im_message = _deco
    message_buffered = _deco
    im_batch_message = _deco
    llm_request = _deco
    llm_response = _deco
    tool_result = _deco
    after_xml_parse = _deco
    message_sent = _deco
    step_result = _deco
    final_result = _deco
    loaded = _deco
    shutdown = _deco
    custom_event = _deco


on = _On()


def register(*_a, **_k):
    def inner(func):
        return func
    return inner


class BasePlugin:
    def __init__(self, ctx, cfg):
        self.ctx = ctx
        self.cfg = cfg
        self.plugin_cfg = cfg
