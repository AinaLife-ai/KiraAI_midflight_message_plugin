"""Stub of ``core.chat.message_utils``.

``KiraMessageBatchEvent`` keeps the two behaviours midflight relies on:
``event_id`` is unique per instance and ``stop()`` is one-way.
"""
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class KiraMessageEvent:
    message: object = None
    session: object = None
    adapter: object = None
    message_types: list = field(default_factory=list)


@dataclass
class KiraMessageBatchEvent:
    message_types: list = field(default_factory=list)
    timestamp: int = 0
    event_id: str = None
    adapter: object = None
    session: object = None
    messages: list = field(default_factory=list)
    extra: Optional[dict] = None
    model_group: list = field(default_factory=list)
    _is_stopped: bool = False

    def __post_init__(self):
        self.event_id = uuid.uuid4().hex

    def is_group_message(self):
        return False

    @property
    def sid(self):
        return self.session.sid

    @property
    def is_stopped(self):
        return self._is_stopped

    def stop(self):
        self._is_stopped = True
