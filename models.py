from enum import Enum, auto, unique
from typing import Dict

from pydantic import BaseModel, ConfigDict


class RecordModel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(**data)


@unique
class JobStatus(str, Enum):

    def _generate_next_value_(name, start, count, last_values):
        return name.capitalize()

    INITIALIZED = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    SCHEDULED = auto()
    IDLE = auto()
    PERMANENTLY_FAILED = "Permanently Failed"

    def __repr__(self):
        return f'"{self.name}"'

    def __json__(self):
        return self.name

    def is_terminal(self) -> bool:
        """Return True if status is a finished state."""
        return self in {JobStatus.SUCCESS, JobStatus.FAILED}

    def is_failed(self) -> bool:
        """Return True if status is a finished state."""
        return self in {JobStatus.PERMANENTLY_FAILED, JobStatus.FAILED}

    @classmethod
    def from_str(cls, value: str) -> "JobStatus":
        """Convert from a string to a JobStatus, case-insensitive."""
        value = value.strip().upper()
        try:
            return cls[value]
        except KeyError:
            raise ValueError(f"Unknown job status: {value}")


@unique
class WSTopic(str, Enum):

    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    ALL_JOBS = auto()

    UNKNOWN_TOPIC = auto()

    @classmethod
    def from_str(cls, value: str) -> "WSTopic":
        """Convert from a string to a WSTopic, case-insensitive."""
        try:
            return cls[value.strip().upper()]
        except KeyError:
            return cls[WSTopic.UNKNOWN_TOPIC.upper()]


@unique
class WSTopicAction(str, Enum):

    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    SUBSCRIBE = auto()
    UNSUBSCRIBE = auto()

    UNKNOWN_ACTION = auto()

    @classmethod
    def from_str(cls, value: str) -> "WSTopicAction":
        """Convert from a string to a WSTopicAction, case-insensitive."""
        try:
            return cls[value.strip().upper()]
        except KeyError:
            return cls[WSTopicAction.UNKNOWN_ACTION.upper()]
