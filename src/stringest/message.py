"""
Messages to be returned to data providers.

"""
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Message:
    """A message to be passed about a specific step."""

    status: Literal["ERROR", "INFO", "WARNING", "INTERNAL_ERROR"]
    """The status of the message."""
    content: str
    """The content of the message."""

    def __post_init__(self):
        for field in ("status", "content"):
            value = getattr(self, field)
            if not isinstance(value, str):
                raise TypeError(f"Field `{field}` must be string, got {type(field)}")

        if self.status not in {"ERROR", "INFO", "WARNING", "INTERNAL_ERROR"}:
            raise ValueError(f"Invalid value for `status`: {self.status!r}")

    @property
    def is_error(self) -> bool:
        """Whether the message is an error."""
        return self.status in ("INTERNAL_ERROR", "ERROR")

    def downgrade(self) -> "Message":
        """
        Return the message with a downgraded status.

        Internal errors will not be downgraded.

        """
        if self.status == "ERROR":
            return type(self)(status="WARNING", content=self.content)
        if self.status == "WARNING":
            return type(self)(status="INFO", content=self.content)
        return self

    def upgrade(self) -> "Message":
        """
        Return the message with an upgraded status.

        Internal errors will not be upgraded.

        """
        if self.status == "INFO":
            return type(self)(status="WARNING", content=self.content)
        if self.status == "WARNING":
            return type(self)(status="ERROR", content=self.content)
        return self
