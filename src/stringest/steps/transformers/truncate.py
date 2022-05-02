"""Tranformation step to truncate a string."""
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class Truncate(AbstractStep):
    """
    A basic transformation step, truncating a string.

    Arguments:
     - `length` the number of characters to truncate the string at

    This is treated slightly differently from standard builtin
    transformations as it's such a common transformation (and should
    not raise an error for None values).

    """

    def __init__(self, *, length: int):
        self._length = length

    @property
    def parameters(self) -> Dict[str, Any]:
        return {"length": self._length}

    @property
    def type(self) -> Literal["transformation"]:
        return "transformation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            return None, True, None

        if not isinstance(value, str):
            message = Message(
                status="ERROR", content=f"Cannot truncate non-string, got {type(value)}"
            )
            return None, False, message

        return value[: self._length], True, None
