"""Transformation steps which modify strings."""
import re
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class RegexReplace(AbstractStep):
    """
    A basic string transformation step, replacing characters in a string.

    Arguments:
     - `pattern`: the pattern to replace in the current string
     - `replacement`: the string to replace the pattern with

    """

    def __init__(self, *, pattern: str, replacement: str):
        self._pattern = pattern
        self._compiled = re.compile(pattern)
        self._replacement = replacement

    @property
    def parameters(self) -> Dict[str, Any]:
        return {"pattern": self._pattern, "replacement": self._replacement}

    @property
    def type(self) -> Literal["transformation"]:
        return "transformation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            message = Message(
                status="ERROR", content="Cannot replace values in null string"
            )
            return None, False, message

        if not isinstance(value, str):
            message = Message(
                status="ERROR",
                content=f"Cannot replace values in non-string, got {type(value)}",
            )
            return None, False, message

        return self._compiled.sub(self._replacement, value), True, None


class Truncate(AbstractStep):
    """
    A basic string transformation step, truncating a string.

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
