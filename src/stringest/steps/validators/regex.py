"""
Validator steps built on top of regular expression matches.

"""
import re
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class RegexValidator(AbstractStep):
    """
    A basic validation step, validating the field using a regex pattern.

    The pattern provided will be placed in a group anchored to the start and end
    of the string.

    Arguments:
     - `pattern`: the regex pattern to match against the input string.

    """

    def __init__(self, *, pattern: str):
        self._pattern = pattern
        self._compiled = re.compile(f"^({pattern.rstrip('$').lstrip('^')})$")

    @property
    def parameters(self) -> Dict[str, Any]:
        return {"pattern": self._pattern}

    @property
    def type(self) -> Literal["validation"]:
        return "validation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            message = Message(status="ERROR", content="Null value cannot be validated")
            return None, False, message

        if not isinstance(value, str):
            message = Message(
                status="ERROR",
                content=f"Cannot use regex to validate non-string, got {type(value)}",
            )
            return None, False, message

        match = self._compiled.match(value)
        if match:
            return value, True, None

        message = Message(
            status="ERROR",
            content=f"String {value!r} does not match regex pattern {self._pattern!r}",
        )
        return None, False, message
