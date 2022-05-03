"""
Parser steps built on top of Python's builtins.

"""
import builtins
from typing import Any, Dict, List, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class BuiltinParser(AbstractStep):
    """
    A basic parsing step, returning the field after passing it to a Python builtin.

    Arguments:
     - `type_name`: the type name to be pulled from
       [`builtins`](https://docs.python.org/3/library/functions.html)
     - `args`: a list of positional arguments to be passed to the builtin's constructor
       after the value
     - `kwargs`: a list of keyword arguments to be passed to the builtin's constructor

    """

    def __init__(
        self,
        *,
        type_name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        self._type_name = type_name
        # TODO: Allowlist for builtin types - some of these could be dangerous
        self._type = getattr(builtins, type_name)

        if args is None:
            self._args = []
        else:
            self._args = args.copy()

        if kwargs is None:
            self._kwargs = {}
        else:
            self._kwargs = kwargs.copy()

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            "type_name": self._type_name,
            "args": self._args.copy(),
            "kwargs": self._kwargs.copy(),
        }

    @property
    def type(self) -> Literal["parser"]:
        return "parser"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            message = Message(
                status="ERROR", content="Null value cannot be parsed as builtin"
            )
            return None, False, message

        try:
            parsed = self._type(value, *self._args, *self._kwargs)
        except ValueError as err:
            message = Message(
                status="ERROR",
                content=(f"Unable to parse string due to incorrect format: {err!s}"),
            )
            return None, False, message

        return parsed, True, None
