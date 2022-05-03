"""
Transformation steps built on top of Python's builtins.

"""
import builtins
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class BuiltinTransform(AbstractStep):
    """
    A basic transformation step, returning the field after applying a builtin
    transformation.

    Arguments:
     - `type_name`: the type name to be pulled from
       [`builtins`](https://docs.python.org/3/library/functions.html)
     - `method_name`: the name of the method from the builtin type
     - `args`: a list of positional arguments to be passed to the method after the value
     - `kwargs`: a list of keyword arguments to be passed to the method

    """

    def __init__(
        self,
        *,
        type_name: str,
        method_name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        # TODO: Allowlist for types/methods. Otherwise, this could be dangerous.
        self._type_name = type_name
        self._type: Type = getattr(builtins, type_name)
        self._method_name = method_name
        self._method: Callable[[Any], Any] = getattr(self._type, method_name)

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
            "method_name": self._method_name,
            "args": self._args.copy(),
            "kwargs": self._kwargs.copy(),
        }

    @property
    def type(self) -> Literal["transformation"]:
        return "transformation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            message = Message(
                status="ERROR", content="Cannot use builtin methods on null value"
            )
            return None, False, message

        if not isinstance(value, self._type):
            message = Message(
                status="ERROR",
                content=f"Cannot use {self._type_name} values on {type(value)}",
            )
            return None, False, message

        transformed = self._method(value, *self._args, *self._kwargs)
        return transformed, True, None
