"""Type aliases used in a few places."""
from typing import Any, Callable, Iterable, Mapping, TypeVar

Success = bool
"""A boolean value indicating success of the step."""
Value = Any
"""
A value passed to (and returned by) the step.

For the first step in the sequence, this is usually a nullable string.

"""

Row = Mapping[str, Value]
"""An inbound or outbound row."""
RowIndex = int
"""An inbound row index."""

T = TypeVar("T")
"""A type within an iterable."""
R = TypeVar("R")
"""The type returnd by applying a `MappedFunc` to an iterable of type `T`."""
FuncToMap = Callable[[T], R]
"""A function to be mapped to an iterable of type `T`."""
MapFunc = Callable[[FuncToMap, Iterable[T]], Iterable[R]]
"""The type of a map function (e.g. `builtins.map`, `multiprocessing.Pool.map`)."""
