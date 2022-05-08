"""
The base implementation of the generic steps to be used in
validation/transformation.

"""
from abc import ABCMeta, abstractmethod
from textwrap import dedent
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.type_aliases import Success, Value


class AbstractStepBase(metaclass=ABCMeta):
    """
    The common base of an abstract transformation, validation or parsing step.

    Steps are a combination of logic and documentation, allowing for
    semantically valuable documentation to be generated from the code.

    """

    @property
    @abstractmethod
    def type(self) -> Literal["validation", "transformation", "parser"]:
        """The type of step implemented by the class."""

    @property
    def name(self) -> str:
        """
        The name of the step. This is used in the documentation, and
        should contain the parameters formatted as keyword arguments.

        e.g. `DateParser(date_format='iso')`

        """
        params = self.parameters
        name = type(self).__name__
        if not params:
            return name

        formatted = ", ".join(f"{k}={v!r}" for k, v in params.items())
        return f"{name}({formatted})"

    @property
    @abstractmethod
    def parameters(self) -> Dict[str, Any]:
        """The parameters passed to the step's constructor."""

    @property
    def description(self) -> str:
        """A human readable description of the step."""
        return dedent(self.__doc__ or "")

    def __repr__(self):
        return self.name


class AbstractStep(AbstractStepBase):
    """
    An abstract transformation, validation or parsing step which supports
    application to a single column.

    """

    @abstractmethod
    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        """
        Apply the step to a value. Where the step is the first in a sequence,
        the initial value is usually an optional string.

        This should return a tuple containing:
         - The new value. This can be the original value, a transformed/parsed
           value, or None.
         - A boolean indicating whether the step has succeded
         - An optional message. In event of failure, this should be an 'ERROR'
           level message.

        """

    def __call__(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        return self.apply(value)


class AbstractMultiStep(AbstractStepBase):
    """
    An abstract transformation, validation or parsing step which supports
    application to a multiple columns.

    """

    @abstractmethod
    def apply(self, *values: Value) -> Tuple[Value, Success, Optional[Message]]:
        """
        Apply the step to multiple values. This is typically done _after_
        individual column steps have been run.

        This should return a tuple containing:
         - A new value. This should be derived from all of the input values.
         - A boolean indicating whether the step has succeded
         - An optional message. In event of failure, this should be an 'ERROR'
           level message.

        """

    def __call__(self, *values: Value) -> Tuple[Value, Success, Optional[Message]]:
        return self.apply(*values)
