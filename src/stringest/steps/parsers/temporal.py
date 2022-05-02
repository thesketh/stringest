"""
Temporal parser steps built on top of Python's `datetime` module.

"""
import datetime as dt
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class DatetimeParser(AbstractStep):
    """
    A datetime parsing step, returning the field as a Python `datetime.datetime`.

    Parser datetime formats can be specified using
    [Python date formatting codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)

    Arguments:
     - `format_string`: the datetime format, as a string containing Python datetime
       formatting codes.

    """

    def __init__(self, *, format_string="%Y-%m-%dT%H:%M:%S%z"):
        self._format_string = format_string

    @property
    def parameters(self) -> Dict[str, Any]:
        return {"format_string": self._format_string}

    @property
    def type(self) -> Literal["parser"]:
        return "parser"

    @property
    def description(self) -> str:
        string = (
            f"This parser tries to parse temporal data using `{self._format_string}`\n"
        )
        return super().description + string

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None:
            message = Message(
                status="ERROR", content="Null value cannot be parsed as temporal data"
            )
            return None, False, message

        try:
            datetime = dt.datetime.strptime(value, self._format_string)
        except ValueError:
            message = Message(
                status="ERROR",
                content=(
                    f"Temporal data does not match format {self._format_string!r}, "
                    + "or resulting date/datetime is invalid"
                ),
            )
            return None, False, message

        return datetime, True, None


class DateParser(DatetimeParser):
    """
    A date parsing step, returning the field as a Python `datetime.date`.

    Parser date formats can be specified using
    [Python date formatting codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)

    Arguments:
     - `format_string`: the date format, as a string containing Python date
       formatting codes.

    """

    def __init__(self, *, format_string="%Y-%m-%d"):
        super().__init__(format_string=format_string)

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        datetime, success, message = super().apply(value)
        date = None if datetime is None else datetime.date()
        return date, success, message
