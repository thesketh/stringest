"""
Temporal parser steps built on top of Python's `datetime` module.

"""
import datetime as dt
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class TemporalParserMixin(AbstractStep):  # pylint: disable=abstract-method
    """A datetime parser mixin, which implements date parsing functionality."""

    def __init__(self, *, format_string="%Y-%m-%dT%H:%M:%S%z"):
        self._format_string = format_string

    @property
    def parameters(self) -> Dict[str, Any]:
        return {"format_string": self._format_string}

    @property
    def type(self) -> Literal["parser"]:
        return "parser"

    def _parse_datetime(
        self, value: Value
    ) -> Tuple[Optional[dt.datetime], Success, Optional[Message]]:
        """Parse a datetime using the built in format."""
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


class DatetimeParser(TemporalParserMixin):
    """
    A datetime parsing step, returning the field as a Python `datetime.datetime`.

    Parser datetime formats can be specified using
    [Python date formatting codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)

    Arguments:
     - `format_string`: the datetime format, as a string containing Python datetime
       formatting codes. Default: `'%Y-%m-%dT%H:%M:%S%z'`

    """

    def apply(
        self, value: Value
    ) -> Tuple[Optional[dt.datetime], Success, Optional[Message]]:
        return self._parse_datetime(value)


class DateParser(TemporalParserMixin):
    """
    A date parsing step, returning the field as a Python `datetime.date`.

    Parser date formats can be specified using
    [Python date formatting codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)

    Arguments:
     - `format_string`: the date format, as a string containing Python date
       formatting codes. Default: `'%Y-%m-%d'`

    """

    def __init__(self, *, format_string="%Y-%m-%d"):
        super().__init__(format_string=format_string)

    def apply(
        self, value: Value
    ) -> Tuple[Optional[dt.date], Success, Optional[Message]]:
        datetime, success, message = self._parse_datetime(value)

        date = None if datetime is None else datetime.date()
        return date, success, message
