"""Representations of inbound schemas."""
from typing import List, Optional, Tuple, Union

# import pyarrow as pa  # type: ignore

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class Field:
    """
    A field within an inbound schema.

    Arguments:
     - `name`: the inbound name of the field
     - `steps`: an (optional) `AbstractStep` or list of `AbstractStep`s to apply to
       the field, in sequence. For free-text fields, this may be omitted.
       The first step must accept an optional string, each in sequence must accept
       the return value of the previous step.
     - `parquet_type`: a string indicating the type of the field in the Parquet file.
     - `outbound_name`: the (optional) name of the field at output time. If not
       provided, `name` will be used.
     - `mandatory`: A flag indicating whether a value must be provided for the field
        on input (default: `False`)
     - `nullable`: A flag indicating whether the value is allowed to be null on
       output (default: `True`)
     - `fail_on_error`: A flag indicating whether an error in the process should
       result on an ingestion failure. If `False`, the field will be nulled instead.
       (default: `True`)

    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: str,
        steps: Union[None, AbstractStep, List[AbstractStep]] = None,
        parquet_type: str = "string",
        outbound_name: Optional[str] = None,
        mandatory: bool = False,
        nullable: bool = True,
        fail_on_error: bool = False,
    ):
        self._name = name
        self._parquet_type = parquet_type
        self._outbound_name = outbound_name or self._name
        self._mandatory = mandatory
        self._nullable = nullable
        self._fail_on_error = fail_on_error

        self._steps: List[AbstractStep]
        if steps is None:
            self._steps = []
        elif isinstance(steps, AbstractStep):
            self._steps = [steps]
        elif isinstance(steps, list):
            if len(steps) == 0:
                self._steps = []

            if not all(map(lambda step: isinstance(step, AbstractStep), steps)):
                raise TypeError("`steps` contains non-step elements")
            self._steps = steps.copy()
        else:
            raise TypeError("`steps` must be a step, a list of steps, or `None`")

    @property
    def name(self) -> str:
        """The name of the inbound field."""

    @property
    def steps(self) -> List[AbstractStep]:
        """The sequence of steps to be applied to the field."""
        return self._steps.copy()

    @property
    def parquet_type(self) -> str:
        """
        The type of the field in the outbound parquet file.

        This will be fetched from the type registry at output time.
        """
        return self._parquet_type

    @property
    def outbound_name(self) -> str:
        """The name of the field in the output."""
        return self._outbound_name

    @property
    def mandatory(self) -> bool:
        """A flag indicating whether the field is mandatory on input."""
        return self._mandatory

    @property
    def nullable(self) -> bool:
        """A flag indicating whether the field is nullable on output."""
        return self._nullable

    @property
    def fail_on_error(self) -> bool:
        """A flag indicating whether errors in a step should result in failure."""
        return self._fail_on_error

    def ingest(  # pylint: disable=too-many-branches
        self, value: Optional[str]
    ) -> Tuple[Value, Success, List[Message]]:
        """Validate and parse an inbound value."""
        if value is not None:
            # Trimming a value's whitespace, replacing with explicit None if the field
            # is empty.
            trimmed = value.strip()
            value = trimmed if trimmed else None

        message: Optional[Message]

        if value is None and self._mandatory:
            message = Message(
                status="ERROR", content="Null value received in mandatory field"
            )
            return None, False, [message]

        messages: List[Message] = []

        for step in self._steps:
            try:
                value, success, message = step(value)
            except Exception as err:  # pylint: disable=broad-except
                value = None
                success = False
                message = Message(
                    status="INTERNAL_ERROR",
                    content=f"Unexpected error in {step.name}: {err!r}",
                )

            if message:
                messages.append(message)
            if not success:
                if not message:
                    message = Message(
                        status="ERROR", content=f"{step.name} reported failure"
                    )
                    messages.append(message)
                break

        field_success = True
        if value is None and not self._nullable:
            field_success = False

        output_messages = []
        for message in messages:
            if message.status == "INTERNAL_ERROR":
                field_success = False
            if message.status == "ERROR":
                if self._fail_on_error:
                    field_success = False
                else:
                    message = message.downgrade()
            output_messages.append(message)

        return value, field_success, output_messages


class Schema:
    """An inbound schema, consisting of a number of fields."""

    def __init__(self, *fields: Field):
        self._fields: List[Field] = list(fields)

    @property
    def fields(self) -> List[Field]:
        """A copy of the fields in the schema."""
        return self._fields.copy()

    # TODO: implement logic to render spec to markdown based on field documentation.
    # TODO: implement logic to apply fields to inbound data.
    # TODO: implement Parquet output.
