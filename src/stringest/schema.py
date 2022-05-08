"""Representations of inbound schemas."""
from copy import deepcopy
from functools import partial
from multiprocessing import Pool
from typing import Any, Iterable, List, Literal, Mapping, Optional, Set, Tuple, Union

import pyarrow as pa

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, MapFunc, Row, RowIndex, Success


class Constant:  # pylint: disable=too-few-public-methods
    """
    A fixed input value for a field. This will be passed
    to the field in place of a value from the source file.

    Arguments:
     - `value`: the value of the constant.

    """

    def __init__(self, value: Any):
        self._value = value

    @property
    def value(self) -> Any:
        """The value of the constant."""
        return deepcopy(self._value)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._value})"


class Special:  # pylint: disable=too-few-public-methods
    """
    A special value inferred from the input file. This will be passed
    to the field in place of a value from the source file.

    Arguments:
     - `value_type`: This should be one of the following:
       - `file_name`: the name of the inbound file
       - `record_index`: the index (zero-based) of the record
       - `record_number`: the index (one-based) of the record

    """

    def __init__(
        self, value_type: Literal["file_name", "record_index", "record_number"]
    ):
        if value_type not in {
            "file_name",
            "record_index",
            "record_number",
        }:
            raise ValueError(
                f"Incorrect value for `value_type` ({value_type!r}). "
                + "Should be one of `{'file_name', 'record_index', 'record_number'}`"
            )
        self._value_type = value_type

    @property
    def value_type(self) -> Literal["file_name", "record_index", "record_number"]:
        """The type of special input value."""
        return self._value_type

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._value_type})"


class Field:
    """
    A field within an inbound schema.

    Arguments:
     - `name`: the inbound name of the field, a `Constant`, or a `Special`.
     - `steps`: an (optional) `AbstractStep` or list of `AbstractStep`s to
       apply to the field, in sequence. For free-text fields, this may be
       omitted. Each step may be passed an optional value, and should accept
       the return value of the previous step.
     - `parquet_type`: a PyArrow type. This defaults to a string.
     - `outbound_name`: the name of the field at output time. If `name` is not
       a string, this must be provided. If not provided and `name` is a string,
       `name` will be used.
     - `mandatory`: A flag indicating whether a value must be provided for the
       field on input (default: `False`)
     - `nullable`: A flag indicating whether the value is allowed to be null on
       output (default: `True`)
     - `fail_on_error`: A flag indicating whether an error in the process
       should result on an ingestion failure. If `False`, the field will be
       nulled instead. (Default: `False`)

    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: Union[str, Constant, Special],
        steps: Union[None, AbstractStep, List[AbstractStep]] = None,
        parquet_type: pa.DataType = pa.string(),
        outbound_name: Optional[str] = None,
        mandatory: bool = False,
        nullable: bool = True,
        fail_on_error: bool = False,
    ):
        if not isinstance(name, (str, Constant, Special)):
            raise TypeError(
                f"`name` must be `str`, `Constant` or `Special`, got {type(name)}"
            )
        self._name = name

        if not isinstance(parquet_type, pa.DataType):
            raise TypeError(
                f"`parquet_type` must be PyArrow DataType, got {type(parquet_type)}"
            )
        self._parquet_type = parquet_type

        if outbound_name:
            self._outbound_name = outbound_name
        elif isinstance(self._name, str):
            self._outbound_name = self._name
        else:
            raise ValueError(
                "If `name` is not a string, `outbound_name` must be provided"
            )

        if mandatory and nullable:
            raise ValueError("A field cannot be both mandatory and nullable")

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
    def name(self) -> Union[str, Constant, Special]:
        """The name of the inbound field (or a constant or special value)."""
        return self._name

    @property
    def steps(self) -> List[AbstractStep]:
        """The sequence of steps to be applied to the field."""
        return self._steps.copy()

    @property
    def parquet_type(self) -> pa.DataType:
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
        self, value: Value
    ) -> Tuple[Value, Success, Set[Message]]:
        """Validate and parse an inbound value."""
        if isinstance(value, str):
            # Trimming a value's whitespace, replacing with explicit None if the field
            # is empty.
            trimmed = value.strip()
            value = trimmed if trimmed else None

        message: Optional[Message]

        if value is None and self._mandatory:
            message = Message(
                status="ERROR", content="Null value received in mandatory field"
            )
            return (
                None,
                False,
                {
                    message,
                },
            )

        messages: Set[Message] = set()
        field_success = True

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

            if not success:
                if message is None:
                    message = Message(
                        status="ERROR", content=f"{step.name} reported failure"
                    )

                # Stop processing additional steps if failing on error
                # or an internal error is encountered.
                if self._fail_on_error or message.status == "INTERNAL_ERROR":
                    field_success = False
                    messages.add(message)
                    break

                # If continuing with process, downgrade errors to warnings.
                if message.status == "ERROR":
                    message = message.downgrade()

            if message is not None:
                messages.add(message)

        if value is None and not self._nullable:
            # This can happen if we don't raise on error and end up with a
            # null value at the end of the steps.
            if field_success is True:
                message = Message(
                    status="ERROR",
                    content="Null value in non-nullable field after ingestion",
                )
                messages.add(message)
                field_success = False

        return value, field_success, messages


class Schema:
    """An inbound schema, consisting of a number of fields."""

    def __init__(self, *fields: Field):
        self._fields: List[Field] = list(fields)

        unique_names: Set[str] = set()
        for field in self._fields:
            field_name = field.outbound_name
            if field_name in unique_names:
                raise ValueError(f"Multiple fields with outbound name {field_name!r}")
            unique_names.add(field_name)

    @property
    def fields(self) -> List[Field]:
        """A copy of the fields in the schema."""
        return self._fields.copy()

    @property
    def arrow_schema(self) -> pa.Schema:
        """The schema of the resulting Parquet file as an Apache arrow schema."""
        arrow_fields: List[pa.Field] = []
        for field in self._fields:
            arrow_field = pa.field(
                field.outbound_name, field.parquet_type, field.nullable
            )
            arrow_fields.append(arrow_field)

        return pa.schema(arrow_fields)

    def _apply_row(
        self, indexed_row: Tuple[RowIndex, Row], file_name: str
    ) -> Tuple[Row, Success, Set[Message]]:
        """Apply the schema to an individual row."""
        row_index, row = indexed_row

        outbound_row = {}
        row_success = True
        row_messages = set()

        for field in self._fields:
            field_source = field.name

            if isinstance(field_source, str):
                inbound_value = row.get(field_source)
            elif isinstance(field_source, Constant):
                inbound_value = field_source.value
            elif isinstance(field_source, Special):
                value_type = field_source.value_type

                if value_type == "file_name":
                    inbound_value = file_name
                elif value_type == "record_index":
                    inbound_value = row_index
                elif value_type == "record_number":
                    inbound_value = row_index + 1
                else:
                    raise ValueError(
                        f"Unexpected `Special` value type {value_type!r}, "
                        + "Expected one of `{'file_name', 'record_index', "
                        + "'record_number'}`"
                    )
            else:
                raise TypeError(
                    "Field name must be `str`, `Constant` or `Special`, got "
                    + str(type(inbound_value))
                )

            value, success, messages = field.ingest(inbound_value)
            outbound_row[field.outbound_name] = value
            if not success:
                row_success = False
            row_messages |= messages

        return outbound_row, row_success, row_messages

    def apply(
        self,
        data: Iterable[Mapping[str, str]],
        file_name: Optional[str] = None,
        n_processes: int = 1,
    ) -> Tuple[pa.Table, Set[Message]]:
        """Apply the schema to inbound data."""
        rows, all_messages = [], set()
        indexed_rows = enumerate(data)
        apply_func = partial(self._apply_row, file_name=file_name)

        if n_processes == 1:
            process_pool = None
            map_func: MapFunc = map
        else:
            process_pool = Pool(n_processes)  # pylint: disable=consider-using-with
            map_func = process_pool.map

        try:
            for row, success, messages in map_func(apply_func, indexed_rows):
                if success:
                    rows.append(row)
                all_messages |= messages
        finally:
            if process_pool is not None:
                process_pool.close()

        return pa.Table.from_pylist(rows, schema=self.arrow_schema), all_messages

    # TODO: actually handle file -> file
    # TODO: implement logic to render spec to markdown based on field documentation.
