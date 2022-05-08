"""Type stubs for relevant functionality from PyArrow."""
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

class DataType: ...
class Schema: ...
class Field: ...

class Table:
    ...

    @classmethod
    def from_pylist(
        cls,
        mapping: List[Mapping[str, Any]],
        schema: Schema,
        metadata: Optional[Dict[str, Any]] = None,
    ): ...

def field(
    name: str,
    type: DataType,
    nullable: bool = True,
    metadata: Optional[Dict[str, Any]] = None,
) -> Field: ...
def schema(
    fields: Iterable[Union[Field, Tuple[str, DataType]]],
    metadata: Optional[Dict[str, Any]] = None,
) -> Schema: ...
def string() -> DataType: ...
