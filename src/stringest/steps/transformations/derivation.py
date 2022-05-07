"""
Transformation steps built around replacing values.

"""
from typing import Any, Dict, Literal, Optional, Tuple

from stringest.message import Message
from stringest.steps.base import AbstractStep
from stringest.type_aliases import Value, Success


class DefaultValueDerivation(AbstractStep):
    """
    A basic transformation step, deriving a default value.

    Arguments:
     - `default_value`: the default value
     - `action`: one of `{'replace', 'fill'}`. If `'replace'`, ignore the current
       value and replace it with the default. If `'fill'`, only replace the current
       value if it is `None` (default: `'replace'`)

    """

    def __init__(
        self, *, default_value: Any, action: Literal["fill", "replace"] = "replace"
    ):
        self._default_value = default_value
        if action not in {"fill", "replace"}:
            raise ValueError("`action` must be one of `{'fill', 'replace'}`")
        self._fill = action == "fill"

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            "default_value": self._default_value,
            "action": "fill" if self._fill else "replace",
        }

    @property
    def type(self) -> Literal["transformation"]:
        return "transformation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        if value is None or self._fill:
            return self._default_value, True, None
        return value, True, None


class DictionaryLookupDerivation(AbstractStep):
    """
    A basic derivation step, replacing values with a dictionary lookup.

    Arguments:
     - `lookup_dict`: the dictionary containing replacement values.
     - `fail_if_missing`: whether to consider having items which aren't in the
       dict a failure. If `False`, the original value will be returned (default:
       `False`)

    """

    def __init__(self, *, lookup_dict: Dict[Any, Any], fail_if_missing: bool = False):
        self._lookup_dict = lookup_dict
        self._fail_if_missing = fail_if_missing

    @property
    def parameters(self) -> Dict[str, Any]:
        return {
            "lookup_dict": self._lookup_dict.copy(),
            "fail_if_missing": self._fail_if_missing,
        }

    @property
    def type(self) -> Literal["transformation"]:
        return "transformation"

    def apply(self, value: Value) -> Tuple[Value, Success, Optional[Message]]:
        try:
            new_value = self._lookup_dict[value]
        except KeyError:
            if self._fail_if_missing:
                message = Message(
                    status="ERROR",
                    content=f"{value!r} is not in the lookup dict for this field",
                )
                return None, False, message

            new_value = value
        return new_value, True, None
