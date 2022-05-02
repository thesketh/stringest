"""Type aliases used in a few places."""
from typing import Any

Success = bool
"""A boolean value indicating success of the step."""
Value = Any
"""
A value passed to (and returned by) the step.

For the first step in the sequence, this is always a nullable string.

"""
