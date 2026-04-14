# Shared in-process state populated by the lifespan handler in main.py.
# All route modules import _state from here to access loaded models and data.

from typing import Any

_state: dict[str, Any] = {}
