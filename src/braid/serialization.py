from __future__ import annotations

from collections.abc import Callable
from typing import Any

import cloudpickle

from braid.errors import PayloadTooLargeError

# Lambda async invocation payload limit is 256KB; leave headroom for metadata
_FN_SIZE_LIMIT = 200 * 1024  # 200KB


def serialize_fn(fn: Callable) -> bytes:
    data = cloudpickle.dumps(fn)
    if len(data) > _FN_SIZE_LIMIT:
        raise PayloadTooLargeError(len(data), _FN_SIZE_LIMIT)
    return data


def serialize_input(x: Any) -> bytes:
    return cloudpickle.dumps(x)
