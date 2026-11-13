"""Write measured results to results/<name>.json. README numbers are read from these files (and a test
checks they match), so nothing in the docs is typed in by hand."""

import json
from datetime import UTC, datetime
from decimal import Decimal

from p360.paths import RESULTS


def _default(o):
    if isinstance(o, Decimal):
        return str(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(type(o))


def record(name: str, payload: dict) -> dict:
    RESULTS.mkdir(parents=True, exist_ok=True)
    body = {"recorded_at": datetime.now(UTC).isoformat(timespec="seconds"), **payload}
    (RESULTS / f"{name}.json").write_text(json.dumps(body, indent=2, default=_default) + "\n")
    return body


def load(name: str) -> dict:
    return json.loads((RESULTS / f"{name}.json").read_text())
