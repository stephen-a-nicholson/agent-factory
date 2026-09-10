"""Loads the JSON a rule needs from `databricks bundle validate -o json`
(the resolved bundle config: `bundle.mode`, `resources.<type>.<key>`) and,
optionally, `databricks bundle plan -o json` (a dict keyed by
`resources.<type>.<key>`, each entry `{action, remote_state, changes,
depends_on}` describing what would change against the live workspace).

Most rules only need the validate config: it is available even against a
target that has never been deployed, which the plan output is not
guaranteed to be. See docs/DESIGN.md section 7.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def resources_of_type(config: dict[str, Any], resource_type: str) -> dict[str, dict[str, Any]]:
    """The resolved resources of one type (e.g. "jobs") from a bundle
    validate config, keyed by resource key. Empty dict if the bundle has
    none of that type."""
    return config.get("resources", {}).get(resource_type, {}) or {}
