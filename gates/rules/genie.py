"""genie_no_permissions: fail if a genie_spaces resource declares a
permissions block. The Genie agent permissions API does not exist yet;
deploy fails after creating the space if one is set. See CLAUDE.md and
factory/generate.py's _genie_space_resource.
"""

from __future__ import annotations

from typing import Any

from gates.loader import resources_of_type
from gates.models import Finding, Rule, Severity


class GenieNoPermissions(Rule):
    name = "genie_no_permissions"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        findings: list[Finding] = []
        for key, space in resources_of_type(config, "genie_spaces").items():
            if "permissions" in space:
                findings.append(
                    Finding(
                        self.name,
                        Severity.FAIL,
                        "genie_spaces resource declares a permissions block, which "
                        "fails deploy: the Genie permissions API does not exist",
                        resource=f"resources.genie_spaces.{key}",
                    )
                )
        return findings
