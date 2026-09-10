"""naming_convention: fail if a resource key is not snake_case, or if a
domain-generated resource's key does not contain its domain's name as an
underscore-delimited token.

DESIGN.md section 7 also says "endpoint names are kebab-case": the only
kebab-case endpoint name in this project (`<domain>-agent-<target>`,
built by build_endpoint_name in src/agent/deploy.py) belongs to the
serving endpoint, which is deliberately not a bundle resource at all (see
DESIGN.md section 4), so there is nothing under `resources.*` for a
static gate to check that clause against. Dropped rather than checked
against something else instead: `vector_search_endpoints.agent_factory_vs`,
the one endpoint-shaped bundle resource that does exist, correctly uses
snake_case like every other resource key in this project, not kebab-case.

A resource key is treated as "domain-generated" if it contains one of the
known domain names (from domains/*/domain.yml) as a token; this correctly
skips hand-written shared resources like `schemas.agent_factory` and
`vector_search_endpoints.agent_factory_vs`, which are not generated per
domain and have no reason to carry one's name.
"""

from __future__ import annotations

import re
from typing import Any

from gates.models import Finding, Rule, Severity

SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")


class NamingConvention(Rule):
    name = "naming_convention"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        domain_names = _domain_names()
        findings: list[Finding] = []
        resources = config.get("resources", {})
        for resource_type, keyed in resources.items():
            for key in keyed or {}:
                path = f"resources.{resource_type}.{key}"
                if not SNAKE_CASE.match(key):
                    findings.append(
                        Finding(
                            self.name,
                            Severity.FAIL,
                            f"resource key {key!r} is not snake_case",
                            resource=path,
                        )
                    )
                tokens = key.split("_")
                if any(name in tokens for name in domain_names):
                    continue
                if any(name in key for name in domain_names):
                    findings.append(
                        Finding(
                            self.name,
                            Severity.FAIL,
                            f"resource key {key!r} looks domain-generated but the "
                            "domain name is not a standalone underscore-delimited token",
                            resource=path,
                        )
                    )
        return findings


def _domain_names() -> set[str]:
    from factory.schema import discover_domains, load_domain

    return {load_domain(p).name for p in discover_domains()}
