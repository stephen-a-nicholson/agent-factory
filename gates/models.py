"""Shared types for bundle quality gates: a Finding a rule reports, and
the Rule base class every rule under gates/rules/ implements.

See docs/DESIGN.md section 7 for the rule table and gates/run.py for how
these are loaded, executed and reported.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Severity(StrEnum):
    FAIL = "fail"
    WARN = "warn"


@dataclass(frozen=True)
class Finding:
    rule: str
    severity: Severity
    message: str
    resource: str | None = None


class Rule:
    """Base class for a gate rule. Subclasses set `name` and implement
    `check`, returning one Finding per violation found (an empty list
    means the rule passed cleanly)."""

    name: str = ""

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        raise NotImplementedError
