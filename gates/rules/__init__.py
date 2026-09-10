"""Gate rules. Each module defines one Rule subclass; ALL_RULES lists
every rule gates/run.py should execute. Adding a rule is one file here
plus a passing and a failing fixture under tests/gates/fixtures/."""

from __future__ import annotations

from gates.models import Rule
from gates.rules.clusters import NoAllPurposeClusters
from gates.rules.endpoints import EndpointScaleToZero
from gates.rules.eval_threshold import EvalThresholdPresent
from gates.rules.genie import GenieNoPermissions
from gates.rules.job_tests import JobHasTest
from gates.rules.naming import NamingConvention
from gates.rules.secrets import NoSecretsInYaml
from gates.rules.tags import RequiredTags
from gates.rules.targets import ProdNoDevMode
from gates.rules.vector_search import VsSyncInNonprod

ALL_RULES: list[Rule] = [
    NoAllPurposeClusters(),
    RequiredTags(),
    NamingConvention(),
    ProdNoDevMode(),
    GenieNoPermissions(),
    VsSyncInNonprod(),
    NoSecretsInYaml(),
    JobHasTest(),
    EndpointScaleToZero(),
    EvalThresholdPresent(),
]
