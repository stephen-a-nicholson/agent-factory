"""One passing check (against the real dev bundle's config, captured as
fixtures/pass_config.json) and one failing check (a small targeted
fixture under fixtures/) per rule. eval_threshold_present is not
config-based (it reads domain.yml directly) and is tested separately in
test_eval_threshold.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from gates.models import Severity
from gates.rules.clusters import NoAllPurposeClusters
from gates.rules.endpoints import EndpointScaleToZero
from gates.rules.genie import GenieNoPermissions
from gates.rules.job_tests import JobHasTest
from gates.rules.naming import NamingConvention
from gates.rules.secrets import NoSecretsInYaml
from gates.rules.tags import RequiredTags
from gates.rules.targets import ProdNoDevMode
from gates.rules.vector_search import VsSyncInNonprod

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


PASS_CONFIG = load("pass_config.json")

CONFIG_BASED_RULES = [
    (NoAllPurposeClusters(), "fail_no_all_purpose_clusters.json"),
    (RequiredTags(), "fail_required_tags.json"),
    (NamingConvention(), "fail_naming_convention.json"),
    (ProdNoDevMode(), "fail_prod_no_dev_mode.json"),
    (GenieNoPermissions(), "fail_genie_no_permissions.json"),
    (VsSyncInNonprod(), "fail_vs_sync_in_nonprod.json"),
    (NoSecretsInYaml(), "fail_no_secrets_in_yaml.json"),
    (JobHasTest(), "fail_job_has_test.json"),
    (EndpointScaleToZero(), "fail_endpoint_scale_to_zero.json"),
]


@pytest.mark.parametrize("rule,_fixture", CONFIG_BASED_RULES, ids=lambda v: getattr(v, "name", v))
def test_rule_passes_on_real_dev_config(rule, _fixture):
    findings = rule.check(PASS_CONFIG, None)
    assert findings == [], f"{rule.name} unexpectedly flagged the real passing config: {findings}"


@pytest.mark.parametrize("rule,fixture", CONFIG_BASED_RULES, ids=lambda v: getattr(v, "name", v))
def test_rule_fails_on_its_fixture(rule, fixture):
    findings = rule.check(load(fixture), None)
    assert findings, f"{rule.name} did not flag its own failing fixture ({fixture})"
    assert all(f.rule == rule.name for f in findings)


def test_naming_convention_accepts_multi_word_domain_names(monkeypatch):
    # Real bug found adding the uk_rail domain in phase 6: the rule used
    # to split a resource key on "_" and check the domain name was one of
    # the resulting tokens, which can never match a domain name that
    # itself contains an underscore. See gates/rules/naming.py.
    monkeypatch.setattr("factory.schema.discover_domains", lambda: [])
    monkeypatch.setattr(
        "gates.rules.naming._domain_names",
        lambda: {"uk_rail"},
    )
    config = {
        "bundle": {"target": "dev"},
        "resources": {
            "jobs": {"ingest_uk_rail": {}, "deploy_agent_uk_rail": {}},
            "schemas": {"uk_rail": {}},
            "genie_spaces": {"uk_rail_genie": {}},
        },
    }
    assert NamingConvention().check(config, None) == []


def test_no_all_purpose_clusters_flags_new_cluster_too():
    config = {
        "bundle": {"target": "dev"},
        "resources": {
            "jobs": {
                "j": {
                    "tags": {"owner": "a", "cost_centre": "b"},
                    "tasks": [{"task_key": "t", "new_cluster": {"num_workers": 2}}],
                }
            }
        },
    }
    findings = NoAllPurposeClusters().check(config, None)
    assert len(findings) == 1
    assert "new_cluster" in findings[0].message


def test_no_all_purpose_clusters_flags_top_level_clusters_resource():
    config = {"bundle": {"target": "dev"}, "resources": {"clusters": {"c": {}}}}
    findings = NoAllPurposeClusters().check(config, None)
    assert len(findings) == 1
    assert findings[0].resource == "resources.clusters"


def test_prod_no_dev_mode_passes_when_production():
    config = {"bundle": {"target": "prod", "mode": "production"}, "resources": {}}
    assert ProdNoDevMode().check(config, None) == []


def test_prod_no_dev_mode_ignores_dev_target():
    config = {"bundle": {"target": "dev", "mode": "development"}, "resources": {}}
    assert ProdNoDevMode().check(config, None) == []


def test_vs_sync_in_nonprod_allows_continuous_in_prod():
    config = {
        "bundle": {"target": "prod"},
        "resources": {
            "vector_search_indexes": {
                "x": {"delta_sync_index_spec": {"pipeline_type": "CONTINUOUS"}}
            }
        },
    }
    assert VsSyncInNonprod().check(config, None) == []


def test_endpoint_scale_to_zero_ignores_prod():
    config = load("fail_endpoint_scale_to_zero.json")
    config["bundle"]["target"] = "prod"
    assert EndpointScaleToZero().check(config, None) == []


def test_no_secrets_in_yaml_flags_url_credentials():
    from gates.rules.secrets import NoSecretsInYaml

    config = {
        "bundle": {"target": "dev"},
        "resources": {"jobs": {}},
        "extra": {"url": "https://user:hunter2@example.com/db"},
    }
    findings = NoSecretsInYaml().check(config, None)
    assert any("url" in f.message for f in findings)


def test_no_secrets_in_yaml_ignores_safe_key_names():
    config = {
        "bundle": {"target": "dev"},
        "resources": {},
        "etag": "a" * 40,
        "job_id": "1234567890123456789012345678901234",
    }
    assert NoSecretsInYaml().check(config, None) == []


def test_finding_severity_matches_rule_expectation():
    findings = RequiredTags().check(load("fail_required_tags.json"), None)
    assert findings[0].severity == Severity.FAIL

    findings = JobHasTest().check(load("fail_job_has_test.json"), None)
    assert findings[0].severity == Severity.WARN
