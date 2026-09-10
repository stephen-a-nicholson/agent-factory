from __future__ import annotations

import json
from pathlib import Path

import pytest

from gates.models import Finding, Severity
from gates.run import report_json, report_text, run_gates

FIXTURES = Path(__file__).parent / "fixtures"


def test_run_gates_on_real_dev_config_has_no_fail_findings():
    config = json.loads((FIXTURES / "pass_config.json").read_text())
    findings = run_gates(config, None)
    assert [f for f in findings if f.severity == Severity.FAIL] == []


def test_report_text_summarises_counts(capsys):
    findings = [
        Finding("r1", Severity.FAIL, "bad", "resources.jobs.x"),
        Finding("r2", Severity.WARN, "meh", None),
    ]
    report_text(findings)
    out = capsys.readouterr().out
    assert "FAIL r1: bad [resources.jobs.x]" in out
    assert "WARN r2: meh" in out
    assert "1 fail, 1 warn" in out


def test_report_text_all_passed(capsys):
    report_text([])
    assert capsys.readouterr().out.strip() == "all gates passed"


def test_report_json_shape(capsys):
    findings = [Finding("r1", Severity.FAIL, "bad", "resources.jobs.x")]
    report_json(findings)
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == [
        {"rule": "r1", "severity": "fail", "message": "bad", "resource": "resources.jobs.x"}
    ]


@pytest.mark.parametrize("fixture_name", ["fail_required_tags.json"])
def test_run_gates_reports_fail_from_fixture(fixture_name):
    config = json.loads((FIXTURES / fixture_name).read_text())
    findings = run_gates(config, None)
    assert any(f.severity == Severity.FAIL for f in findings)
