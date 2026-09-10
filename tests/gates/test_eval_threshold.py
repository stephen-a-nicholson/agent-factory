"""eval_threshold_present reads domain.yml directly (see its module
docstring for why), so it is tested against real and temporary domain
directories rather than a JSON fixture like the other rules.
"""

from __future__ import annotations

from pathlib import Path

from gates.rules.eval_threshold import EvalThresholdPresent

MINIMAL_DOMAIN = """
name: {name}
display_name: {name}
description: test
owner: a@b.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: t
      primary_key: id
rag:
  enabled: true
  llm: databricks-meta-llama-3-3-70b-instruct
  eval:
    dataset: eval/rag.jsonl
    judges: [correctness]
    promotion_threshold: {threshold}
"""


def test_passes_on_real_f1_domain():
    findings = EvalThresholdPresent().check({}, None)
    assert findings == []


def test_fails_when_rag_enabled_with_no_promotion_threshold(tmp_path: Path, monkeypatch):
    domain_dir = tmp_path / "domains" / "empty_threshold"
    domain_dir.mkdir(parents=True)
    (domain_dir / "domain.yml").write_text(
        MINIMAL_DOMAIN.format(name="empty_threshold", threshold="{}")
    )

    monkeypatch.setattr("factory.schema.discover_domains", lambda: [domain_dir / "domain.yml"])

    findings = EvalThresholdPresent().check({}, None)
    assert len(findings) == 1
    assert findings[0].rule == "eval_threshold_present"
    assert "empty_threshold" in findings[0].message


def test_passes_when_promotion_threshold_present(tmp_path: Path, monkeypatch):
    domain_dir = tmp_path / "domains" / "with_threshold"
    domain_dir.mkdir(parents=True)
    (domain_dir / "domain.yml").write_text(
        MINIMAL_DOMAIN.format(name="with_threshold", threshold="{correctness: 0.8}")
    )

    monkeypatch.setattr("factory.schema.discover_domains", lambda: [domain_dir / "domain.yml"])

    assert EvalThresholdPresent().check({}, None) == []
