"""Reads every domains/*/domain.yml and writes resources/generated/<name>.yml.

Never hand-edit files under resources/generated/: change this generator or
the domain metadata, then run `uv run python -m factory.generate` again.
CI runs this and fails the build if the committed output is stale.

Phase 0 scope (see docs/PLAN.md): each domain gets a schema, a raw volume
and a placeholder ingest job. Genie spaces, vector search indexes, the RAG
agent job and its serving endpoint, evaluation, alerts and the dashboard
are added by later phases as the schema and this generator grow to support
them.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

from factory.schema import REPO_ROOT, Domain, discover_domains, export_json_schema, load_domain

GENERATED_DIR = REPO_ROOT / "resources" / "generated"

HEADER = """\
# GENERATED FILE. Do not edit by hand.
# Source: domains/{name}/domain.yml
# Regenerate with: uv run python -m factory.generate
"""


CATALOG_REF = "${var.catalog}"


def _schema_resource(domain: Domain) -> dict[str, Any]:
    return {
        domain.name: {
            "name": domain.name,
            "catalog_name": CATALOG_REF,
            "comment": domain.description.strip(),
        }
    }


def _volume_resource(domain: Domain) -> dict[str, Any]:
    volume_name = f"{domain.name}_raw"
    return {
        volume_name: {
            "name": volume_name,
            "catalog_name": CATALOG_REF,
            "schema_name": domain.name,
            "volume_type": "MANAGED",
            "comment": f"Raw seed files and downloaded documents for the {domain.name} domain.",
        }
    }


def _ingest_job_resource(domain: Domain) -> dict[str, Any]:
    job_name = f"ingest_{domain.name}"
    return {
        job_name: {
            "name": job_name,
            "tags": {
                "owner": domain.owner,
                **domain.tags,
            },
            "tasks": [
                {
                    "task_key": "ingest_structured",
                    "environment_key": "default",
                    "spark_python_task": {
                        "python_file": "../../src/ingest/placeholder.py",
                    },
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {"environment_version": "2"},
                }
            ],
        }
    }


def render_domain_resources(domain: Domain) -> dict[str, Any]:
    """Build the `resources:` mapping for one domain, phase-0 scope only."""
    resources: dict[str, Any] = {
        "schemas": _schema_resource(domain),
        "volumes": _volume_resource(domain),
        "jobs": _ingest_job_resource(domain),
    }
    return {"resources": resources}


def write_generated_file(domain: Domain) -> Path:
    out_path = GENERATED_DIR / f"{domain.name}.yml"
    body = yaml.dump(
        render_domain_resources(domain),
        sort_keys=False,
        default_flow_style=False,
    )
    out_path.write_text(HEADER.format(name=domain.name) + "\n" + body)
    return out_path


def remove_stale_generated_files(current_domain_names: set[str]) -> list[Path]:
    """Delete generated files left over from domains that no longer exist."""
    removed = []
    if not GENERATED_DIR.exists():
        return removed
    for path in GENERATED_DIR.glob("*.yml"):
        if path.stem not in current_domain_names:
            path.unlink()
            removed.append(path)
    return removed


def generate() -> list[Path]:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    domain_paths = discover_domains()
    domains = [load_domain(p) for p in domain_paths]

    written = [write_generated_file(d) for d in domains]
    removed = remove_stale_generated_files({d.name for d in domains})
    export_json_schema()

    for path in removed:
        print(f"removed stale generated file: {path.relative_to(REPO_ROOT)}")
    for path in written:
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    return written


if __name__ == "__main__":
    if not discover_domains():
        print("no domains found under domains/*/domain.yml", file=sys.stderr)
        sys.exit(1)
    generate()
