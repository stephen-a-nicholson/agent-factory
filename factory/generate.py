"""Reads every domains/*/domain.yml and writes resources/generated/<name>.yml.

Never hand-edit files under resources/generated/: change this generator or
the domain metadata, then run `uv run python -m factory.generate` again.
CI runs this and fails the build if the committed output is stale.

Phase 1 scope (see docs/PLAN.md): each domain gets a schema, a raw volume,
a real ingest job (src/ingest/structured.py) and, if genie.enabled, a
Genie space. Vector search indexes, the RAG agent job and its serving
endpoint, evaluation, alerts and the dashboard are added by later phases.
"""

from __future__ import annotations

import hashlib
import json
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


class LiteralStr(str):
    """A str that yaml.dump renders as a literal block (`|`) scalar."""


def _literal_str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralStr, _literal_str_representer)


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


def _schema_ref(domain: Domain) -> str:
    return f"${{resources.schemas.{domain.name}.name}}"


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
                        "python_file": "../../src/ingest/structured.py",
                        "parameters": [
                            "--domain",
                            domain.name,
                            "--catalog",
                            CATALOG_REF,
                            "--schema",
                            _schema_ref(domain),
                        ],
                    },
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {"environment_version": "2", "dependencies": ["pyyaml"]},
                }
            ],
        }
    }


def _stable_id(*parts: str) -> str:
    """A deterministic stand-in for the random IDs Genie normally assigns,
    so regenerating a domain's space produces byte-identical output."""
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _genie_space_json(domain: Domain) -> str:
    """The .geniespace.json content, per Databricks' documented schema
    (version 2: config.sample_questions, data_sources.tables,
    instructions.text_instructions). Embedded as serialized_space rather
    than written to a file and referenced via file_path: file_path content
    is uploaded verbatim with no variable substitution, but the table
    identifiers here need ${var.catalog} and the schema's resolved (and,
    in dev, prefixed) name, which only serialized_space interpolates. See
    docs/PLAN.md notes, phase 1."""
    genie = domain.genie
    assert genie is not None

    table_prefix = f"{CATALOG_REF}.{_schema_ref(domain)}"
    space: dict[str, Any] = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": _stable_id(domain.name, "question", q), "question": [q]}
                for q in genie.sample_questions
            ]
        },
        "data_sources": {
            # The Genie API rejects a create/update whose tables aren't
            # sorted by identifier; since the prefix is identical for every
            # table here, sorting by table name is equivalent and simpler.
            "tables": [
                {"identifier": f"{table_prefix}.{table.name}"}
                for table in sorted(domain.structured.tables, key=lambda t: t.name)
            ]
        },
        "instructions": {
            "text_instructions": (
                [
                    {
                        "id": _stable_id(domain.name, "instructions"),
                        "content": [genie.instructions.strip()],
                    }
                ]
                if genie.instructions.strip()
                else []
            )
        },
    }
    return json.dumps(space, indent=2)


def _genie_space_key(domain: Domain) -> str:
    return f"{domain.name}_genie"


def _genie_space_resource(domain: Domain) -> dict[str, Any] | None:
    if domain.genie is None or not domain.genie.enabled:
        return None
    return {
        _genie_space_key(domain): {
            "title": domain.display_name,
            "description": domain.description.strip(),
            "warehouse_id": "${var.warehouse_id}",
            "serialized_space": LiteralStr(_genie_space_json(domain)),
            # No permissions block: the Genie agent permissions API doesn't
            # exist yet and deploy fails after creating the space if one is
            # set. Grant access through Unity Catalog on the underlying
            # tables instead. See CLAUDE.md.
        }
    }


def _evaluate_genie_job_resource(domain: Domain) -> dict[str, Any] | None:
    """The benchmark job for domain.genie.benchmarks. Only emitted once a
    Genie space exists to query, and only if the domain actually points at
    a benchmark file (phase 1 scope: the job itself; the regression alert
    and observability dashboard are added in phase 3 alongside the RAG
    evaluate job, see docs/PLAN.md)."""
    if domain.genie is None or not domain.genie.enabled or not domain.genie.benchmarks:
        return None
    job_name = f"evaluate_genie_{domain.name}"
    space_ref = f"${{resources.genie_spaces.{_genie_space_key(domain)}.id}}"
    # The shared agent_factory schema (resources/core/shared.yml) is a
    # bundle resource like any other, so dev mode prefixes its deployed
    # name too; resolve it the same way as the domain's own schema rather
    # than hardcoding "agent_factory".
    eval_table_ref = "${var.catalog}.${resources.schemas.agent_factory.name}.eval_results"
    return {
        job_name: {
            "name": job_name,
            "tags": {
                "owner": domain.owner,
                **domain.tags,
            },
            "tasks": [
                {
                    "task_key": "evaluate_genie_benchmarks",
                    "environment_key": "default",
                    "spark_python_task": {
                        "python_file": "../../src/evaluate/genie.py",
                        "parameters": [
                            "--domain",
                            domain.name,
                            "--space-id",
                            space_ref,
                            "--eval-table",
                            eval_table_ref,
                            "--target",
                            "${bundle.target}",
                        ],
                    },
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {
                        "environment_version": "2",
                        "dependencies": ["pyyaml", "databricks-sdk"],
                    },
                }
            ],
        }
    }


def render_domain_resources(domain: Domain) -> dict[str, Any]:
    """Build the `resources:` mapping for one domain."""
    resources: dict[str, Any] = {
        "schemas": _schema_resource(domain),
        "volumes": _volume_resource(domain),
        "jobs": _ingest_job_resource(domain),
    }
    genie_space = _genie_space_resource(domain)
    if genie_space is not None:
        resources["genie_spaces"] = genie_space

    evaluate_job = _evaluate_genie_job_resource(domain)
    if evaluate_job is not None:
        resources["jobs"].update(evaluate_job)

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
