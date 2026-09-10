"""Reads every domains/*/domain.yml and writes resources/generated/<name>.yml.

Never hand-edit files under resources/generated/: change this generator or
the domain metadata, then run `uv run python -m factory.generate` again.
CI runs this and fails the build if the committed output is stale.

Phase 3 scope (see docs/PLAN.md): each domain gets a schema, a raw volume,
an ingest job (structured data and, if documents.loader != "none",
documents), a Genie space and its benchmark job, a vector search index, a
RAG agent deploy job, a RAG evaluate+promote job, and a regression alert.
"""

from __future__ import annotations

import hashlib
import json
import sys
import textwrap
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


def _volume_ref(domain: Domain) -> str:
    return f"${{resources.volumes.{domain.name}_raw.name}}"


def _ingest_documents_task(domain: Domain) -> dict[str, Any] | None:
    """--index-name lets documents.py trigger a vector search sync after
    rewriting the chunks table: a TRIGGERED delta-sync index does not
    re-sync on its own just because the source table changed, or because
    the (unchanged) index resource gets redeployed — confirmed by adding
    a second document and finding the index still only had the original
    chunks until an explicit sync-index call. See docs/PLAN.md notes,
    phase 3. documents.py looks the index up and skips the sync (rather
    than failing the job) if it doesn't exist yet, since on a domain's
    very first ingest the index hasn't been created yet either."""
    if domain.documents is None or domain.documents.loader == "none":
        return None
    return {
        "task_key": "ingest_documents",
        "environment_key": "documents",
        "spark_python_task": {
            "python_file": "../../src/ingest/documents.py",
            "parameters": [
                "--domain",
                domain.name,
                "--catalog",
                CATALOG_REF,
                "--schema",
                _schema_ref(domain),
                "--volume-name",
                _volume_ref(domain),
                "--index-name",
                _vector_search_index_name(domain),
            ],
        },
    }


def _ingest_job_resource(domain: Domain) -> dict[str, Any]:
    job_name = f"ingest_{domain.name}"
    tasks: list[dict[str, Any]] = [
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
    ]
    environments: list[dict[str, Any]] = [
        {
            "environment_key": "default",
            "spec": {"environment_version": "2", "dependencies": ["pyyaml"]},
        }
    ]

    documents_task = _ingest_documents_task(domain)
    if documents_task is not None:
        tasks.append(documents_task)
        environments.append(
            {
                "environment_key": "documents",
                # ai_parse_document/ai_prep_search require serverless
                # environment version 3+ (VARIANT support); confirmed
                # against a real Free Edition workspace. See docs/PLAN.md
                # notes, phase 2.
                "spec": {"environment_version": "3", "dependencies": ["pyyaml"]},
            }
        )

    return {
        job_name: {
            "name": job_name,
            "tags": {
                "owner": domain.owner,
                **domain.tags,
            },
            "tasks": tasks,
            "environments": environments,
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


def _chunks_table_ref(domain: Domain) -> str:
    return f"{CATALOG_REF}.{_schema_ref(domain)}.chunks"


def _vector_search_index_key(domain: Domain) -> str:
    return f"{domain.name}_chunks"


def _vector_search_index_name(domain: Domain) -> str:
    """The fully qualified index name, computed the same statically-known
    way whether it's used inside the vector_search_indexes resource
    itself or referenced from a job's parameters. Deliberately not a
    ${resources.vector_search_indexes...} reference: the ingest job needs
    this name too (to trigger a sync after writing new chunks, see
    _ingest_documents_task), and referencing the index resource from the
    ingest job would recreate the exact circular dependency documented on
    _deploy_agent_job_resource, since the index can't be created until
    the ingest job has run once. This string is fully computable from the
    domain and CATALOG_REF/schema references alone, so no resource
    reference, and no dependency edge, is needed at all."""
    return f"{CATALOG_REF}.{_schema_ref(domain)}.{_vector_search_index_key(domain)}"


def _vector_search_index_resource(domain: Domain) -> dict[str, Any] | None:
    """The delta-sync index over the chunks table src/ingest/documents.py
    writes. Only emitted if the domain has documents to index; the index
    can't be created until the chunks table exists, same
    deploy-then-ingest-then-deploy sequencing as the Genie space.

    Unlike jobs/schemas/genie spaces, a vector search index's `name` field
    must be the fully qualified <catalog>.<schema>.<index> name, not a
    bare identifier: confirmed by a real deploy failure ("Invalid index
    name. Must specify the full index name"). See docs/PLAN.md notes,
    phase 2. The bundle resource *key* (dict key below) can still be a
    short logical name.
    """
    if domain.documents is None or domain.documents.loader == "none":
        return None
    key = _vector_search_index_key(domain)
    return {
        key: {
            "name": _vector_search_index_name(domain),
            "endpoint_name": "${resources.vector_search_endpoints.agent_factory_vs.name}",
            "primary_key": "chunk_id",
            "index_type": "DELTA_SYNC",
            "delta_sync_index_spec": {
                "source_table": _chunks_table_ref(domain),
                "pipeline_type": "${var.vs_sync}",
                "embedding_source_columns": [
                    {
                        "name": "chunk_to_embed",
                        "embedding_model_endpoint_name": domain.documents.embedding_model,
                    }
                ],
            },
        }
    }


def _deploy_agent_job_resource(domain: Domain) -> dict[str, Any] | None:
    """The job that logs and registers the RAG agent as the candidate
    version. Depends on the vector search index (and, if genie.tools is
    set, the Genie space) already existing, so it can only succeed after
    those have deployed. Does not touch the serving endpoint itself: only
    src/agent/promote.py does that, once evaluate_<name> has passed (see
    that job in _rag_eval_job_resource) — deploy.py's own module docstring
    explains why a first version of this job updating the endpoint
    straight away was a real bug, found running the phase 3
    deliberately-bad-system-prompt scenario for real.

    There is deliberately no bundle-declared model_serving_endpoints
    resource: a real deploy hit a genuine circular dependency trying to
    add one. The endpoint needs the domain's rag_agent model to already
    exist (it doesn't, until this job runs); referencing the endpoint's
    resolved (dev-mode-prefixed) name from this job's parameters, so the
    job could tell deploy.py what to call it, made *creating the job
    resource itself* depend on the endpoint already having deployed
    successfully — direct-engine dependency edges are conservative about
    any `${resources...}` reference, even to a statically-known field.
    Neither resource could ever be created on a fresh deploy.
    src/agent/deploy.py's update_serving_endpoint already creates-or-
    updates the endpoint directly via the SDK, and is a strictly better
    fit anyway: Databricks serving endpoints only accept a numeric
    entity_version, never an alias, so a bundle-declared endpoint could
    only ever bootstrap version "1" and would drift on every real
    promotion. See deploy.py's module docstring and docs/PLAN.md notes,
    phase 2. --target lets deploy.py compute a workspace-unique endpoint
    name itself (<domain>-agent-<target>), since dev/test/prod may share
    one workspace (see docs/PLAN.md phase 0 notes on Free Edition) and
    nothing here can rely on DAB's own dev-mode prefixing to keep the
    literal name unique without the resource reference that caused this
    problem in the first place.
    """
    if domain.rag is None or not domain.rag.enabled:
        return None
    job_name = f"deploy_agent_{domain.name}"
    parameters = [
        "--domain",
        domain.name,
        "--catalog",
        CATALOG_REF,
        "--schema",
        _schema_ref(domain),
        "--index-name",
        _vector_search_index_name(domain),
        "--target",
        "${bundle.target}",
    ]
    if domain.rag.tools.genie and domain.genie is not None and domain.genie.enabled:
        parameters += [
            "--genie-space-id",
            f"${{resources.genie_spaces.{_genie_space_key(domain)}.id}}",
            "--warehouse-id",
            "${var.warehouse_id}",
        ]
    return {
        job_name: {
            "name": job_name,
            "tags": {
                "owner": domain.owner,
                **domain.tags,
            },
            "tasks": [
                {
                    "task_key": "deploy_agent",
                    "environment_key": "default",
                    "spark_python_task": {
                        "python_file": "../../src/agent/deploy.py",
                        "parameters": parameters,
                    },
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {
                        "environment_version": "2",
                        "dependencies": [
                            "pyyaml",
                            "mlflow[databricks]",
                            "databricks-sdk",
                            "databricks-ai-search",
                        ],
                    },
                }
            ],
        }
    }


def _rag_eval_job_resource(domain: Domain) -> dict[str, Any] | None:
    """Evaluates the domain's candidate RAG agent version against
    domain.yml's judges and promotion thresholds, then, only if that
    passes, promotes it to champion. The "only if" is a task dependency,
    not application logic: the promote task depends_on evaluate, and a
    Databricks job skips a task whose dependency failed rather than
    running it, so a failed evaluate task (src/evaluate/rag.py exits
    non-zero below any threshold) means promote never runs and champion
    never moves. This is deliberately a separate job from
    deploy_agent_<name>: evaluation is meant to run against whatever
    version is currently aliased candidate, independent of exactly when
    or how it got deployed, matching DESIGN.md section 6's CI/CD flow of
    deploy, then evaluate, as separate steps."""
    if domain.rag is None or not domain.rag.enabled:
        return None
    job_name = f"evaluate_{domain.name}"
    eval_table_ref = "${var.catalog}.${resources.schemas.agent_factory.name}.eval_results"
    common_parameters = [
        "--domain",
        domain.name,
        "--catalog",
        CATALOG_REF,
        "--schema",
        _schema_ref(domain),
        "--target",
        "${bundle.target}",
    ]
    return {
        job_name: {
            "name": job_name,
            "tags": {
                "owner": domain.owner,
                **domain.tags,
            },
            "tasks": [
                {
                    "task_key": "evaluate",
                    "environment_key": "default",
                    "spark_python_task": {
                        "python_file": "../../src/evaluate/rag.py",
                        "parameters": [
                            *common_parameters,
                            "--eval-table",
                            eval_table_ref,
                        ],
                    },
                },
                {
                    "task_key": "promote",
                    "environment_key": "default",
                    "depends_on": [{"task_key": "evaluate"}],
                    "spark_python_task": {
                        "python_file": "../../src/agent/promote.py",
                        "parameters": common_parameters,
                    },
                },
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {
                        "environment_version": "2",
                        # databricks-ai-search is needed here, not just by
                        # deploy_agent_<name>: mlflow.pyfunc.load_model()
                        # below loads rag_agent.py itself into this task's
                        # process to call predict(), so its retriever tool
                        # (AISearchClient) needs the package present here
                        # too. Confirmed by a real run failing predict_fn
                        # with "No module named 'databricks.ai_search'".
                        # See docs/PLAN.md notes, phase 3.
                        "dependencies": [
                            "pyyaml",
                            "mlflow[databricks]",
                            "databricks-sdk",
                            "databricks-ai-search",
                        ],
                    },
                }
            ],
        }
    }


def _eval_regression_alert_resource(domain: Domain) -> dict[str, Any] | None:
    """Fires when the domain's most recent recorded rag_eval score for any
    judge with a configured promotion_threshold has dropped below it,
    independent of whether that run actually blocked promotion (a
    regression that got caught and gated is still worth an operator
    looking at). Queries the shared eval_results table (see
    src/evaluate/rag.py's write_results) directly with SQL rather than
    reusing check_thresholds: an alert's query_text has to be a single
    self-contained SQL statement the warehouse runs on a schedule, not a
    Python callable, per the AlertV2 resource shape (see docs/PLAN.md
    notes, phase 3).

    Not emitted if the domain has no RAG agent, or no thresholds
    configured to regress against."""
    if domain.rag is None or not domain.rag.enabled:
        return None
    promotion_threshold = domain.rag.eval.promotion_threshold
    if not promotion_threshold:
        return None

    eval_table_ref = "${var.catalog}.${resources.schemas.agent_factory.name}.eval_results"
    threshold_values = ", ".join(
        f"('rag_eval.{judge}', {threshold})" for judge, threshold in promotion_threshold.items()
    )
    query_text = LiteralStr(
        f"""WITH latest AS (
  SELECT metric, score,
         ROW_NUMBER() OVER (PARTITION BY metric ORDER BY timestamp DESC) AS rn
  FROM {eval_table_ref}
  WHERE domain = '{domain.name}' AND target = '${{bundle.target}}' AND metric LIKE 'rag_eval.%'
),
thresholds(metric, threshold) AS (VALUES {threshold_values})
SELECT MIN(latest.score - thresholds.threshold) AS margin
FROM thresholds
JOIN latest ON latest.metric = thresholds.metric AND latest.rn = 1
"""
    )
    return {
        f"{domain.name}_eval_regression": {
            "display_name": f"{domain.display_name}: RAG evaluation regression",
            "warehouse_id": "${var.warehouse_id}",
            "query_text": query_text,
            "evaluation": {
                "comparison_operator": "LESS_THAN",
                "source": {"name": "margin"},
                "threshold": {"value": {"double_value": 0}},
                # OK rather than UNKNOWN (the SDK's own doc warns UNKNOWN is
                # being deprecated) for the case where evaluate_<name> has
                # never run yet on this target, so the alert doesn't fire
                # before there is any result to judge.
                "empty_result_state": "OK",
            },
            "schedule": {
                "quartz_cron_schedule": "0 0 8 * * ?",
                "timezone_id": "UTC",
            },
        }
    }


def _dashboard_dataset(name: str, display_name: str, query: str) -> dict[str, Any]:
    """A Lakeview dataset: `queryLines` is a list of strings, one per source
    line including its trailing newline, not a single multi-line string.
    Shape confirmed against a real dashboard already in the workspace (see
    docs/PLAN.md notes, phase 3): `databricks lakeview get <id>` on an
    existing AI/BI dashboard, since there is no public JSON schema for
    serialized_dashboard, unlike every other resource type here."""
    return {
        "name": name,
        "displayName": display_name,
        "queryLines": [line + "\n" for line in textwrap.dedent(query).strip().split("\n")],
    }


def _dashboard_json(domain: Domain) -> str:
    eval_table_ref = "${var.catalog}.${resources.schemas.agent_factory.name}.eval_results"
    endpoint_name = f"{domain.name}-agent-${{bundle.target}}"

    dashboard: dict[str, Any] = {
        "datasets": [
            _dashboard_dataset(
                "eval_history",
                "RAG eval score history",
                f"""
                SELECT metric, score, timestamp
                FROM {eval_table_ref}
                WHERE domain = '{domain.name}' AND target = '${{bundle.target}}'
                  AND metric LIKE 'rag_eval.%'
                ORDER BY timestamp
                """,
            ),
            _dashboard_dataset(
                "latest_scores",
                "Latest RAG eval scores",
                f"""
                SELECT metric, score, timestamp
                FROM (
                  SELECT metric, score, timestamp,
                         ROW_NUMBER() OVER (PARTITION BY metric ORDER BY timestamp DESC) AS rn
                  FROM {eval_table_ref}
                  WHERE domain = '{domain.name}' AND target = '${{bundle.target}}'
                    AND metric LIKE 'rag_eval.%'
                )
                WHERE rn = 1
                ORDER BY metric
                """,
            ),
            _dashboard_dataset(
                "endpoint_traffic",
                "Serving endpoint traffic",
                f"""
                SELECT DATE(u.request_time) AS request_date, COUNT(*) AS request_count
                FROM system.serving.endpoint_usage u
                JOIN system.serving.served_entities e ON u.served_entity_id = e.served_entity_id
                WHERE e.endpoint_name = '{endpoint_name}'
                GROUP BY DATE(u.request_time)
                ORDER BY request_date
                """,
            ),
        ],
        "pages": [
            {
                "name": "observability",
                "displayName": "Observability",
                "pageType": "PAGE_TYPE_CANVAS",
                "layout": [
                    {
                        "widget": {
                            "name": "eval_history_chart",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "eval_history",
                                        "fields": [
                                            {"name": "timestamp", "expression": "`timestamp`"},
                                            {"name": "score", "expression": "`score`"},
                                            {"name": "metric", "expression": "`metric`"},
                                        ],
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "line",
                                "encodings": {
                                    "x": {
                                        "fieldName": "timestamp",
                                        "displayName": "Timestamp",
                                        "scale": {"type": "temporal"},
                                    },
                                    "y": {
                                        "fieldName": "score",
                                        "displayName": "Score",
                                        "scale": {"type": "quantitative"},
                                    },
                                    "color": {
                                        "fieldName": "metric",
                                        "displayName": "Metric",
                                        "scale": {"type": "categorical"},
                                    },
                                },
                            },
                        },
                        "position": {"x": 0, "y": 0, "width": 6, "height": 6},
                    },
                    {
                        "widget": {
                            "name": "latest_scores_table",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "latest_scores",
                                        "fields": [
                                            {"name": "metric", "expression": "`metric`"},
                                            {"name": "score", "expression": "`score`"},
                                            {"name": "timestamp", "expression": "`timestamp`"},
                                        ],
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "table",
                                "encodings": {
                                    "columns": [
                                        {"fieldName": "metric", "displayName": "Metric"},
                                        {"fieldName": "score", "displayName": "Score"},
                                        {"fieldName": "timestamp", "displayName": "Last run"},
                                    ]
                                },
                            },
                        },
                        "position": {"x": 0, "y": 6, "width": 6, "height": 6},
                    },
                    {
                        "widget": {
                            "name": "endpoint_traffic_chart",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "endpoint_traffic",
                                        "fields": [
                                            {
                                                "name": "request_date",
                                                "expression": "`request_date`",
                                            },
                                            {
                                                "name": "request_count",
                                                "expression": "`request_count`",
                                            },
                                        ],
                                        "disaggregated": True,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 3,
                                "widgetType": "bar",
                                "encodings": {
                                    "x": {
                                        "fieldName": "request_date",
                                        "displayName": "Date",
                                        "scale": {"type": "temporal"},
                                    },
                                    "y": {
                                        "fieldName": "request_count",
                                        "displayName": "Requests",
                                        "scale": {"type": "quantitative"},
                                    },
                                },
                            },
                        },
                        "position": {"x": 0, "y": 12, "width": 6, "height": 6},
                    },
                ],
            }
        ],
    }
    return json.dumps(dashboard, indent=2)


def _observability_dashboard_resource(domain: Domain) -> dict[str, Any] | None:
    """The `<name>_observability` AI/BI dashboard: RAG eval score history
    and latest scores from agent_factory.eval_results, and serving
    endpoint request volume from the system.serving system tables
    (confirmed queryable on this Free Edition workspace, see docs/PLAN.md
    notes, phase 3). Embedded as serialized_dashboard rather than a
    file_path-referenced .lvdash.json for the same reason as the Genie
    space's serialized_space: the queries need ${var.catalog}/
    ${resources...}/${bundle.target} substitution, which file_path content
    does not get. Only emitted for a domain with a RAG agent: the
    endpoint-traffic query needs build_endpoint_name's <domain>-agent-
    <target> naming, which only exists once deploy_agent_<name> is."""
    if domain.rag is None or not domain.rag.enabled:
        return None
    return {
        f"{domain.name}_observability": {
            "display_name": f"{domain.display_name}: observability",
            "warehouse_id": "${var.warehouse_id}",
            "serialized_dashboard": LiteralStr(_dashboard_json(domain)),
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

    vector_search_index = _vector_search_index_resource(domain)
    if vector_search_index is not None:
        resources["vector_search_indexes"] = vector_search_index

    deploy_agent_job = _deploy_agent_job_resource(domain)
    if deploy_agent_job is not None:
        resources["jobs"].update(deploy_agent_job)

    rag_eval_job = _rag_eval_job_resource(domain)
    if rag_eval_job is not None:
        resources["jobs"].update(rag_eval_job)

    eval_regression_alert = _eval_regression_alert_resource(domain)
    if eval_regression_alert is not None:
        resources["alerts"] = eval_regression_alert

    observability_dashboard = _observability_dashboard_resource(domain)
    if observability_dashboard is not None:
        resources["dashboards"] = observability_dashboard

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
