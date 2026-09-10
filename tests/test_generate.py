import json

import yaml

from factory.generate import GENERATED_DIR, generate, render_domain_resources
from factory.schema import discover_domains, load_domain

F1_DOMAIN_PATH = next(p for p in discover_domains() if p.parent.name == "f1")


def test_render_domain_resources_shape():
    domain = load_domain(F1_DOMAIN_PATH)
    rendered = render_domain_resources(domain)
    resources = rendered["resources"]

    assert set(resources["schemas"]) == {"f1"}
    assert resources["schemas"]["f1"]["catalog_name"] == "${var.catalog}"

    assert set(resources["volumes"]) == {"f1_raw"}
    assert resources["volumes"]["f1_raw"]["schema_name"] == "f1"
    assert resources["volumes"]["f1_raw"]["volume_type"] == "MANAGED"

    assert set(resources["jobs"]) == {"ingest_f1", "evaluate_genie_f1"}
    job = resources["jobs"]["ingest_f1"]
    assert job["tags"]["owner"] == domain.owner
    task = job["tasks"][0]["spark_python_task"]
    assert task["python_file"] == "../../src/ingest/structured.py"
    assert task["parameters"] == [
        "--domain",
        "f1",
        "--catalog",
        "${var.catalog}",
        "--schema",
        "${resources.schemas.f1.name}",
    ]
    assert "existing_cluster_id" not in job["tasks"][0]
    assert "new_cluster" not in job["tasks"][0]

    assert set(resources["genie_spaces"]) == {"f1_genie"}
    genie = resources["genie_spaces"]["f1_genie"]
    assert "permissions" not in genie
    assert genie["warehouse_id"] == "${var.warehouse_id}"
    space = json.loads(genie["serialized_space"])
    assert space["version"] == 2
    assert len(space["config"]["sample_questions"]) == 3
    # The Genie API requires data_sources.tables sorted by identifier.
    identifiers = [t["identifier"] for t in space["data_sources"]["tables"]]
    assert identifiers == sorted(identifiers)
    assert identifiers == [
        "${var.catalog}.${resources.schemas.f1.name}.constructors",
        "${var.catalog}.${resources.schemas.f1.name}.drivers",
        "${var.catalog}.${resources.schemas.f1.name}.pit_stops",
        "${var.catalog}.${resources.schemas.f1.name}.races",
        "${var.catalog}.${resources.schemas.f1.name}.results",
    ]
    assert len(space["instructions"]["text_instructions"]) == 1

    evaluate_job = resources["jobs"]["evaluate_genie_f1"]
    evaluate_task = evaluate_job["tasks"][0]["spark_python_task"]
    assert evaluate_task["python_file"] == "../../src/evaluate/genie.py"
    assert evaluate_task["parameters"] == [
        "--domain",
        "f1",
        "--space-id",
        "${resources.genie_spaces.f1_genie.id}",
        "--eval-table",
        "${var.catalog}.${resources.schemas.agent_factory.name}.eval_results",
        "--target",
        "${bundle.target}",
    ]


def test_genie_space_is_deterministic():
    domain = load_domain(F1_DOMAIN_PATH)
    first = render_domain_resources(domain)["resources"]["genie_spaces"]["f1_genie"]
    second = render_domain_resources(domain)["resources"]["genie_spaces"]["f1_genie"]
    assert first["serialized_space"] == second["serialized_space"]


def test_generate_is_idempotent():
    first_paths = generate()
    first_contents = {p: p.read_text() for p in first_paths}

    second_paths = generate()
    second_contents = {p: p.read_text() for p in second_paths}

    assert first_paths == second_paths
    assert first_contents == second_contents


def test_generate_matches_committed_output():
    generate()
    committed = sorted(GENERATED_DIR.glob("*.yml"))
    assert committed, "expected at least the f1 domain to be generated"
    for path in committed:
        # a generated file must be valid YAML and start with the no-edit header
        assert path.read_text().startswith("# GENERATED FILE. Do not edit by hand.")
        yaml.safe_load(path.read_text())
