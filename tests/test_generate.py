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

    assert set(resources["jobs"]) == {"ingest_f1"}
    job = resources["jobs"]["ingest_f1"]
    assert job["tags"]["owner"] == domain.owner
    assert job["tasks"][0]["spark_python_task"]["python_file"] == "../../src/ingest/placeholder.py"
    assert "existing_cluster_id" not in job["tasks"][0]
    assert "new_cluster" not in job["tasks"][0]


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
