import pytest
from pydantic import ValidationError

from factory.schema import Domain, discover_domains, load_domain

F1_DOMAIN_PATH = next(p for p in discover_domains() if p.parent.name == "f1")


def test_f1_domain_loads():
    domain = load_domain(F1_DOMAIN_PATH)
    assert domain.name == "f1"
    assert domain.structured.loader == "parquet_folder"
    assert {t.name for t in domain.structured.tables} == {
        "races",
        "results",
        "drivers",
        "constructors",
        "pit_stops",
    }


def test_f1_domain_has_genie_documents_and_rag():
    domain = load_domain(F1_DOMAIN_PATH)

    assert domain.genie is not None
    assert domain.genie.enabled is True
    assert len(domain.genie.sample_questions) == 3

    assert domain.documents is not None
    assert domain.documents.loader == "url_list"
    assert len(domain.documents.sources) == 3
    assert {s.category for s in domain.documents.sources} == {"sporting", "technical", "financial"}

    assert domain.rag is not None
    assert domain.rag.enabled is True
    assert domain.rag.tools.genie is True


def test_discover_domains_is_sorted_and_deterministic():
    first = discover_domains()
    second = discover_domains()
    assert first == second
    assert first == sorted(first)


def test_unknown_field_is_rejected():
    raw = {
        "name": "bad",
        "display_name": "Bad",
        "description": "x",
        "owner": "a@b.com",
        "structured": {
            "loader": "parquet_folder",
            "source": "data/",
            "tables": [{"name": "t", "primary_key": "id"}],
        },
        "not_a_real_field": True,
    }
    with pytest.raises(ValidationError):
        Domain.model_validate(raw)


def test_invalid_loader_is_rejected():
    raw = {
        "name": "bad",
        "display_name": "Bad",
        "description": "x",
        "owner": "a@b.com",
        "structured": {
            "loader": "ftp",
            "source": "data/",
            "tables": [{"name": "t", "primary_key": "id"}],
        },
    }
    with pytest.raises(ValidationError):
        Domain.model_validate(raw)


def test_relationship_from_alias():
    raw = {
        "name": "bad",
        "display_name": "Bad",
        "description": "x",
        "owner": "a@b.com",
        "structured": {
            "loader": "parquet_folder",
            "source": "data/",
            "tables": [{"name": "t", "primary_key": "id"}],
            "relationships": [{"from": "t.id", "to": "t.id"}],
        },
    }
    domain = Domain.model_validate(raw)
    assert domain.structured.relationships[0].from_ == "t.id"
