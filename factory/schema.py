"""Pydantic model for the domain.yml contract.

This is the only file a consumer of the factory needs to understand: it
defines every field `factory/generate.py` reads when it turns a domain
description into bundle resources. See docs/DESIGN.md section 3 for the
annotated example this model implements.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

REPO_ROOT = Path(__file__).resolve().parent.parent
DOMAIN_SCHEMA_PATH = REPO_ROOT / "domains" / "domain.schema.json"


class StructuredTable(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    primary_key: str | list[str]
    description: str | None = None


class StructuredRelationship(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_: str = Field(alias="from")
    to: str


class StructuredConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    loader: Literal["parquet_folder", "csv_folder", "sql", "custom"]
    source: str
    tables: list[StructuredTable]
    relationships: list[StructuredRelationship] = Field(default_factory=list)


class DocumentSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    title: str
    category: str


class DocumentsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    loader: Literal["url_list", "volume_folder", "none"]
    sources: list[DocumentSource] = Field(default_factory=list)
    embedding_model: str


class GenieConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    instructions: str = ""
    sample_questions: list[str] = Field(default_factory=list)
    benchmarks: str | None = None


class RetrieverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    top_k: int = Field(default=6, gt=0)
    filter_columns: list[str] = Field(default_factory=list)


class RagToolsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    genie: bool = False


class RagEvalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    judges: list[str] = Field(default_factory=list)
    promotion_threshold: dict[str, float] = Field(default_factory=dict)


class RagConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    llm: str
    system_prompt: str = ""
    retriever: RetrieverConfig = Field(default_factory=RetrieverConfig)
    tools: RagToolsConfig = Field(default_factory=RagToolsConfig)
    eval: RagEvalConfig


class Domain(BaseModel):
    """The full contract of a `domains/<name>/domain.yml` file."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    display_name: str
    description: str
    owner: str
    tags: dict[str, str] = Field(default_factory=dict)

    structured: StructuredConfig
    documents: DocumentsConfig | None = None
    genie: GenieConfig | None = None
    rag: RagConfig | None = None


def load_domain(path: Path) -> Domain:
    """Parse and validate a single domain.yml file."""
    raw = yaml.safe_load(path.read_text())
    return Domain.model_validate(raw)


def discover_domains(domains_dir: Path = REPO_ROOT / "domains") -> list[Path]:
    """Return sorted paths to every domain.yml under `domains/`, deterministically."""
    return sorted(domains_dir.glob("*/domain.yml"))


def export_json_schema(path: Path = DOMAIN_SCHEMA_PATH) -> None:
    """Write the JSON schema for domain.yml so editors can validate it on save."""
    schema = Domain.model_json_schema(by_alias=True)
    path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    export_json_schema()
