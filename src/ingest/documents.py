"""Downloads a domain's regulation PDFs, parses and chunks them, and
writes the result to <schema>.chunks for the vector search index to sync
from.

Runs as a Databricks job (spark_python_task, serverless). Parsing and
chunking use Databricks' native ai_parse_document and ai_prep_search SQL
functions (see docs/DESIGN.md section 4), which only run on a real
Databricks workspace, so the SQL that calls them can only be unit-tested
as a string (parse_and_chunk_sql); download_all and enrich_chunks are
directly testable, and ingest_documents itself is exercised by actually
deploying and running the job. Keep this file thin: see CLAUDE.md, "Keep
notebooks thin."
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, element_at, split

# See src/ingest/structured.py for why this sys.path bootstrap has to run
# before the sibling-package import that follows.
try:
    _REPO_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    _REPO_ROOT = Path(sys.argv[0]).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingest.delta import write_delta_table  # noqa: E402


def _default_repo_root() -> Path:
    return _REPO_ROOT


def load_documents_config(domain_yml_path: Path) -> dict | None:
    """Read the documents block straight out of domain.yml, without
    depending on factory.schema, so this job has no import-path
    dependency on the factory package at runtime."""
    raw = yaml.safe_load(domain_yml_path.read_text())
    return raw.get("documents")


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def source_filename(source: dict[str, str]) -> str:
    extension = Path(urlparse(source["url"]).path).suffix or ".pdf"
    return f"{slugify(source['title'])}{extension}"


def download_source(url: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310
        dest_path.write_bytes(response.read())


def download_all(sources: list[dict[str, str]], dest_dir: Path) -> list[dict[str, str]]:
    """Downloads every source into dest_dir and returns the source list
    with a `filename` key added, for joining chunks back to their title
    and category later."""
    manifest = []
    for source in sources:
        filename = source_filename(source)
        download_source(source["url"], dest_dir / filename)
        manifest.append({**source, "filename": filename})
    return manifest


def volume_documents_dir(catalog: str, schema: str, volume_name: str) -> str:
    return f"/Volumes/{catalog}/{schema}/{volume_name}/documents"


def parse_and_chunk_sql(documents_dir: str) -> str:
    """Layout-aware parsing (ai_parse_document) followed by semantic,
    context-enriched chunking (ai_prep_search) for every file in
    documents_dir. Chunking is not configurable (no chunk_size/overlap):
    both functions manage that internally. See docs/DESIGN.md section 4."""
    return f"""
WITH parsed AS (
  SELECT path, ai_parse_document(content) AS p
  FROM READ_FILES('{documents_dir}', format => 'binaryFile')
),
prepped AS (
  SELECT path, ai_prep_search(p) AS result FROM parsed
)
SELECT
  path,
  chunk.value:chunk_id::STRING AS chunk_id,
  chunk.value:chunk_position::INT AS chunk_position,
  chunk.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
  chunk.value:chunk_to_embed::STRING AS chunk_to_embed
FROM prepped, LATERAL variant_explode(prepped.result:document.contents) AS chunk
""".strip()


def enrich_chunks(
    spark: SparkSession, raw_chunks: DataFrame, manifest: list[dict[str, str]]
) -> DataFrame:
    """Joins ai_prep_search's chunk rows (keyed by source file path) back
    to the title/category/url each source was configured with in
    domain.yml, and renames chunk_to_retrieve to content."""
    manifest_df = spark.createDataFrame(
        [(m["filename"], m["title"], m["category"], m["url"]) for m in manifest],
        schema=["filename", "title", "category", "source_url"],
    )
    chunks = raw_chunks.withColumn("filename", element_at(split(col("path"), "/"), -1))
    return chunks.join(manifest_df, on="filename", how="inner").select(
        "chunk_id",
        "title",
        "category",
        "source_url",
        "chunk_position",
        col("chunk_to_retrieve").alias("content"),
        "chunk_to_embed",
    )


def ingest_documents(
    spark: SparkSession,
    domain_name: str,
    catalog: str,
    schema: str,
    volume_name: str,
    repo_root: Path | None = None,
    apply_primary_key_constraint: bool = True,
) -> None:
    domain_dir = (repo_root or _default_repo_root()) / "domains" / domain_name
    documents = load_documents_config(domain_dir / "domain.yml")
    if documents is None or documents["loader"] == "none":
        return
    if documents["loader"] != "url_list":
        raise NotImplementedError(f"documents.loader {documents['loader']!r} is not supported")

    documents_dir = volume_documents_dir(catalog, schema, volume_name)
    manifest = download_all(documents["sources"], Path(documents_dir))

    raw_chunks = spark.sql(parse_and_chunk_sql(documents_dir))
    chunks_df = enrich_chunks(spark, raw_chunks, manifest)

    write_delta_table(
        spark,
        chunks_df,
        f"{catalog}.{schema}.chunks",
        table_name="chunks",
        primary_key=["chunk_id"],
        comment="Chunked regulation documents for RAG retrieval.",
        table_properties={"delta.enableChangeDataFeed": "true"},
        apply_primary_key_constraint=apply_primary_key_constraint,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, help="Domain name, e.g. f1")
    parser.add_argument("--catalog", required=True, help="Destination Unity Catalog catalog")
    parser.add_argument(
        "--schema", required=True, help="Destination schema (resolved, may be dev-prefixed)"
    )
    parser.add_argument(
        "--volume-name", required=True, help="Raw volume name (resolved, may be dev-prefixed)"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ingest_documents(spark, args.domain, args.catalog, args.schema, args.volume_name)


if __name__ == "__main__":
    main()
