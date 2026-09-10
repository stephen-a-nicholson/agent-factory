from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.ingest.documents import (
    download_all,
    download_source,
    enrich_chunks,
    load_documents_config,
    parse_and_chunk_sql,
    slugify,
    source_filename,
    sync_vector_search_index,
    volume_documents_dir,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-documents")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_slugify():
    assert slugify("2026 Sporting Regulations") == "2026_sporting_regulations"
    assert slugify("A/B  --  C") == "a_b_c"


def test_source_filename():
    source = {"url": "https://example.com/path/to/doc.PDF", "title": "2026 Sporting Regs"}
    assert source_filename(source) == "2026_sporting_regs.PDF"


def test_volume_documents_dir():
    assert (
        volume_documents_dir("agent_factory_dev", "dev_steph_f1", "f1_raw")
        == "/Volumes/agent_factory_dev/dev_steph_f1/f1_raw/documents"
    )


def test_load_documents_config(tmp_path: Path):
    domain_yml = tmp_path / "domain.yml"
    domain_yml.write_text(
        """
name: f1
display_name: F1
description: test
owner: a@b.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: t
      primary_key: id
documents:
  loader: url_list
  sources:
    - url: https://example.com/a.pdf
      title: A
      category: sporting
  embedding_model: databricks-gte-large-en
"""
    )
    documents = load_documents_config(domain_yml)
    assert documents["loader"] == "url_list"
    assert documents["sources"][0]["title"] == "A"


def test_load_documents_config_missing_block(tmp_path: Path):
    domain_yml = tmp_path / "domain.yml"
    domain_yml.write_text(
        """
name: f1
display_name: F1
description: test
owner: a@b.com
structured:
  loader: parquet_folder
  source: data/
  tables:
    - name: t
      primary_key: id
"""
    )
    assert load_documents_config(domain_yml) is None


def test_download_source_via_file_url(tmp_path: Path):
    src = tmp_path / "src.txt"
    src.write_bytes(b"hello world")
    dest = tmp_path / "downloaded" / "out.txt"

    download_source(src.as_uri(), dest)

    assert dest.read_bytes() == b"hello world"


def test_download_all_builds_manifest_with_filenames(tmp_path: Path):
    src = tmp_path / "src.pdf"
    src.write_bytes(b"%PDF-1.4 fake")
    sources = [{"url": src.as_uri(), "title": "My Doc", "category": "sporting"}]
    dest_dir = tmp_path / "dest"

    manifest = download_all(sources, dest_dir)

    assert manifest == [
        {
            "url": src.as_uri(),
            "title": "My Doc",
            "category": "sporting",
            "filename": "my_doc.pdf",
        }
    ]
    assert (dest_dir / "my_doc.pdf").read_bytes() == b"%PDF-1.4 fake"


def test_parse_and_chunk_sql_shape():
    sql = parse_and_chunk_sql("/Volumes/cat/schema/vol/documents")
    assert "ai_parse_document" in sql
    assert "ai_prep_search" in sql
    assert "/Volumes/cat/schema/vol/documents" in sql
    assert "variant_explode" in sql


def test_enrich_chunks_joins_manifest_metadata(spark):
    raw_chunks = spark.createDataFrame(
        [
            (
                "/Volumes/cat/schema/vol/documents/my_doc.pdf",
                "abc123_0",
                0,
                "raw text",
                "embed text",
            )
        ],
        schema=["path", "chunk_id", "chunk_position", "chunk_to_retrieve", "chunk_to_embed"],
    )
    manifest = [
        {
            "filename": "my_doc.pdf",
            "title": "My Doc",
            "category": "sporting",
            "url": "https://example.com/my_doc.pdf",
        }
    ]

    result = enrich_chunks(spark, raw_chunks, manifest).collect()

    assert len(result) == 1
    row = result[0]
    assert row.chunk_id == "abc123_0"
    assert row.title == "My Doc"
    assert row.category == "sporting"
    assert row.source_url == "https://example.com/my_doc.pdf"
    assert row.content == "raw text"
    assert row.chunk_to_embed == "embed text"


def test_enrich_chunks_drops_unmatched_files(spark):
    raw_chunks = spark.createDataFrame(
        [("/Volumes/cat/schema/vol/documents/unknown.pdf", "x_0", 0, "text", "embed")],
        schema=["path", "chunk_id", "chunk_position", "chunk_to_retrieve", "chunk_to_embed"],
    )
    manifest = [
        {
            "filename": "my_doc.pdf",
            "title": "My Doc",
            "category": "sporting",
            "url": "https://example.com/my_doc.pdf",
        }
    ]

    result = enrich_chunks(spark, raw_chunks, manifest).collect()

    assert result == []


def test_sync_vector_search_index_noop_without_index_name():
    # No index_name means documents.chunking is effectively disabled for
    # this call; should return immediately without even trying to reach
    # a live workspace.
    sync_vector_search_index(None)
    sync_vector_search_index("")
