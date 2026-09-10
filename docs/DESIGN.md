# agent-factory: design

## 1. Purpose

A reusable Databricks Declarative Automation Bundle that takes a metadata definition of a knowledge domain and produces, per environment:

1. Unity Catalog schema, volumes and tables for the domain's structured data
2. A document ingestion pipeline into a Delta table with chunks
3. A vector search index over those chunks, on a shared endpoint
4. A Genie agent configured with the domain's tables, instructions and sample questions
5. A RAG agent served on a model serving endpoint, with the Genie agent available as a tool
6. An evaluation job that scores the agents against a golden question set
7. A promotion flow in GitHub Actions: PR checks, dev deploy, test deploy with eval gate, prod deploy with approval and rollback path
8. A set of bundle quality gates that run before any deploy

The consumer's job is to write one `domain.yml`, add data and an eval set, and push.

## 2. Demo domain: Formula 1

The demo domain is motorsport, specifically Formula 1 from 2020 onwards. It was chosen because it has two naturally different knowledge sources that map cleanly onto the two agent types:

- **Structured data** (races, results, drivers, constructors, qualifying, pit stops, lap times) from the open Jolpica/Ergast API. This feeds the Genie agent. Questions like "who had the most podiums in 2025" or "average pit stop time by team at Silverstone" are exactly the shape Genie handles well and demos well.
- **Documents** (FIA Sporting Regulations, Technical Regulations, Financial Regulations, published as PDFs) for the RAG agent. Questions like "what is the minimum car weight" or "what happens if a team exceeds the cost cap" need retrieval over long documents, not SQL.
- **Combined questions** ("did any driver breach the track limits rule more than three times at Austria, and what is the penalty") show the RAG agent calling Genie as a tool.

It is public, well understood by a technical audience, politically neutral, has no overlap with client work, and nobody is tired of it on LinkedIn. Data is snapshotted into `domains/f1/data/` as Parquet so the repo works offline and the ingestion job does not depend on a third-party API being up. A refresh script pulls new seasons on demand. Regulation PDFs are downloaded by the ingestion job from the FIA public URLs listed in `domain.yml` and stored in a volume; they are not committed.

Naming: the domain is called `f1` in metadata. The repo name and README avoid using "Formula 1" or "F1" in a way that implies affiliation.

A second, deliberately small domain is added late in the plan to prove the factory is not special-cased for the demo (see PLAN.md phase 6).

## 3. The domain contract

`domains/<name>/domain.yml` is the only file a consumer must write. Everything else is derived.

```yaml
name: f1
display_name: Formula 1 Analyst
description: >
  Answers questions about Formula 1 results since 2020 and the FIA
  regulations that govern the sport.
owner: stephen@example.com
tags:
  cost_centre: demo
  data_classification: public

structured:
  loader: parquet_folder          # parquet_folder | csv_folder | sql | custom
  source: data/                   # relative to the domain folder
  tables:
    - name: races
      primary_key: race_id
      description: One row per Grand Prix
    - name: results
      primary_key: [race_id, driver_id]
      description: Finishing position, points and status per driver per race
    - name: drivers
      primary_key: driver_id
    - name: constructors
      primary_key: constructor_id
    - name: pit_stops
      primary_key: [race_id, driver_id, stop]
  relationships:                  # used to build Genie join hints
    - from: results.race_id
      to: races.race_id
    - from: results.driver_id
      to: drivers.driver_id

documents:
  loader: url_list                # url_list | volume_folder | none
  sources:
    - url: https://www.fia.com/system/files/documents/fia_2026_f1_regulations_-_section_b_sporting_-_iss_05_-_2026-02-27.pdf
      title: 2026 Sporting Regulations
      category: sporting
    - url: https://www.fia.com/system/files/documents/fia_2026_f1_regulations_-_section_c_technical_-_iss_17_-_2026-04-28.pdf
      title: 2026 Technical Regulations
      category: technical
  embedding_model: databricks-gte-large-en

genie:
  enabled: true
  instructions: |
    Points are awarded 25-18-15-12-10-8-6-4-2-1 for positions 1 to 10.
    A podium is a finish in positions 1 to 3.
    "Status" values other than "Finished" or "+N Laps" mean the driver retired.
  sample_questions:
    - Who scored the most points in 2025?
    - Which constructor had the fastest average pit stop at Silverstone in 2024?
    - How many races did each driver retire from in 2023?
  benchmarks: eval/genie.jsonl    # question, expected SQL or expected answer

rag:
  enabled: true
  llm: databricks-gpt-oss-120b   # any chat-task serving endpoint name; no Claude models
                                  # are available as Databricks foundation models on
                                  # Free Edition, see docs/PLAN.md notes, phase 2
  system_prompt: |
    You answer questions about Formula 1 regulations. Cite the article
    number you relied on. If the question is about results or statistics,
    call the Genie tool rather than guessing.
  retriever:
    top_k: 6
    filter_columns: [category]
  tools:
    genie: true                   # expose the Genie agent as a tool
  eval:
    dataset: eval/rag.jsonl       # question, expected_answer, expected_sources
    judges: [correctness, groundedness, relevance]
    promotion_threshold:
      correctness: 0.80
      groundedness: 0.85
```

`factory/schema.py` holds the Pydantic model for this file and exports a JSON schema to `domains/domain.schema.json` so editors get validation.

## 4. What the generator produces

`factory/generate.py` reads every `domains/*/domain.yml` and writes `resources/generated/<name>.yml` containing:

| Resource | Key pattern | Notes |
|---|---|---|
| `schemas` | `<name>` | one per domain, catalog from `${var.catalog}` |
| `volumes` | `<name>_raw` | documents and seed files |
| `jobs` | `ingest_<name>` | loads structured tables, downloads and chunks documents |
| `vector_search_indexes` | `<name>_chunks` | delta sync, triggered in dev/test, continuous allowed only in prod |
| `genie_spaces` | `<name>_genie` | `serialized_space` (inline), not `file_path` (see below) |
| `jobs` | `deploy_agent_<name>` | logs and registers the RAG agent in UC as the `candidate` alias; does not touch the serving endpoint (see below) |
| `jobs` | `evaluate_genie_<name>` | runs the Genie benchmark questions, writes results table, fails below threshold |
| `jobs` | `evaluate_<name>` | runs MLflow evaluation for the RAG agent, writes results table, fails below threshold |
| `alerts` | `<name>_eval_regression` | fires if latest eval score drops below threshold |
| `dashboards` | `<name>_observability` | eval history, retrieval stats, endpoint traffic |

Shared resources in `resources/core/` (hand-written): the vector search endpoint, a shared `agent_factory` schema for eval results and run metadata, a SQL warehouse reference variable.

The generator also renders the Genie agent JSON from the `genie` block plus the table list, embedded directly into `resources/generated/<name>.yml` as `serialized_space` rather than written to a separate `domains/<name>/genie/<name>.geniespace.json` and referenced via `file_path`: content behind `file_path` is uploaded verbatim with no `${var...}`/`${resources...}` substitution, which breaks table identifiers that need the target's resolved catalog and (in dev mode) prefixed schema name. `serialized_space` is a normal YAML string field, so it gets substituted like any other. The Genie space create/update API also requires `data_sources.tables` sorted by identifier, which the generator does before emitting. The generator writes the eval datasets into the domain's schema on first ingest.

Document parsing and chunking uses Databricks' native `ai_parse_document` and `ai_prep_search` SQL functions rather than a hand-rolled splitter: `ai_parse_document` extracts layout-aware elements (text, tables, headers) from the downloaded PDF, and `ai_prep_search` turns that into embedding-ready chunks with document context (title, section header, page number) baked into each chunk's text. Confirmed working against a real Free Edition workspace on the actual FIA regulation PDFs while building phase 2 (see docs/PLAN.md notes). Because of this, `documents.chunking` (`strategy`/`chunk_size`/`chunk_overlap`) was dropped from the domain.yml contract: neither function takes a configurable chunk size or overlap, so those knobs had nothing to control. `documents.embedding_model` remains: it names the model serving endpoint the vector search index's delta sync uses to compute embeddings from each chunk, which is a separate step from parsing/chunking.

There is deliberately no bundle-declared `model_serving_endpoints` resource, even though CLAUDE.md lists it among the resource types this project depends on: the endpoint is created and updated directly via the Databricks SDK instead (`src/agent/deploy.py`, `update_serving_endpoint`), but only called from `src/agent/promote.py`, never from `deploy_agent_<name>` itself — see this section's `evaluate_<name>` and `deploy_agent_<name>` rows and the next paragraph for why. Two independent, both real, reasons for owning the endpoint outside the bundle at all. First, a genuine circular dependency: the endpoint needs the domain's `rag_agent` model to already exist, which it doesn't until `deploy_agent_<name>` runs, so the bundle resource fails on a fresh deploy exactly like the Genie space and vector search index do at first — except the job itself also referenced the endpoint's resolved name in its parameters, and the direct engine treats any `${resources...}` reference as a hard dependency (even to a field, like a name, that's statically known ahead of the resource actually deploying), so the *job resource's own creation* ended up depending on the endpoint having already deployed successfully. Neither could ever be created on a fresh deploy. Second, even once bootstrapped, Databricks serving endpoints only accept a numeric `entity_version` for a served entity, never an alias, and this generator has no live workspace access at generate time to know the real latest version number — a documented Databricks Asset Bundles limitation, not specific to this project. A bundle-declared endpoint could only ever pin version `"1"`; real promotion has to update `entity_version` directly via the SDK regardless, so owning the endpoint there entirely, rather than half in the bundle and half out of it, is the more honest design. `deploy_agent_<name>` names the endpoint `<domain>-agent-<target>` (not just `<domain>-agent`): dev/test/prod may share a single workspace on Free Edition (see docs/PLAN.md phase 0 notes), so the target has to disambiguate the name itself rather than relying on DAB's dev-mode name prefixing, which doesn't apply to a resource the bundle never declares.

`deploy_agent_<name>` only registers the RAG agent and aliases it `candidate`; it does not call `update_serving_endpoint`. An earlier version did, immediately pointing the live endpoint at whatever version had just been registered. Running phase 3's deliberately-bad-system-prompt scenario for real exposed this as a genuine bug rather than a theoretical one: `evaluate_<name>` correctly failed against the bad candidate and `promote` was correctly skipped (the `champion` alias never moved), but the serving endpoint had already been updated to the bad candidate by `deploy_agent_<name>` moments earlier and nothing ever pointed it back, so the live endpoint kept serving a version that had just failed evaluation. That directly contradicts section 6's CI/CD flow, "the serving endpoint alias is only moved after eval passes". The fix is to only ever call `update_serving_endpoint` from `src/agent/promote.py`, gated the same way the `champion` alias move already is: by the `promote` task's `depends_on` the `evaluate` task succeeding. This also means a domain's serving endpoint does not exist at all until its first successful `evaluate_<name>` run, not from the moment `deploy_agent_<name>` first runs; `update_serving_endpoint`'s create-or-update branching already handled this correctly regardless of which job called it. See docs/PLAN.md notes, phase 3.

`mlflow.pyfunc.log_model`'s `resources` argument (`src/agent/deploy.py`, `build_resources`) has to declare more than the tools it looks like the agent uses. Declaring only `DatabricksGenieSpace` is not enough to make the Genie tool actually work when called from inside the served model: a real deploy could authenticate the conversation-start call fine but every query Genie then tried to run failed with `PERMISSION_DENIED`, "No access to" every one of the domain's tables. The served identity separately needs `DatabricksSQLWarehouse` (the warehouse backing the Genie space) and a `DatabricksTable` per structured table declared too, or the automatic auth passthrough for those never gets set up. `build_resources` declares all of them together whenever the Genie tool is enabled.

`<name>_eval_regression` is an `AlertV2` resource (`evaluation`/`schedule`/`query_text`/`warehouse_id`, confirmed against the CLI's own JSON schema while building phase 3). `AlertV2Evaluation` compares exactly one numeric `source` column against one `threshold`, so the generator collapses however many judges the domain configures a `rag.eval.promotion_threshold` for into a single query: a `VALUES` table built from that mapping joined against each judge's latest `agent_factory.eval_results` row, returning `MIN(score - threshold) AS margin`, with the alert firing on `margin < 0`. `empty_result_state` is set to `OK` (not the default `UNKNOWN`, which the SDK's own schema warns is being deprecated) so the alert does not fire before `evaluate_<name>` has ever run on that target. Confirmed against a real Free Edition deploy: the alert's stored `query_text` (read back via `GET /api/2.0/alerts/<id>`) has `${var.catalog}` and every `${resources...}` reference fully resolved to real names, even though `databricks bundle summary`'s own JSON output displays those same references unresolved for both this alert and pre-existing job task parameters, which is a `bundle summary` display quirk rather than anything left unresolved in what actually gets deployed. Not emitted for a domain with no RAG agent or no configured thresholds. See docs/PLAN.md notes, phase 3.

The generator is idempotent and deterministic. CI runs it and diffs against the committed output.

## 5. Bundle targets

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: agent_factory_dev
      vs_sync: TRIGGERED
  test:
    mode: production
    variables:
      catalog: agent_factory_test
      vs_sync: TRIGGERED
    run_as:
      service_principal_name: ${var.test_sp}
  prod:
    mode: production
    variables:
      catalog: agent_factory
      vs_sync: TRIGGERED        # consumers may switch to CONTINUOUS
    run_as:
      service_principal_name: ${var.prod_sp}
```

Dev uses development mode so each engineer gets prefixed resources and the deploy is fast. Test and prod use production mode and run as a service principal.

`workspace.host` is deliberately absent from every target: `databricks bundle validate` refuses variable interpolation on fields that configure authentication, and CLAUDE.md forbids committing workspace URLs. Each target's host is resolved from the environment instead, either a matching profile in `~/.databrickscfg` locally, or the `DATABRICKS_HOST` environment variable set by the OIDC step in CI. Discovered against CLI 1.16.0 while validating phase 0; re-check if a future CLI version allows this.

In the demo, test and prod can be two catalogs in the same workspace. Consumers with separate workspaces change which profile or `DATABRICKS_HOST` they deploy with, not the bundle YAML.

## 6. CI/CD in GitHub Actions

Auth is GitHub OIDC federated to a Databricks service principal per target. No PATs.

This needs a paid workspace with account-level admin. Databricks Free Edition has no account console or account-level APIs, so it cannot create service principals at all; confirmed while deploying phase 0 (see docs/PLAN.md notes, 2026-09-10). On Free Edition, phase 5 falls back to authenticating as a user (OAuth U2M) instead of a service principal, and `test`/`prod` are catalogs in the one available workspace rather than separate workspaces.

**On pull request**

1. `uv run ruff check`, `uv run pytest`
2. `python -m factory.generate` then `git diff --exit-code resources/generated` (generated output must be committed)
3. `databricks bundle validate -t test`
4. `databricks bundle plan -t test -o json` piped into the quality gates; any `fail` blocks the PR
5. Plan summary posted as a PR comment

**On merge to main**

1. Deploy to `test`
2. Run `ingest_<domain>` for each domain (incremental, so cheap after the first run)
3. Run `deploy_agent_<domain>` then `evaluate_<domain>`
4. If evaluation fails threshold, the workflow stops and the previous test deployment stays live (the serving endpoint alias is only moved after eval passes)
5. Tag the commit `test-passed-<sha>`

**Promotion to prod**

1. `workflow_dispatch` or environment approval on the `prod` GitHub Environment
2. Deploy to `prod`, run deploy agent, run evaluate again against prod data
3. Move the `champion` alias on the registered model only after prod eval passes
4. Rollback is `databricks bundle deploy` of the previous tag plus moving the alias back; a `rollback.yml` workflow does both from a tag input

## 7. Quality gates

`gates/` is a standalone Python package that reads the JSON output of `databricks bundle plan` (and the resolved config from `bundle validate -o json`) and runs rules. Each rule returns pass, warn or fail with a message pointing at the resource key. It is exposed as a composite GitHub Action in `.github/actions/bundle-gates` so other repos can use it with one step.

Initial rule set:

| Rule | Severity | What it checks |
|---|---|---|
| `no_all_purpose_clusters` | fail | No `existing_cluster_id` and no `clusters` resources; jobs use serverless or job clusters |
| `required_tags` | fail | Every job and endpoint carries `owner` and `cost_centre` tags |
| `naming_convention` | fail | Resource keys are `snake_case`, endpoint names are `kebab-case`, all prefixed with the domain name |
| `prod_no_dev_mode` | fail | `prod` and `test` targets are `mode: production` |
| `genie_no_permissions` | fail | No `permissions` block on `genie_spaces` (known platform limitation) |
| `vs_sync_in_nonprod` | fail | Vector search indexes are `TRIGGERED` outside prod |
| `no_secrets_in_yaml` | fail | Regex scan for token, key and URL-with-credential patterns |
| `job_has_test` | warn | Each job's entry point has a matching file under `tests/` |
| `endpoint_scale_to_zero` | warn | Serving endpoints set `scale_to_zero_enabled: true` outside prod |
| `eval_threshold_present` | fail | Every domain with `rag.enabled` defines promotion thresholds |

Rules are plain Python classes with a `check(config, plan) -> list[Finding]` method. Adding a rule is one file plus two fixtures.

## 8. Evaluation and promotion

Evaluation uses MLflow's GenAI evaluation with built-in judges plus a domain-specific exact-match judge for Genie SQL benchmarks. Results land in `agent_factory.eval_results` with columns for domain, target, git sha, metric, score, timestamp. The evaluate job exits non-zero if any metric is below its threshold. The observability dashboard reads this table; the alert watches it.

The RAG agent's retriever tool (`_call_retriever` in `src/agent/rag_agent.py`) has to return structured rows from its `RETRIEVER`-typed span, not a pre-joined string: `mlflow.genai`'s retrieval scorers (`RetrievalGroundedness`, `RetrievalRelevance`, `RetrievalSufficiency`) extract retrieval context by reading a RETRIEVER span's captured return value directly, and only recognise a list of dicts carrying a `page_content`/`content`/`text` key, never a formatted string. Confirmed by a real `evaluate_<name>` run: with the span returning formatted text, `result.metrics` had no `retrieval_groundedness/mean` key at all, not a low score, an absent one, so `check_thresholds` treated it as an automatic failure regardless of the configured threshold. `_run_tool` formats the structured rows into text afterwards, for the tool-call response the LLM actually sees.

`RetrievalGroundedness` only checks a claim against the RETRIEVER span's context, never any other tool's. For a domain with `rag.tools.genie: true`, a "combined" question whose answer correctly cites a Genie-derived structured fact (a race result, a points total) alongside a regulation fact cannot be fully grounded by this judge even when both halves of the answer are correct: only the regulation half has a matching RETRIEVER span to check against. Confirmed against a real evaluate run on the f1 domain's eval set: every combined-category question that correctly cited both a Genie stat and a regulation scored `groundedness: no`, dragging the domain's mean groundedness below what a documents-only domain could reach. There is no per-category threshold in the domain.yml contract to work around this, so `promotion_threshold` should be set from a real baseline evaluate run for any domain enabling the Genie tool, not chosen upfront: f1's own thresholds (see `domains/f1/domain.yml`) were set this way. A future phase could add a Genie-aware groundedness scorer; out of scope here.

The RAG agent is written with the MLflow ResponsesAgent interface, logged with `mlflow.pyfunc.log_model`, registered in Unity Catalog as `<catalog>.<name>.rag_agent`, and served with the `champion` alias. Promotion means moving the alias, which means rollback is instant and does not require redeploying anything.

## 9. Template packaging

Once stable, the repo doubles as a custom bundle template: `databricks bundle init https://github.com/<user>/agent-factory` asks for a domain name and catalog and produces a ready-to-run bundle with an empty domain folder and the F1 domain as an example. The `databricks_template_schema.json` and templated files live at the repo root alongside the working project so the template and the reference implementation cannot drift.

## 10. Out of scope

- Multi-workspace Unity Catalog sharing
- Fine-tuning
- Any UI beyond Genie, Playground and the observability dashboard
- Non-Azure clouds (should work but is not tested)

## 11. Content plan (for reference)

Posts this repo should produce, in order:

1. The launch: one YAML file to a governed agent, with the demo
2. What broke moving to the direct deployment engine and Genie agents in bundles
3. Evaluation as a promotion gate: stopping a worse agent reaching prod
4. Quality gates for bundles as a reusable GitHub Action
5. Adding a second domain in an afternoon, and what that proved
6. Reflective piece: what a "factory" actually means for an AI platform team
