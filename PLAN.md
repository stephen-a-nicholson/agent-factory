# agent-factory: plan

Work through phases in order. Tick items as they are done and add a one-line note if something diverged from DESIGN.md. Do not start a phase until the previous phase's "done when" is true. Each phase should end with a commit and a short summary in the notes section at the bottom.

## Phase 0: skeleton

- [ ] Init repo with `uv`, `ruff`, `pytest`, `pre-commit` (ruff, yamllint, no-secrets hook)
- [ ] `databricks.yml` with `engine: direct`, three targets, variables for catalog, hosts, service principals, warehouse id
- [ ] `resources/core/` with shared schema `agent_factory`, vector search endpoint, warehouse variable
- [ ] `factory/schema.py` Pydantic model for `domain.yml`; export JSON schema to `domains/domain.schema.json`
- [ ] `factory/generate.py` that reads domains and writes `resources/generated/<name>.yml` with schema, volume and a placeholder ingest job only
- [ ] `domains/f1/domain.yml` with structured block only
- [ ] `tests/` for schema validation and generator determinism
- [ ] `databricks bundle validate -t dev` passes

Done when: `uv run pytest` and `bundle validate` pass and a fresh clone can run `bundle deploy -t dev` creating the schema and volume.

## Phase 1: structured data and Genie

- [ ] `factory/refresh_f1.py` pulls seasons 2020 to current from the Jolpica API and writes Parquet to `domains/f1/data/` (run once, commit output)
- [ ] `src/ingest/structured.py` loads Parquet from the domain folder into Delta tables with primary key constraints and column comments taken from `domain.yml`
- [ ] Generator emits full `ingest_<name>` job (serverless) and renders `domains/<name>/genie/<name>.geniespace.json` from the `genie` block
- [ ] Generator emits `genie_spaces` resource with `file_path` and no permissions block
- [ ] Deploy to dev, run ingest, open the Genie agent, ask the three sample questions, record the results in notes
- [ ] `domains/f1/eval/genie.jsonl` with 15 benchmark questions and expected answers
- [ ] `src/evaluate/genie.py` runs benchmarks via the Genie API and writes results to `agent_factory.eval_results`

Done when: Genie answers all three sample questions correctly in dev and the benchmark job runs end to end.

## Phase 2: documents, vector search, RAG agent

- [ ] `src/ingest/documents.py` downloads the regulation PDFs from `domain.yml` into the volume, parses with `ai_parse_document`, chunks per the config, writes `<schema>.chunks`
- [ ] Generator emits `vector_search_indexes` resource (delta sync, triggered, `${var.vs_sync}`)
- [ ] `src/agent/rag_agent.py` implementing a ResponsesAgent: retriever tool over the index, Genie tool if enabled, system prompt from `domain.yml`
- [ ] `src/agent/deploy.py` logs the model with MLflow, registers in UC, creates or updates the serving endpoint, sets alias `candidate`
- [ ] Generator emits `deploy_agent_<name>` job and `model_serving_endpoints` resource
- [ ] Deploy to dev, run ingest and deploy_agent, test in Playground with a regulations question, a stats question and a combined question; record in notes

Done when: the served agent answers all three question types in dev and cites article numbers for regulation questions.

## Phase 3: evaluation and promotion logic

- [ ] `domains/f1/eval/rag.jsonl` with 25 questions covering sporting, technical, financial regs and combined questions
- [ ] `src/evaluate/rag.py` using MLflow GenAI evaluate with the judges from `domain.yml`, writes to `eval_results`, exits non-zero below threshold
- [ ] `src/agent/promote.py` moves `champion` alias to the evaluated version only on pass
- [ ] Generator emits `evaluate_<name>` job, the regression `alert`, and the observability `dashboard`
- [ ] Run the full chain in dev: ingest, deploy_agent, evaluate, promote

Done when: a deliberately bad system prompt causes evaluate to fail and the champion alias does not move.

## Phase 4: quality gates

- [ ] `gates/` package: `Finding`, `Rule` base class, loader for `bundle validate -o json` and `bundle plan -o json`
- [ ] Implement the ten rules in DESIGN.md section 7, each with pass and fail fixtures under `tests/gates/fixtures/`
- [ ] `python -m gates.run --target test` CLI with text and JSON output and a non-zero exit on any fail
- [ ] `.github/actions/bundle-gates/action.yml` composite action wrapping the CLI
- [ ] README section for the gates showing use from another repo

Done when: all rules have passing tests and the action runs in this repo's PR workflow.

## Phase 5: GitHub Actions end to end

- [ ] Configure OIDC federation for a Databricks service principal per target; document the exact steps in `docs/AUTH.md`
- [ ] `pr.yml`: lint, tests, generate diff check, validate, plan, gates, plan summary comment
- [ ] `main.yml`: deploy test, ingest, deploy agent, evaluate, tag on success
- [ ] `prod.yml`: environment approval, deploy prod, deploy agent, evaluate, promote
- [ ] `rollback.yml`: takes a tag, redeploys it, moves alias back
- [ ] Run a full cycle: PR with a prompt change, merge, watch test eval, approve prod, then roll back

Done when: a PR to main reaches prod through approvals with no manual CLI steps, and rollback restores the previous agent.

## Phase 6: prove reusability

- [ ] Add a second domain with no changes to the generator. Suggested: `uk-rail` using ORR open data for structured (station usage, punctuality) and the National Rail Conditions of Travel PDF for documents. Keep it small.
- [ ] Note every place the generator or gates needed a change to accommodate it; those are bugs in the factory, fix them
- [ ] Time the exercise honestly and record it for the post

Done when: the second domain's agents pass evaluation in test with the same workflows.

## Phase 7: template and release

- [ ] `databricks_template_schema.json` and templated files so `databricks bundle init <repo url>` works
- [ ] README: what it is, 20 minute quickstart, domain contract reference, gates reference, architecture diagram (Mermaid)
- [ ] `CONTRIBUTING.md`, `LICENSE` (MIT), issue templates
- [ ] Tag `v0.1.0`

Done when: a colleague who has never seen the repo gets a working agent from `bundle init` to Playground in under 30 minutes, timed.

## Notes

(Append dated notes here as phases complete: what was learned, what diverged from DESIGN.md, anything worth a post.)
