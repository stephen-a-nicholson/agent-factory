# CLAUDE.md

## What this repo is

**agent-factory** is a Databricks Declarative Automation Bundle (DAB) that turns a small metadata file describing a knowledge domain into a fully deployed, governed, evaluated set of AI agents: a Genie agent over structured tables and a RAG agent over documents, with vector search, promotion gates and rollback handled in CI/CD.

The point of the project is the factory, not the demo. Every decision should make it easier for a stranger to clone the repo, drop in their own `domains/<name>/domain.yml`, and get working agents in their workspace. If a change only helps the demo domain, it is probably in the wrong place.

Read `docs/DESIGN.md` before making any structural change. Read `docs/PLAN.md` at the start of every session to see what phase we are in and what is already done. Update the checklist in `docs/PLAN.md` as you complete items.

## Hard constraints

- Target platform: Azure Databricks, Unity Catalog, serverless compute wherever the resource supports it. No all-purpose clusters anywhere in the bundle.
- Databricks CLI 1.3.0 or later. The bundle uses the **direct deployment engine** (`bundle.engine: direct`). Never add Terraform-engine-only workarounds.
- Bundle resource types we depend on: `genie_spaces`, `vector_search_endpoints`, `vector_search_indexes`, `jobs`, `schemas`, `volumes`, `model_serving_endpoints`, `alerts`, `dashboards`. Do not invent resource keys. If unsure of a field, check the JSON schema at `https://github.com/databricks/cli/blob/main/bundle/schema/jsonschema.json` and the resources reference at `https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources`. Run `databricks bundle validate` after every YAML change.
- Known platform gotcha: Genie agents do not currently support a `permissions` block in the bundle (the permissions API endpoint for them does not exist and deploy fails after creating the space). Do not add one and do not include Genie agents in bundle-level `permissions`. Grant access through Unity Catalog on the underlying tables instead. Re-check this against the CLI changelog before assuming it is still true.
- Generated bundle YAML under `resources/generated/` is produced by `factory/generate.py` from `domains/*/domain.yml`. Never hand-edit generated files. Change the generator or the domain metadata, then regenerate. CI fails if generated output is stale.
- Python 3.11. Package management with `uv`. Formatting and linting with `ruff`. Type hints on all public functions. Tests with `pytest`.
- No secrets, tokens, workspace URLs with embedded credentials, or personal identifiers in any committed file. Auth in CI is GitHub OIDC federated to a Databricks service principal. Locally, use the Databricks CLI profile.
- Documentation and comments in UK English. No em dashes. No emoji in code, docs or commit messages.
- MIT licence. Anything we write must be safe to publish.

## Repo layout

```
agent-factory/
  databricks.yml            # bundle root, targets dev/test/prod, engine: direct
  resources/
    core/                   # hand-written: schemas, volumes, vs endpoint, shared jobs
    generated/              # written by factory/generate.py, never edited by hand
  domains/
    <name>/domain.yml       # the metadata contract, see docs/DESIGN.md
    <name>/data/            # small seed files or a loader spec
    <name>/eval/            # evaluation questions with expected answers
    <name>/genie/           # rendered .geniespace.json lives here after generate
  factory/                  # generator, loaders, eval runner, promotion logic
  gates/                    # bundle quality gates (standalone package + GH Action)
  src/                      # notebooks and Python entry points run by jobs
  tests/
  docs/
    DESIGN.md
    PLAN.md
  .github/
    workflows/
    actions/bundle-gates/
```

## Commands

```bash
uv sync                                     # install
uv run ruff check . && uv run ruff format . # lint and format
uv run pytest                               # unit tests, no workspace needed
uv run python -m factory.generate           # regenerate resources/generated/
uv run python -m gates.run --target dev     # run quality gates against a plan
databricks bundle validate -t dev
databricks bundle plan -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev ingest_<domain>
databricks bundle run -t dev evaluate_<domain>
```

## How to work in this repo

- Prefer editing the generator over adding special cases to YAML.
- Every job defined in the bundle must have a corresponding unit test of the code it runs and must be covered by at least one quality gate rule.
- When you add a gate rule, add a passing and a failing fixture under `tests/gates/fixtures/`.
- When you touch `domain.yml` schema, update the Pydantic model in `factory/schema.py`, the JSON schema it exports, the example in `docs/DESIGN.md`, and the demo domain.
- Keep notebooks thin. Logic lives in `src/` as importable Python so it can be tested with pytest; notebooks call it.
- Before finishing a task, run: ruff, pytest, generate, `bundle validate`. Report which of these you actually ran.
- Do not claim something deployed or passed unless you saw the command output. If you cannot reach a workspace, say so and stop at `bundle validate`.
- Commit messages: imperative mood, one line summary, body explaining why if not obvious.

## Things to avoid

- Do not add Terraform, Helm, Docker or any second deployment mechanism. The bundle is the whole story.
- Do not add a web UI. Consumers use Genie, the serving endpoint, or the Databricks Playground.
- Do not reach for pandas on data that lives in Unity Catalog; use Spark or Databricks SQL.
- Do not hardcode catalog or schema names. They come from bundle variables per target.
- Do not swallow exceptions in jobs. A failed ingestion or evaluation must fail the job so the pipeline can gate on it.
