# agent-factory

A Databricks Declarative Automation Bundle that turns a small metadata file describing a knowledge domain into a fully deployed, governed, evaluated set of AI agents. See `docs/DESIGN.md` for the full architecture and `docs/PLAN.md` for build progress.

This section covers the bundle quality gates only; a full quickstart, domain contract reference and architecture diagram are added in a later phase (see `docs/PLAN.md` phase 7).

## Bundle quality gates

`gates/` is a standalone Python package that checks a Databricks Asset Bundle's resolved configuration against a fixed set of rules before it is allowed to deploy: no all-purpose clusters, required tags present, resource naming conventions, `test`/`prod` targets not left in development mode, no `permissions` block on a Genie space (a known platform limitation, see `CLAUDE.md`), vector search indexes `TRIGGERED` outside prod, no secret-shaped values in the resolved config, every job has a matching test file, serving endpoints scale to zero outside prod, and every domain with a RAG agent has a promotion threshold configured. See `docs/DESIGN.md` section 7 for the full rule table and the reasoning behind each one.

### Running the gates locally

```bash
uv sync
databricks bundle validate -t dev   # confirms auth and bundle shape first
uv run python -m gates.run --target dev
```

`gates.run` invokes `databricks bundle validate -t <target> -o json` (and, best-effort, `databricks bundle plan -t <target> -o json`) itself, so it needs a working Databricks CLI profile or `DATABRICKS_HOST`/`DATABRICKS_TOKEN` in the environment, the same as any other `databricks bundle` command. Exits non-zero if any rule reports a fail-severity finding; warn-severity findings are printed but do not fail the run. Pass `--output json` for machine-readable output, or `--config`/`--plan` to check a pre-captured JSON file instead of invoking the CLI (used by `tests/gates/`).

### Using the gates from another repository

The gates are also exposed as a composite GitHub Action, so a different bundle project can use them without vendoring this repository's Python:

```yaml
- uses: stephen-a-nicholson/agent-factory/.github/actions/bundle-gates@main
  with:
    target: test
```

The calling workflow is responsible for Databricks authentication before this step runs (an OIDC login step, or `DATABRICKS_HOST`/`DATABRICKS_TOKEN` secrets) and for checking out a copy of this repository's `gates/` package and `pyproject.toml` (the action installs `uv` and the Databricks CLI itself). See `.github/actions/bundle-gates/action.yml` for the full set of inputs.

### Adding a rule

A rule is a `gates.models.Rule` subclass under `gates/rules/` with a `name` and a `check(config, plan) -> list[Finding]` method, registered in `gates/rules/__init__.py`'s `ALL_RULES`. Add a passing case (checked against `tests/gates/fixtures/pass_config.json`, a real resolved dev bundle config with identifying details redacted) and a small, targeted failing fixture under `tests/gates/fixtures/`, per CLAUDE.md.
