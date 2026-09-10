# CI/CD authentication

The workflows under `.github/workflows/` (`pr.yml`, `main.yml`, `prod.yml`, `rollback.yml`) need a Databricks identity to run `databricks bundle` commands against `test` and `prod`. This project's hard constraint (`CLAUDE.md`) is GitHub OIDC federated to a Databricks service principal, no PATs. Follow this document to set that up on a real Databricks workspace.

**This does not work on Databricks Free Edition.** Confirmed while building phase 0 (see `docs/PLAN.md` notes, 2026-09-10): Free Edition has no account console and no account-level APIs, so there is no way to create a service principal at all, and therefore nothing for GitHub OIDC to federate to. Free Edition is also one workspace per account, so `test` and `prod` cannot even be separate workspaces the way the steps below assume. If you are following this repo along on Free Edition, stop here: `pr.yml`'s lint/test/generate/validate/gates steps can still run in CI against no live workspace (they need no Databricks auth at all — `validate` needs auth, see the note on that job below), but `main.yml`/`prod.yml`/`rollback.yml` cannot run unattended without a workspace identity, and this repo has deliberately not added a PAT-based fallback to work around that, since a PAT in a GitHub secret is exactly the risk OIDC federation exists to avoid. Anyone doing this for real on Free Edition would need to accept that risk explicitly and knowingly, which is a decision for a human operating a specific workspace, not a default this template should ship with.

## Prerequisites

- A Databricks account with account admin access (Free Edition does not have this; a standard Azure Databricks account with account console access does).
- Separate `test` and `prod` workspaces, or at minimum separate catalogs the deploying identity is scoped to (`databricks.yml`'s `test`/`prod` targets already assume this via `${var.catalog}`).
- Repository admin access on GitHub, to add federated credentials and repository/environment secrets.

## 1. Create a service principal per target

In the Databricks account console (`https://accounts.azuredatabricks.net/` for Azure), under **User management > Service principals**, create one service principal per non-dev target (`agent-factory-test-sp`, `agent-factory-prod-sp`). Note each principal's **Application ID** (UUID) and the **numeric account ID** shown in the account console URL.

Grant each service principal:

- Workspace access to the corresponding workspace (account console > workspace > Permissions).
- `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME` on the target catalog (or `ALL PRIVILEGES` for simplicity in test; scope down for prod).
- `CAN_MANAGE` on the SQL warehouse referenced by `${var.warehouse_id}`.
- Bundle deploy needs the principal to be able to create jobs, Genie spaces, vector search indexes, alerts and dashboards in its own name (`run_as: service_principal_name: ${var.test_sp}` / `${var.prod_sp}` in `databricks.yml`, restored once this phase starts, see the phase 0 note on why they were removed initially).

## 2. Federate the service principal to GitHub OIDC

Still in the account console, on each service principal's **Federated credential policies** tab, add a policy trusting GitHub's OIDC issuer for this repository:

- Issuer: `https://token.actions.githubusercontent.com`
- Subject: `repo:<org>/<repo>:environment:test` (for the test service principal) and `repo:<org>/<repo>:environment:prod` (for the prod one) — matching the `environment:` used in `main.yml`/`prod.yml` below, so only workflow runs deploying to that GitHub Environment can assume that identity.
- Audience: `api://AzureADTokenExchange` (default).

## 3. Configure GitHub

Under **Settings > Environments**, create `test` and `prod` GitHub Environments. Add `prod` as a required reviewers environment (Settings > Environments > prod > Deployment protection rules) so promotion needs manual approval, per `docs/DESIGN.md` section 6.

Add these as environment-scoped secrets/variables (not repository-wide, so `test`'s workflow run cannot see `prod`'s identity or vice versa):

| Name | Environment | Value |
|---|---|---|
| `DATABRICKS_HOST` | test | the test workspace URL |
| `DATABRICKS_CLIENT_ID` | test | the test service principal's Application ID |
| `DATABRICKS_HOST` | prod | the prod workspace URL |
| `DATABRICKS_CLIENT_ID` | prod | the prod service principal's Application ID |

No client secret: OIDC federation means GitHub proves its identity to Databricks with a short-lived signed token instead, via `databricks/setup-cli`'s OIDC support (`id-token: write` permission, set per-job in the workflows below).

## 4. Verify

Push a no-op change to a branch, open a PR (exercises `pr.yml`, which does not need Databricks auth beyond `bundle validate`, itself only checking config shape), then merge to `main` and watch `main.yml` deploy to `test` in the Actions tab. A failure at the `databricks bundle deploy -t test` step with an auth error means the federated credential policy's subject does not match the workflow's actual `environment:` value; GitHub's own error on the OIDC token exchange step usually names the subject it presented, which should be compared against what was configured in step 2.
