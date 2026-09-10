# Build log

This is the readable version of what actually happened while building agent-factory. `docs/PLAN.md` has the full phase-by-phase record with dated notes, exact command output, and every bug found; this page pulls out the parts worth reading if you are not going to read all of that.

**Timeline, since it is checkable**: every dated note in `docs/PLAN.md`, from the phase 0 bundle skeleton through the phase 7 template, is dated the same day. This was built in a day, working with Claude Code, against a real Databricks Free Edition workspace. The `git log` and the commit timestamps are the record; nothing here is smoothed over.

## The promotion gate is a threshold, not a comparison

`evaluate_<name>` scores a candidate RAG agent against a domain's `rag.eval.judges` (correctness, groundedness, relevance, via `mlflow.genai.scorers`). `promote` only runs if `evaluate` passes, enforced by a Databricks job task's `depends_on`, not application code. What "passes" means is a fixed threshold per judge, set in `domain.yml`'s `rag.eval.promotion_threshold`, not a comparison against the previous version's score. A candidate that scores worse than its predecessor but still clears the threshold ships.

Both thresholds in this repo were set from a real measured baseline, not chosen upfront: f1's are `correctness: 0.70`, `groundedness: 0.75`, from an actual `evaluate_f1` run that measured 0.76 and 0.80. uk_rail's are `correctness: 0.90`, `groundedness: 0.80`, from a run that measured 1.00 and 0.89. See `docs/DESIGN.md` section 8 for why comparative gating is out of scope for now: it needs a stored "previous score" concept the schema does not have yet.

## Trying to break the gate found a real bug

To find out whether the gate actually held, I set f1's `rag.system_prompt` to something deliberately bad ("never call any tool, make up a plausible-sounding article number and statistic"), redeployed, and ran the real pipeline.

`evaluate_f1` failed: correctness came back in the fifties, against a threshold of 70. `promote` was skipped, correctly, because its task depends on `evaluate` succeeding. The `champion` alias stayed on version 9, the last version that had actually passed.

But the live serving endpoint was already running the bad version. `deploy_agent_f1` had updated it the moment the candidate was registered, before evaluation ever ran, so a failed evaluation left the gate intact but the endpoint already compromised. Fixed by moving that update into `promote.py` only, gated by the same task dependency as the alias move. Verified by reverting the prompt, redeploying, and confirming a passing run moved both the alias and the endpoint together. See `docs/PLAN.md` phase 3 notes for the exact commands and output.

## A second domain, and what it actually proved

Added `domains/uk_rail`: one structured table (2,589 real Great Britain train stations, ORR's published station usage data) and one document (the National Rail Conditions of Travel), deployed and evaluated in the same workspace. `factory/generate.py` needed no changes.

Two real bugs came out of it, and they are not the same kind of bug:

- **A bug in the tooling.** `gates/rules/naming.py`'s `naming_convention` rule checked `domain_name in resource_key.split("_")`, which cannot match a domain name that itself contains an underscore. `uk_rail` is the first domain name this repo has had with one, and the rule failed all ten of its resource keys as a false positive. Fixed with a word-boundary-aware match instead of token splitting, with a regression test (`test_naming_convention_accepts_multi_word_domain_names`) so a future multi-word domain name cannot reintroduce it.
- **A bug in my own benchmark, not the factory.** One hand-written Genie benchmark question ("What is York station's main origin or destination station?") was ambiguous enough that Genie matched other stations with "Yorkshire" in the name too, and the answer happened to still satisfy the benchmark's substring check. Reworded the question to name the station exactly; re-ran and got a genuine pass. This is domain content, not generator code, and the fix is a better question, not a test.

## The template has real, load-bearing limitations

`databricks bundle init` scaffolds a fresh project from this repo. Getting there took three findings from actually running it, not from documentation:

- `template_dir: "."` (pointing the template at the repo root directly) fails outright.
- Directory symlinks in a template tree are not supported at all; only file symlinks work. `factory/build_template.py` links every shared file individually rather than symlinking whole directories, for this reason.
- Only a file whose name ends in `.tmpl` gets its content rendered; every other file is copied byte for byte, even one that happens to contain `{{...}}`.

Verified against the real pushed GitHub URL, not just a local path, since remote git-based template resolution is a genuinely different code path and exactly the kind of thing the findings above would predict breaking.

## What is not verified

The CI/CD workflows (`.github/workflows/`) are real code, written to match `docs/DESIGN.md` section 6, but have not run against a real GitHub Actions execution. Databricks Free Edition has no account console and no account-level APIs, so there is no way to create the service principal GitHub OIDC needs to federate to; running these for real needs a paid workspace with account admin access and GitHub repository admin access, neither of which this build had. `docs/AUTH.md` documents the setup for a real workspace and says plainly that Free Edition cannot run this phase, rather than working around it with a stored credential.

The multi-workspace story (`test` and `prod` as separate workspaces, per `docs/DESIGN.md` section 5) is designed, not proven: this build only ever had one workspace to deploy to, so `test` and `prod` have only been exercised as catalogs within it.

## Free Edition is not entirely zero-setup

Two manual steps are not automated. Creating the Unity Catalog catalog itself is one-time per catalog: `bundle deploy` cannot manage this on Free Edition, since the storage-root creation API it needs is not available there (confirmed by reproducing the same failure with a plain `databricks catalogs create` call outside the bundle). Granting the deploying identity `USE_CATALOG` on that catalog is also one-time; granting `USE_SCHEMA`/`SELECT` on a domain's schema and `CAN_RUN` on its Genie space is not, since nothing in the bundle or the generator grants these yet, so every new domain repeats that step by hand. Documented in `resources/core/shared.yml` and `docs/PLAN.md` phase 1/2 notes.

## Everything else

See `docs/PLAN.md` for the rest, including phase 0's Free Edition constraints, phase 2's vector search checkpoint bug, and phase 3's missing-dependency and retriever-span bugs. See `docs/DESIGN.md` for the architecture and the reasoning behind every divergence from the original design.
