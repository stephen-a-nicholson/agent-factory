"""Standalone bundle quality gates: reads `databricks bundle validate -o
json` (and, best-effort, `bundle plan -o json`) and checks a fixed set of
rules against it. See docs/DESIGN.md section 7 and gates/run.py."""
