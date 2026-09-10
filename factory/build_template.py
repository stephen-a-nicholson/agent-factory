"""(Re)builds template/{{.project_name}}/ as a tree of symlinks back to
this repository's own real files, so `databricks bundle init` (see
databricks_template_schema.json at the repo root) scaffolds a new project
from the exact same source this repo runs from, with no second copy to
drift out of sync. Run after adding or removing a file anywhere under one
of SHARED_PATHS below.

Usage: uv run python -m factory.build_template

Only individual *files* are symlinked, never directories: a real deploy
of this template found `databricks bundle init` fails outright on a
directory symlink ("copy_file_range: is a directory"), so this walks
every SHARED_PATHS entry itself and creates a matching real directory
plus one file symlink per file. See docs/PLAN.md phase 7 notes.

Excluded deliberately, not by omission: docs/PLAN.md (this repo's own
build journal, not useful to a fresh consumer) and domains/uk_rail (kept
in the working repo to prove the factory handles more than one domain,
but docs/DESIGN.md section 9 only promises "f1 as an example" in a fresh
scaffold, and there is no reason to ship a second, unexplained example
domain by default).
"""

from __future__ import annotations

import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_PROJECT_DIR = REPO_ROOT / "template" / "{{.project_name}}"

# Whole directories to mirror (every file under them gets one symlink),
# and individual files to symlink directly. Deliberately explicit rather
# than "everything except an exclude list", so a new top-level file has to
# be added here on purpose before it ships in a fresh scaffold.
SHARED_DIRS = [
    "factory",
    "src",
    "gates",
    "tests",
    ".github",
    "resources/core",
    "domains/f1",
]
SHARED_FILES = [
    "README.md",
    "LICENSE",
    "CONTRIBUTING.md",
    "pyproject.toml",
    "uv.lock",
    ".pre-commit-config.yaml",
    ".secrets.baseline",
    "CLAUDE.md",
    "docs/DESIGN.md",
    "docs/AUTH.md",
    "domains/domain.schema.json",
]

# Copied directly rather than symlinked like everything else in
# SHARED_FILES: a symlinked .gitignore made `git status` on this repo
# itself fail with "Too many levels of symbolic links" even though the
# symlink resolves fine under `cat`/`realpath` (git evidently treats a
# per-directory .gitignore specially in a way plain file reads do not).
# Low risk of drift either way: it does not change often.
COPIED_FILES = [".gitignore"]

# Real (non-symlinked) content that only exists under template/: the
# empty domain slot the user fills in, and a fresh docs/PLAN.md for their
# own project (this repo's docs/PLAN.md is deliberately excluded above).
# Sourced from template_sources/, not hand-duplicated from anywhere else.
TEMPLATE_ONLY_FILES = [
    "domains/{{.domain_name}}/domain.yml.tmpl",
    "docs/PLAN.md.tmpl",
]

# databricks.yml is a mechanical text transform of the real file (just
# the bundle name), not a symlink or a hand-maintained duplicate: this
# keeps it impossible for the templated version to drift from the real
# one on anything other than that one line. The output needs the .tmpl
# suffix (stripped by `bundle init`) for its {{.project_name}} to
# actually be rendered: confirmed by a real `bundle init` run leaving a
# literal, unrendered "{{.project_name}}" in the output when the
# destination file was plain "databricks.yml" instead. Only a file
# ending in .tmpl gets its *content* template-rendered; every other file
# is copied byte-for-byte, even one that happens to contain "{{...}}".
REWRITTEN_FILES = {
    "databricks.yml": ("databricks.yml.tmpl", [("name: agent-factory", "name: {{.project_name}}")]),
}


def _relative_symlink_target(link_path: Path, target_path: Path) -> Path:
    import os

    return Path(os.path.relpath(target_path, start=link_path.parent))


def _link_file(source: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() or dest.is_symlink():
        dest.unlink()
    dest.symlink_to(_relative_symlink_target(dest, source))


def build() -> None:
    if TEMPLATE_PROJECT_DIR.exists():
        shutil.rmtree(TEMPLATE_PROJECT_DIR)
    TEMPLATE_PROJECT_DIR.mkdir(parents=True)

    linked = 0
    for shared_dir in SHARED_DIRS:
        source_dir = REPO_ROOT / shared_dir
        for source_file in source_dir.rglob("*"):
            if source_file.is_file():
                relative = source_file.relative_to(REPO_ROOT)
                _link_file(source_file, TEMPLATE_PROJECT_DIR / relative)
                linked += 1

    for shared_file in SHARED_FILES:
        source_file = REPO_ROOT / shared_file
        _link_file(source_file, TEMPLATE_PROJECT_DIR / shared_file)
        linked += 1

    for copied_file in COPIED_FILES:
        dest_file = TEMPLATE_PROJECT_DIR / copied_file
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(REPO_ROOT / copied_file, dest_file)

    for template_only in TEMPLATE_ONLY_FILES:
        source_file = REPO_ROOT / "template_sources" / template_only
        dest_file = TEMPLATE_PROJECT_DIR / template_only
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, dest_file)

    for source_name, (dest_name, replacements) in REWRITTEN_FILES.items():
        content = (REPO_ROOT / source_name).read_text()
        for old, new in replacements:
            if old not in content:
                raise ValueError(f"{source_name}: expected text {old!r} not found")
            content = content.replace(old, new)
        dest_file = TEMPLATE_PROJECT_DIR / dest_name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_text(content)

    print(
        f"linked {linked} files, copied {len(TEMPLATE_ONLY_FILES)} template-only files, "
        f"rewrote {len(REWRITTEN_FILES)} files"
    )
    print(f"into {TEMPLATE_PROJECT_DIR.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    build()
