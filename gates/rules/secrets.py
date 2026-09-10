"""no_secrets_in_yaml: fail if any string value in the resolved bundle
config (or plan) looks like a credential: a URL with an embedded
username:password, a token matching a known provider's format (AWS,
GitHub, Slack), or a non-empty value under a key name that itself
suggests a secret (password, token, api_key, ...). A second line of
defence alongside the detect-secrets pre-commit hook, this one running
against the *resolved* config (post-${var...} substitution) rather than
the committed source YAML, so it also catches a secret that only appears
after variable substitution.

Deliberately does not do blind entropy/length scanning across every
string: this project's real resolved config already contains plenty of
long opaque-looking strings that are not secrets (the Genie space's
serialized_space JSON, md5-derived stable ids, SQL query text in alerts
and dashboards), and a generic "looks random and long" pattern flagged
almost all of them in an early version of this rule. Known formats and
suspicious key names are a much lower false-positive way to catch the
same real mistakes.
"""

from __future__ import annotations

import re
from typing import Any

from gates.models import Finding, Rule, Severity

VALUE_PATTERNS = {
    "url with embedded credentials": re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:[^/\s:@]+@"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "Databricks personal access token": re.compile(r"\bdapi[0-9a-f]{32,}\b"),
}
SUSPICIOUS_KEY_NAMES = re.compile(
    r"(password|passwd|secret|api[_-]?key|access[_-]?key|private[_-]?key|"
    r"client[_-]?secret|auth[_-]?token)",
    re.IGNORECASE,
)
# Fields legitimately containing long opaque identifiers that are not
# secrets: skip these rather than trying to make the patterns above
# precise enough to never false-positive on them.
SAFE_KEY_NAMES = {
    "etag",
    "job_id",
    "id",
    "run_id",
    "space_id",
    "index_id",
    "endpoint_id",
    "commit",
    "metastore_id",
}


class NoSecretsInYaml(Rule):
    name = "no_secrets_in_yaml"

    def check(self, config: dict[str, Any], plan: dict[str, Any] | None) -> list[Finding]:
        findings: list[Finding] = []
        findings.extend(self._scan(config, "config"))
        if plan is not None:
            findings.extend(self._scan(plan, "plan"))
        return findings

    def _scan(self, obj: Any, path: str, key_name: str | None = None) -> list[Finding]:
        findings: list[Finding] = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in SAFE_KEY_NAMES:
                    continue
                findings.extend(self._scan(value, f"{path}.{key}", key_name=key))
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                findings.extend(self._scan(item, f"{path}[{i}]", key_name=key_name))
        elif isinstance(obj, str):
            if key_name and SUSPICIOUS_KEY_NAMES.search(key_name) and obj.strip():
                findings.append(
                    Finding(
                        self.name,
                        Severity.FAIL,
                        f"value under suspicious key {key_name!r} looks like a secret",
                        resource=path,
                    )
                )
                return findings
            for label, pattern in VALUE_PATTERNS.items():
                if pattern.search(obj):
                    findings.append(
                        Finding(self.name, Severity.FAIL, f"value looks like a {label}", path)
                    )
                    break
        return findings
