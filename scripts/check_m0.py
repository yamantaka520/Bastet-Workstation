#!/usr/bin/env python3
"""Dependency-free validation for the Bastet Workstation M0 baseline."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "README.md",
    "LICENSE",
    "NOTICE",
    "THIRD_PARTY_NOTICES",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "REUSE.toml",
    "docs/MASTER_PLAN.md",
    "docs/adr/README.md",
    ".github/workflows/ci.yml",
    "Cargo.toml",
    "package.json",
    "rust-toolchain.toml",
)
ADR_FILES = tuple(f"docs/adr/{number:04d}-{slug}.md" for number, slug in (
    (1, "product-scope-and-mvp"),
    (2, "desktop-daemon-and-persistence"),
    (3, "identity-and-extension-contracts"),
    (4, "security-policy-and-credentials"),
    (5, "workspaces-concurrency-and-recovery"),
    (6, "role-bound-pets-and-meetings"),
    (7, "ui-localization-and-accessibility"),
))


def validate(root: Path = ROOT) -> list[str]:
    errors: list[str] = []
    for relative in (*REQUIRED_FILES, *ADR_FILES):
        if not (root / relative).is_file():
            errors.append(f"missing required file: {relative}")

    license_path = root / "LICENSE"
    if license_path.is_file():
        license_text = license_path.read_text(encoding="utf-8")
        if "Apache License" not in license_text or "Version 2.0" not in license_text:
            errors.append("LICENSE is not Apache License 2.0")
        if "MIT License" in license_text:
            errors.append("LICENSE still contains the superseded MIT license")

    reuse_path = root / "REUSE.toml"
    if reuse_path.is_file():
        reuse = reuse_path.read_text(encoding="utf-8")
        if 'SPDX-License-Identifier = "Apache-2.0"' not in reuse:
            errors.append("REUSE.toml lacks the Apache-2.0 SPDX identifier")

    plan_path = root / "docs/MASTER_PLAN.md"
    if plan_path.is_file():
        plan = plan_path.read_text(encoding="utf-8")
        required_plan_terms = (
            "# Bastet Workstation Master Plan",
            "License: Apache-2.0",
            "### M0 — Repository and specification baseline",
            "### M9 — BastetAgentOS handoff",
            "Agent prose is not test evidence.",
        )
        for term in required_plan_terms:
            if term not in plan:
                errors.append(f"Master Plan missing authority marker: {term}")

    for relative in ADR_FILES:
        path = root / relative
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            if "Status: Accepted" not in text or "Authority: Master Plan" not in text:
                errors.append(f"ADR lacks accepted status or authority link: {relative}")

    ignored_roots = {".git", ".venv", "node_modules", "target"}
    stale_product_name = "Agent" + " Orchestra"
    stale_identity = re.compile(re.escape(stale_product_name), re.IGNORECASE)
    allowed_stale_files = {"docs/MASTER_PLAN.md"}
    for path in root.rglob("*"):
        if not path.is_file() or ignored_roots.intersection(path.relative_to(root).parts):
            continue
        relative = path.relative_to(root).as_posix()
        if relative in allowed_stale_files:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if stale_identity.search(text):
            errors.append(f"stale product identity outside migration history: {relative}")

    return errors


def main() -> int:
    errors = validate()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"M0 baseline OK: {len(REQUIRED_FILES)} required files and {len(ADR_FILES)} ADRs validated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
