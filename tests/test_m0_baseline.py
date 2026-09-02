from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.check_m0 import ADR_FILES, ROOT, validate


class M0BaselineTests(unittest.TestCase):
    def test_repository_baseline_is_valid(self) -> None:
        self.assertEqual(validate(ROOT), [])

    def test_missing_files_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            errors = validate(Path(directory))
        self.assertIn("missing required file: README.md", errors)

    def test_stale_identity_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in (
                "README.md", "LICENSE", "NOTICE", "THIRD_PARTY_NOTICES",
                "CONTRIBUTING.md", "SECURITY.md", "REUSE.toml", "docs/MASTER_PLAN.md",
                "docs/adr/README.md", ".github/workflows/ci.yml", "Cargo.toml",
                "package.json", "rust-toolchain.toml",
            ):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("Apache License Version 2.0\n", encoding="utf-8")
            for relative in ADR_FILES:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("Status: Accepted\nAuthority: Master Plan\n", encoding="utf-8")
            (root / "legacy.txt").write_text("Agent" + " Orchestra", encoding="utf-8")
            errors = validate(root)
        self.assertIn(
            "stale product identity outside migration history: legacy.txt",
            errors,
        )


if __name__ == "__main__":
    unittest.main()
