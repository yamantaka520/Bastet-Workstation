#!/usr/bin/env python3
"""Cross-platform M1 smoke against the compiled local-IPC daemon."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess


def run(executable: Path) -> dict[str, object]:
    if not executable.is_file():
        raise FileNotFoundError(executable)
    repository = Path(__file__).resolve().parent.parent
    environment = os.environ.copy()
    environment["BASTET_SMOKE_DAEMON"] = str(executable)
    result = subprocess.run(
        [
            "cargo",
            "test",
            "-p",
            "bastet-client",
            "--test",
            "daemon_lifecycle",
            "--",
            "--ignored",
            "--nocapture",
        ],
        cwd=repository,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    evidence_prefix = "BASTET_SMOKE_EVIDENCE="
    for line in result.stdout.splitlines():
        if line.startswith(evidence_prefix) and result.returncode == 0:
            return json.loads(line.removeprefix(evidence_prefix))
    raise RuntimeError("compiled local IPC daemon lifecycle smoke failed")


def main() -> int:
    default = Path("target/debug") / (
        "bastet-daemon.exe" if os.name == "nt" else "bastet-daemon"
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", type=Path, default=default)
    args = parser.parse_args()
    print(json.dumps(run(args.daemon.resolve()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
