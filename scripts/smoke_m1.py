#!/usr/bin/env python3
"""Cross-platform M1 smoke against the compiled daemon executable."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def request_json(base_url: str, path: str, payload: dict[str, object] | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(
        f"{base_url}{path}",
        data=data,
        headers={"content-type": "application/json"} if data else {},
    )
    with urlopen(request, timeout=2) as response:
        return json.load(response)


def wait_ready(base_url: str, process: subprocess.Popen, timeout: float = 10) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"daemon exited early with code {process.returncode}")
        try:
            snapshot = request_json(base_url, "/v1/health")
            if snapshot["lifecycle"] == "ready":
                return snapshot
        except URLError:
            pass
        time.sleep(0.05)
    raise TimeoutError("daemon did not become ready")


def start_daemon(executable: Path, database: Path, port: int) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update(
        BASTET_DATABASE=str(database),
        BASTET_LISTEN=f"127.0.0.1:{port}",
    )
    return subprocess.Popen(
        [str(executable)],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def run(executable: Path) -> dict[str, object]:
    if not executable.is_file():
        raise FileNotFoundError(executable)
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    evidence: dict[str, object] = {"daemon": str(executable), "port": port}
    process: subprocess.Popen | None = None
    with tempfile.TemporaryDirectory(prefix="bastet-m1-smoke-") as directory:
        database = Path(directory) / "bastet.db"
        try:
            process = start_daemon(executable, database, port)
            initial = wait_ready(base_url, process)
            evidence["initial"] = initial

            suspended = request_json(
                base_url,
                "/v1/power/suspend",
                {"expected_revision": initial["revision"], "reason": "automated sleep smoke"},
            )
            try:
                request_json(
                    base_url,
                    "/v1/checkpoints",
                    {"expected_revision": suspended["revision"], "reason": "must reject"},
                )
                raise AssertionError("checkpoint was accepted while suspended")
            except HTTPError as error:
                if error.code != 409:
                    raise
                evidence["suspended_checkpoint_status"] = error.code

            request_json(
                base_url,
                "/v1/power/resume",
                {"expected_revision": suspended["revision"], "reason": "automated wake smoke"},
            )
            resumed = request_json(base_url, "/v1/health")
            evidence["resumed"] = resumed
            shutdown = request_json(
                base_url,
                "/v1/shutdown",
                {"expected_revision": resumed["revision"], "reason": "automated graceful stop"},
            )
            process.wait(timeout=3)
            evidence["graceful_shutdown"] = shutdown

            process = start_daemon(executable, database, port)
            recovered = wait_ready(base_url, process)
            if recovered["daemon_id"] != initial["daemon_id"]:
                raise AssertionError("daemon identity changed after restart")
            evidence["recovered"] = recovered

            process.kill()
            process.wait(timeout=3)
            process = start_daemon(executable, database, port)
            crash_recovered = wait_ready(base_url, process)
            if crash_recovered["daemon_id"] != initial["daemon_id"]:
                raise AssertionError("daemon identity changed after forced kill")
            evidence["crash_recovered"] = crash_recovered
            request_json(
                base_url,
                "/v1/shutdown",
                {
                    "expected_revision": crash_recovered["revision"],
                    "reason": "automated smoke complete",
                },
            )
            process.wait(timeout=3)
            process = None
        finally:
            if process is not None:
                stop_process(process)
    return evidence


def main() -> int:
    default = Path("target/debug") / ("bastet-daemon.exe" if os.name == "nt" else "bastet-daemon")
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", type=Path, default=default)
    args = parser.parse_args()
    print(json.dumps(run(args.daemon.resolve()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
