"""Run independent unit/fixture or real-host checks with fail-closed accounting."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("unit", "host"), default="unit")
    args = parser.parse_args()
    modules = sorted((ROOT / "tests").glob("test_*.py"))
    modules = [p for p in modules if (p.stem == "test_loader_integration") == (args.suite == "host")]
    total = 0
    failed = []
    with tempfile.TemporaryDirectory(prefix="pi-test-home-") as home:
        env = {**os.environ, "HERMES_HOME": home, "PYTHONDONTWRITEBYTECODE": "1"}
        if args.suite == "host":
            env["PI_REQUIRE_HOST_TESTS"] = "1"
        for module in modules:
            print(f"--- {args.suite}: {module.stem}", flush=True)
            try:
                result = subprocess.run([sys.executable, "-m", "unittest", module.stem, "-v"],
                                        cwd=ROOT / "tests", env=env, capture_output=True,
                                        text=True, timeout=300)
            except subprocess.TimeoutExpired:
                print("Module timed out", flush=True)
                failed.append(module.stem)
                continue
            output = result.stdout + result.stderr
            print(output, end="" if output.endswith("\n") else "\n", flush=True)
            counts = re.findall(r"Ran (\d+) tests? in", output)
            count = int(counts[-1]) if counts else 0
            total += count
            if result.returncode or not count or re.search(r"skipped=\d+", output):
                failed.append(module.stem)
    print(f"SUMMARY suite={args.suite} modules={len(modules)} tests={total} failed={failed}")
    return int(bool(failed) or not modules or not total)


if __name__ == "__main__":
    raise SystemExit(main())
