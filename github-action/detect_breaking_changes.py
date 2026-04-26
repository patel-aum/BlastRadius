#!/usr/bin/env python3
"""Detect breaking changes between the current contract and the base branch version."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

ENGINE_DIR = Path(__file__).resolve().parent.parent / "contract_engine"
sys.path.insert(0, str(ENGINE_DIR))

from validator import detect_breaking_changes, load_contract


def run_breaking_change_detection(
    contract_path: str, base_ref: str = "origin/main"
) -> list[dict] | None:
    """Compare contract_path against the same file on base_ref.

    Returns None if no base version exists, or a list of breaking changes.
    """
    rel_path = contract_path

    try:
        result = subprocess.run(
            ["git", "show", f"{base_ref}:{rel_path}"],
            capture_output=True,
            text=True,
            check=True,
        )
        old_content = result.stdout
    except subprocess.CalledProcessError:
        return None

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(old_content)
        tmp_path = tmp.name

    try:
        return detect_breaking_changes(tmp_path, contract_path)
    except Exception as exc:
        print(f"::warning::Could not compare versions: {exc}")
        return None
    finally:
        Path(tmp_path).unlink(missing_ok=True)


if __name__ == "__main__":
    import json

    if len(sys.argv) < 2:
        print("Usage: detect_breaking_changes.py <contract_path> [base_ref]")
        sys.exit(1)

    path = sys.argv[1]
    ref = sys.argv[2] if len(sys.argv) > 2 else "origin/main"
    changes = run_breaking_change_detection(path, ref)

    if changes is None:
        print("No base version found — skipping")
    elif not changes:
        print("No breaking changes detected")
    else:
        print(f"Found {len(changes)} breaking change(s):")
        print(json.dumps(changes, indent=2))
        sys.exit(1)
