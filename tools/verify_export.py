"""Refuse to publish a dataset that is broken in a way nobody would notice.

The refresh runs unattended and commits its own output, so nothing between a bad
build and the live site catches it. The failure that matters is not a crash --
that is loud and stops the job -- but a build that succeeds and quietly ships
something wrong: an adapter that silently returned nothing, a merge that lost
most of its rows, a raw price column that slipped into the published files.

Every check here is one that a green build could otherwise hide. Exit non-zero
and the workflow stops before the commit step.

    python -m tools.verify_export [--min-funds N] [--baseline data/processed]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
PUBLISHED = ROOT / "docs" / "data"

# The policy from README.md, enforced here as well as in export.py. Two guards
# rather than one because this is the boundary that cannot be walked back: once
# a raw series is committed to a public repository, deleting it does not unpublish
# it.
FORBIDDEN_COLUMNS = {"open", "high", "low", "close", "adj_close", "volume"}

# GitHub hard-rejects a push containing a file above 100 MiB, so a dataset that
# grew past it would fail at `git push` -- after the build, with a confusing
# error. Catch it here where the message can say what to do about it.
MAX_FILE_BYTES = 95 * 1024 * 1024

# Tables the site cannot render without.
REQUIRED = ["funds.parquet", "listings.parquet", "manifest.json"]


def _fail(problems: list[str]) -> int:
    print(f"\nExport verification FAILED ({len(problems)} problem(s)):\n")
    for p in problems:
        print(f"  - {p}")
    print("\nThe dataset was not published.")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--published", type=Path, default=PUBLISHED)
    parser.add_argument(
        "--min-funds",
        type=int,
        default=5000,
        # A real merge yields ~12,000. Anything under this means several adapters
        # returned nothing and the build should not overwrite a good dataset with
        # the remains.
        help="fail if the funds table has fewer rows than this",
    )
    args = parser.parse_args(argv)

    out: Path = args.published
    problems: list[str] = []

    if not out.is_dir():
        return _fail([f"{out} does not exist -- export did not run"])

    for name in REQUIRED:
        if not (out / name).exists():
            problems.append(f"{name} is missing")

    for path in sorted(out.glob("*.parquet")):
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            problems.append(
                f"{path.name} is {size / 1048576:.0f} MB, over the {MAX_FILE_BYTES / 1048576:.0f} MB "
                "limit git will accept -- publish it as a release asset instead"
            )
        try:
            schema = pq.read_schema(path)
        except Exception as exc:  # noqa: BLE001 -- any unreadable file is a failure
            problems.append(f"{path.name} is not readable as Parquet: {exc}")
            continue

        leaked = FORBIDDEN_COLUMNS.intersection(schema.names)
        if leaked:
            problems.append(
                f"{path.name} carries raw price column(s) {sorted(leaked)}, which this "
                "project does not redistribute (see README, 'Sources and what this "
                "repository publishes')"
            )

        if pq.read_metadata(path).num_rows == 0:
            problems.append(f"{path.name} has no rows")

    funds = out / "funds.parquet"
    if funds.exists():
        rows = pq.read_metadata(funds).num_rows
        if rows < args.min_funds:
            problems.append(
                f"funds.parquet has only {rows:,} rows, under the {args.min_funds:,} floor -- "
                "an adapter probably returned nothing; check the build summary before republishing"
            )

    manifest_path = out / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("synthetic"):
            problems.append(
                "manifest reports synthetic data -- the demo fixtures would be published "
                "as if they were real funds"
            )
        if not manifest.get("as_of"):
            problems.append("manifest has no as_of date, so the site cannot date its figures")

    if problems:
        return _fail(problems)

    total = sum(p.stat().st_size for p in out.glob("*")) / 1048576
    print(f"Export verified: {len(list(out.glob('*.parquet')))} tables, {total:.1f} MB published.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
