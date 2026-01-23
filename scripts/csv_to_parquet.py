#!/usr/bin/env python3
"""Convert a CSV file into Parquet."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet.")
    parser.add_argument("--input", required=True, help="Path to input CSV.")
    parser.add_argument("--output", required=True, help="Path to output Parquet.")
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default: ,).",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Treat CSV as headerless.",
    )
    args = parser.parse_args()

    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("pandas is required: pip install pandas pyarrow") from exc

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    header = None if args.no_header else "infer"
    df = pd.read_csv(input_path, delimiter=args.delimiter, header=header)
    if args.no_header:
        df.columns = [f"column_{idx}" for idx in range(len(df.columns))]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(output_path, index=False)
    except Exception as exc:
        raise SystemExit(f"Failed to write Parquet: {exc}") from exc

    print(f"Wrote {output_path} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
