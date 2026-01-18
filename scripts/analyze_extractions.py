#!/usr/bin/env python3
"""Analyze extraction JSON files into scored outputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from probo.analysis import (
    analyze_payload,
    fetch_etherscan_tx_bounds,
    load_flagged_addresses,
    load_stablecoins,
)
from probo.blocknumber import _load_dotenv
from probo.infra_detection import summarize_infra


def _load_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_output(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze extraction JSON files.")
    parser.add_argument(
        "--input-dir",
        default="data/extractions",
        help="Directory containing extraction JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/analysis",
        help="Directory to write analysis JSON files.",
    )
    parser.add_argument(
        "--stablecoins-path",
        default="data/stablecoins.json",
        help="Path to stablecoin list JSON.",
    )
    parser.add_argument(
        "--flagged-path",
        default=None,
        help="Optional path to flagged address list.",
    )
    parser.add_argument(
        "--dust-threshold",
        type=float,
        default=0.001,
        help="Dust threshold for normalized token balances.",
    )
    parser.add_argument(
        "--etherscan-enrich",
        action="store_true",
        help="Fetch earliest/latest tx timestamps from Etherscan.",
    )
    parser.add_argument(
        "--etherscan-timeout",
        type=int,
        default=10,
        help="Timeout in seconds for Etherscan requests.",
    )
    parser.add_argument(
        "--infra",
        dest="infra",
        action="store_true",
        help="Include infra-behavior detection output.",
    )
    parser.add_argument(
        "--no-infra",
        dest="infra",
        action="store_false",
        help="Disable infra-behavior detection output.",
    )
    parser.set_defaults(infra=True)
    args = parser.parse_args()

    stablecoins = load_stablecoins(args.stablecoins_path)
    flagged = load_flagged_addresses(args.flagged_path)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    if not input_dir.exists():
        raise SystemExit(f"Missing input dir: {input_dir}")

    files = sorted(input_dir.glob("*.json"))
    if not files:
        raise SystemExit(f"No JSON files found in {input_dir}")

    _load_dotenv()
    etherscan_key = os.getenv("ETHERSCAN_API_KEY")

    for file_path in files:
        payload = _load_payload(file_path)
        result = analyze_payload(
            payload,
            stablecoins=stablecoins,
            flagged=flagged,
            dust_threshold=args.dust_threshold,
        )
        infra = None
        if args.infra:
            risk_memory = {
                "known_phishing": result.address.lower() in flagged,
                "known_scam": result.address.lower() in flagged,
            }
            infra = summarize_infra(payload, risk_memory=risk_memory)
        etherscan_info = None
        if args.etherscan_enrich and etherscan_key:
            earliest_ts, latest_ts = fetch_etherscan_tx_bounds(
                result.address,
                api_key=etherscan_key,
                timeout=args.etherscan_timeout,
            )
            etherscan_info = {
                "earliest_tx_ts": earliest_ts,
                "latest_tx_ts": latest_ts,
            }
        output = {
            "address": result.address,
            "score": result.score,
            "label": result.label,
            "reasons": [reason.__dict__ for reason in result.reasons],
            "features": result.features,
            "source_file": str(file_path),
        }
        if infra:
            output["infra"] = infra
        if etherscan_info:
            output["etherscan"] = etherscan_info
        out_path = output_dir / file_path.name
        _write_output(out_path, output)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
