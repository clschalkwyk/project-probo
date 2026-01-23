#!/usr/bin/env python3
"""Analyze extraction JSON files into scored outputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from probo.analysis import (
    analyze_payload,
    fetch_etherscan_tx_bounds,
    load_stablecoins,
)
from probo.blocknumber import _load_dotenv
from probo.infra_detection import summarize_infra


def _log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def _load_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_output(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_config(path: str | None) -> dict:
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"Config file not found: {file_path}")
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON config: {file_path}") from exc


def main() -> None:
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config", default=None, help="Optional JSON config.")
    config_args, remaining = config_parser.parse_known_args()
    config = _load_config(config_args.config)

    parser = argparse.ArgumentParser(description="Analyze extraction JSON files.")
    parser.add_argument(
        "--config",
        default=config_args.config,
        help="Optional JSON config.",
    )
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
        "--etherscan-retries",
        type=int,
        default=3,
        help="Retry count for Etherscan requests.",
    )
    parser.add_argument(
        "--etherscan-backoff",
        type=float,
        default=1.0,
        help="Backoff seconds for Etherscan retries.",
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
    if config:
        parser.set_defaults(**config)
    args = parser.parse_args(remaining)

    stablecoins = load_stablecoins(args.stablecoins_path)
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
        _log(f"[analyze] file={file_path.name}")
        payload = _load_payload(file_path)
        result = analyze_payload(
            payload,
            stablecoins=stablecoins,
            dust_threshold=args.dust_threshold,
        )
        infra = None
        if args.infra:
            infra = summarize_infra(payload)
        etherscan_info = None
        if args.etherscan_enrich and etherscan_key:
            try:
                earliest_ts, latest_ts = fetch_etherscan_tx_bounds(
                    result.address,
                    api_key=etherscan_key,
                    timeout=args.etherscan_timeout,
                    retries=args.etherscan_retries,
                    backoff=args.etherscan_backoff,
                )
                etherscan_info = {
                    "earliest_tx_ts": earliest_ts,
                    "latest_tx_ts": latest_ts,
                }
            except Exception as exc:
                _log(f"[analyze] etherscan error address={result.address} err={exc}")
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
        _log(f"[analyze] wrote={out_path}")


if __name__ == "__main__":
    main()
