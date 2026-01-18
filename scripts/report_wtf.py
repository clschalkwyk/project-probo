#!/usr/bin/env python3
"""Summarize extraction JSON into a human-readable WTF report."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_lower(value: object) -> str:
    return str(value).lower() if value is not None else ""


def _parse_amount(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _transfer_timestamp(item: dict) -> Optional[int]:
    if "blockTimestamp" in item and item["blockTimestamp"]:
        return _parse_iso_ts(item["blockTimestamp"])
    metadata = item.get("metadata") or {}
    return _parse_iso_ts(metadata.get("blockTimestamp"))


def _parse_iso_ts(value: object) -> Optional[int]:
    if not value:
        return None
    try:
        cleaned = str(value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(cleaned).replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def _summarize_transfers(address: str, transfers: Iterable[dict]) -> dict:
    addr = address.lower()
    categories = Counter()
    assets = Counter()
    counterparties = Counter()
    in_count = 0
    out_count = 0
    total_in = 0.0
    total_out = 0.0
    timestamps: List[int] = []

    for item in transfers:
        categories[item.get("category") or "unknown"] += 1
        asset = item.get("asset") or "unknown"
        assets[asset] += 1
        from_addr = _safe_lower(item.get("from"))
        to_addr = _safe_lower(item.get("to"))
        value = _parse_amount(item.get("value"))
        ts = _transfer_timestamp(item)
        if ts is not None:
            timestamps.append(ts)
        if from_addr == addr:
            out_count += 1
            total_out += value
            if to_addr:
                counterparties[to_addr] += 1
        elif to_addr == addr:
            in_count += 1
            total_in += value
            if from_addr:
                counterparties[from_addr] += 1

    first_seen = min(timestamps) if timestamps else None
    last_seen = max(timestamps) if timestamps else None

    return {
        "categories": categories,
        "assets": assets,
        "counterparties": counterparties,
        "in_count": in_count,
        "out_count": out_count,
        "total_in": total_in,
        "total_out": total_out,
        "first_seen": first_seen,
        "last_seen": last_seen,
    }


def _summarize_fanout(fanout: dict) -> dict:
    nodes = fanout.get("nodes") or []
    edges = fanout.get("edges") or []
    levels = Counter()
    truncated = 0
    for node in nodes:
        levels[node.get("level")] += 1
        if node.get("transfers_truncated"):
            truncated += 1
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "levels": levels,
        "truncated_nodes": truncated,
        "capped": fanout.get("capped"),
        "config": fanout.get("config") or {},
    }


def _print_top(counter: Counter, limit: int) -> List[Tuple[str, int]]:
    return counter.most_common(limit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize extraction JSON.")
    parser.add_argument("input", help="Path to extraction JSON file.")
    parser.add_argument("--top", type=int, default=5, help="Top N entries to show.")
    args = parser.parse_args()

    payload = _load_json(Path(args.input))
    address = payload.get("address") or "unknown"
    transfers = payload.get("transfers") or []
    summary = _summarize_transfers(address, transfers)

    print(f"WTF report for: {address}")
    print(f"Window: {payload.get('window', {}).get('from_iso')} -> {payload.get('window', {}).get('to_iso')}")
    print(f"Transfers fetched: {len(transfers)} (truncated={payload.get('transfers_truncated')})")
    print(f"First seen: {_fmt_ts(summary['first_seen'])}")
    print(f"Last seen:  {_fmt_ts(summary['last_seen'])}")
    print(f"In/Out: {summary['in_count']} in, {summary['out_count']} out")
    print(f"Total value: in={summary['total_in']:.6f}, out={summary['total_out']:.6f}")

    print("\nTop categories:")
    for name, count in _print_top(summary["categories"], args.top):
        print(f"- {name}: {count}")

    print("\nTop assets:")
    for name, count in _print_top(summary["assets"], args.top):
        print(f"- {name}: {count}")

    print("\nTop counterparties:")
    for name, count in _print_top(summary["counterparties"], args.top):
        print(f"- {name}: {count}")

    first_transfer = payload.get("first_transfer") or {}
    if first_transfer:
        print("\nEarliest transfer (global):")
        print(f"- timestamp: {first_transfer.get('iso')}")
        print(f"- hash: {first_transfer.get('hash')}")
        print(f"- category: {first_transfer.get('category')}")

    fanout = payload.get("fanout")
    if fanout:
        fanout_summary = _summarize_fanout(fanout)
        print("\nFan-out summary:")
        print(f"- nodes: {fanout_summary['node_count']}, edges: {fanout_summary['edge_count']}")
        print(f"- capped: {fanout_summary['capped']}")
        if fanout_summary["levels"]:
            level_str = ", ".join(
                f"L{level}={count}" for level, count in sorted(fanout_summary["levels"].items())
            )
            print(f"- levels: {level_str}")
        if fanout_summary["truncated_nodes"]:
            print(f"- truncated_nodes: {fanout_summary['truncated_nodes']}")
        if fanout_summary["config"]:
            print(f"- config: {fanout_summary['config']}")


if __name__ == "__main__":
    main()
