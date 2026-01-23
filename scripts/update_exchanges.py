#!/usr/bin/env python3
"""Fetch exchange addresses from merklescience/ethereum-exchange-addresses."""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone
from typing import Iterable
from pathlib import Path

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
REPO = "https://github.com/merklescience/ethereum-exchange-addresses"
ZIP_URLS = [
    f"{REPO}/archive/refs/heads/main.zip",
    f"{REPO}/archive/refs/heads/master.zip",
]


def fetch_zip(timeout: int = 30) -> bytes:
    last_error = None
    for url in ZIP_URLS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "probo/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError("Unable to download exchange address repository.")


def _norm_addr(value: str) -> str | None:
    if not value:
        return None
    match = ADDR_RE.fullmatch(value.strip())
    if not match:
        return None
    return value.lower()


def _collect_address(
    index: dict[str, dict],
    address: str,
    label: str | None,
    source: str,
) -> None:
    if address not in index:
        index[address] = {
            "address": address,
            "labels": set(),
            "sources": set(),
        }
    if label:
        index[address]["labels"].add(label)
    index[address]["sources"].add(source)


def _add_matches(index: dict[str, dict], text: str, source: str, label: str | None = None) -> None:
    for addr in ADDR_RE.findall(text):
        _collect_address(index, addr.lower(), label, source)


def _parse_json_payload(index: dict[str, dict], payload: object, source: str) -> None:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                addr = _norm_addr(item)
                if addr:
                    _collect_address(index, addr, None, source)
            elif isinstance(item, dict):
                label = (
                    item.get("exchange")
                    or item.get("name")
                    or item.get("label")
                    or item.get("exchange_name")
                )
                addresses = []
                if "address" in item:
                    addresses.append(item.get("address"))
                if "addresses" in item and isinstance(item["addresses"], list):
                    addresses.extend(item["addresses"])
                for addr in addresses:
                    norm = _norm_addr(str(addr)) if addr else None
                    if norm:
                        _collect_address(index, norm, label, source)
    elif isinstance(payload, dict):
        for key, value in payload.items():
            label = str(key)
            if isinstance(value, list):
                for item in value:
                    norm = _norm_addr(str(item)) if item else None
                    if norm:
                        _collect_address(index, norm, label, source)
            elif isinstance(value, dict):
                addr = value.get("address")
                norm = _norm_addr(str(addr)) if addr else None
                if norm:
                    _collect_address(index, norm, label, source)
            else:
                addr = _norm_addr(str(value))
                if addr:
                    _collect_address(index, addr, label, source)


def _parse_csv_payload(index: dict[str, dict], text: str, source: str) -> None:
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return
    header = [cell.strip().lower() for cell in rows[0]]
    addr_idx = next((i for i, col in enumerate(header) if "address" in col), None)
    label_idx = next(
        (
            i
            for i, col in enumerate(header)
            if col in {"exchange", "name", "label", "entity"}
        ),
        None,
    )
    for row in rows[1:]:
        if not row:
            continue
        addr = None
        if addr_idx is not None and addr_idx < len(row):
            addr = _norm_addr(row[addr_idx])
        if not addr:
            for cell in row:
                addr = _norm_addr(cell)
                if addr:
                    break
        if not addr:
            continue
        label = None
        if label_idx is not None and label_idx < len(row):
            label = row[label_idx].strip() or None
        if label is None and len(row) >= 2:
            for cell in row:
                if _norm_addr(cell):
                    continue
                if cell and cell.strip():
                    label = cell.strip()
                    break
        _collect_address(index, addr, label, source)


def _iter_text_files(zip_bytes: bytes, max_size: int) -> Iterable[tuple[str, str]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        for info in zip_file.infolist():
            if info.is_dir():
                continue
            if info.file_size > max_size:
                continue
            name = info.filename
            if not name.lower().endswith((".json", ".csv", ".txt", ".md")):
                continue
            with zip_file.open(info) as handle:
                raw = handle.read()
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue
            yield name, text


def build_exchange_db(zip_bytes: bytes, max_size: int) -> dict:
    index: dict[str, dict] = {}
    scanned_files = []

    for name, text in _iter_text_files(zip_bytes, max_size=max_size):
        scanned_files.append(name)
        lower = name.lower()
        if lower.endswith(".json"):
            try:
                payload = json.loads(text)
                _parse_json_payload(index, payload, name)
            except json.JSONDecodeError:
                _add_matches(index, text, name)
        elif lower.endswith(".csv"):
            _parse_csv_payload(index, text, name)
        else:
            _add_matches(index, text, name)

    entries = []
    for address, payload in index.items():
        entries.append(
            {
                "address": address,
                "labels": sorted(payload["labels"]),
                "sources": sorted(payload["sources"]),
            }
        )

    return {
        "source": {
            "repo": REPO,
            "files_scanned": scanned_files,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(entries),
        "entries": sorted(entries, key=lambda item: item["address"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Update exchange address list.")
    parser.add_argument(
        "--output",
        default="data/exchanges.json",
        help="Output JSON path.",
    )
    parser.add_argument(
        "--csv-out",
        default="data/exchanges.csv",
        help="Output CSV path (address,name).",
    )
    parser.add_argument(
        "--max-file-size",
        type=int,
        default=30_000_000,
        help="Max size of files to scan (bytes).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Network timeout in seconds.",
    )
    args = parser.parse_args()

    try:
        zip_bytes = fetch_zip(timeout=args.timeout)
    except Exception as exc:
        print(f"Failed to download repo: {exc}", file=sys.stderr)
        return 1

    payload = build_exchange_db(zip_bytes, max_size=args.max_file_size)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    csv_path = Path(args.csv_out) if args.csv_out else None
    if csv_path:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["address", "name"])
            for entry in payload["entries"]:
                label = entry.get("labels")[0] if entry.get("labels") else ""
                writer.writerow([entry["address"], label])

    print(f"Wrote {output_path} with {payload['count']} entries")
    if csv_path:
        print(f"Wrote {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
