#!/usr/bin/env python3
"""Fetch DFPI crypto scam tracker and store name + website entries."""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import List


DFPI_URL = "https://dfpi.ca.gov/consumers/crypto/crypto-scam-tracker/"


def _fetch_html(url: str, timeout: int) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _strip_tags(text: str) -> str:
    text = re.sub(r"<br\\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return (
        text.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&#x2d;", "-")
        .strip()
    )


def _split_websites(value: str) -> List[str]:
    parts = re.split(r"[\\s,]+", value.strip())
    cleaned = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        cleaned.append(part)
    return cleaned


def parse_dfpi_table(html_doc: str) -> List[dict]:
    match = re.search(r"<table[^>]*>(.*?)</table>", html_doc, re.S | re.I)
    if not match:
        raise RuntimeError("No table found in DFPI scam tracker page.")

    table_html = match.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S | re.I)
    if not rows:
        return []

    headers = [h.lower() for h in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rows[0], re.S | re.I)]
    header_labels = [_strip_tags(h) for h in headers]
    website_idx = None
    subject_idx = None
    for idx, label in enumerate(header_labels):
        if label.lower() == "website":
            website_idx = idx
        if label.lower() == "primary subject":
            subject_idx = idx

    if website_idx is None or subject_idx is None:
        raise RuntimeError("Unexpected DFPI table format.")

    entries: List[dict] = []
    for row in rows[1:]:
        cols = [
            _strip_tags(c)
            for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.S | re.I)
        ]
        if len(cols) <= max(website_idx, subject_idx):
            continue
        subject = cols[subject_idx].strip()
        websites = _split_websites(cols[website_idx])
        if not subject and not websites:
            continue
        entries.append({"name": subject, "websites": websites})

    return entries


def main() -> None:
    parser = argparse.ArgumentParser(description="Update DFPI scam tracker list.")
    parser.add_argument(
        "--output",
        default="data/dfpi_scams.json",
        help="Output JSON path.",
    )
    parser.add_argument("--timeout", type=int, default=20, help="Request timeout.")
    args = parser.parse_args()

    html_doc = _fetch_html(DFPI_URL, args.timeout)
    entries = parse_dfpi_table(html_doc)
    payload = {
        "source": DFPI_URL,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "count": len(entries),
        "entries": entries,
    }
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {out_path} ({len(entries)} entries)")


if __name__ == "__main__":
    main()
