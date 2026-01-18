#!/usr/bin/env python3
"""Update stablecoin address list for Ethereum."""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from urllib.error import HTTPError

STABLECOINS_URL = "https://stablecoins.llama.fi/stablecoins"
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"


def fetch_json(url: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "probo/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_coingecko_list(max_retries: int = 3) -> list[dict]:
    for attempt in range(max_retries):
        try:
            return fetch_json(COINGECKO_LIST_URL, timeout=60)
        except HTTPError as exc:
            if exc.code == 429 and attempt < max_retries - 1:
                time.sleep(20)
                continue
            raise
    return []


def circulating_usd(asset: dict) -> float:
    circ = asset.get("circulating", {})
    return float(circ.get("peggedUSD", 0) or 0)


def main() -> int:
    llama = fetch_json(STABLECOINS_URL, timeout=30)
    coingecko_list = fetch_coingecko_list()
    platforms_by_id = {item.get("id"): item.get("platforms", {}) for item in coingecko_list}

    assets = llama.get("peggedAssets", [])
    assets_sorted = sorted(assets, key=circulating_usd, reverse=True)

    results = []
    seen_addresses = set()

    for asset in assets_sorted:
        if len(results) >= 100:
            break
        gecko_id = asset.get("gecko_id")
        if not gecko_id:
            continue
        platforms = platforms_by_id.get(gecko_id) or {}
        eth_address = platforms.get("ethereum")
        if not eth_address or not eth_address.startswith("0x"):
            continue
        if eth_address in seen_addresses:
            continue

        results.append(
            {
                "name": asset.get("name"),
                "symbol": asset.get("symbol"),
                "address": eth_address,
                "gecko_id": gecko_id,
            }
        )
        seen_addresses.add(eth_address)

    payload = {
        "source": {
            "stablecoin_rank": STABLECOINS_URL,
            "address_lookup": COINGECKO_LIST_URL,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "chain": "ethereum",
        "count": len(results),
        "stablecoins": results,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/stablecoins.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote data/stablecoins.json with {len(results)} entries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
