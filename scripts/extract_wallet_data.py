#!/usr/bin/env python3
"""Extract transfer history and token balances for a list of addresses."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from probo.blocknumber import _endpoint_from_notes, _load_dotenv


_ALCHEMY_MAINNET = "https://eth-mainnet.g.alchemy.com/v2/{}"
_ETHERSCAN_API = "https://api.etherscan.io/api"


@dataclass(frozen=True)
class BlockInfo:
    number: int
    timestamp: int


def _post_json(url: str, payload: dict, timeout: int) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "accept": "application/json",
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_json(url: str, timeout: int) -> dict:
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _alchemy_endpoint(endpoint: Optional[str], notes_path: str) -> str:
    _load_dotenv()
    if endpoint:
        return endpoint
    api_key = os.getenv("ALCHEMY_API_KEY")
    if api_key:
        return _ALCHEMY_MAINNET.format(api_key)
    return _endpoint_from_notes(notes_path)


def _latest_block(endpoint: str, timeout: int) -> int:
    payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_blockNumber"}
    response = _post_json(endpoint, payload, timeout)
    result = response.get("result")
    if not result:
        raise RuntimeError("No result in eth_blockNumber response")
    return int(result, 16)


def _block_info(endpoint: str, block_num: int, timeout: int) -> BlockInfo:
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_num), False],
    }
    response = _post_json(endpoint, payload, timeout)
    result = response.get("result")
    if not result:
        raise RuntimeError(f"No result in eth_getBlockByNumber response for {block_num}")
    return BlockInfo(number=block_num, timestamp=int(result["timestamp"], 16))


def _find_block_by_timestamp(endpoint: str, target_ts: int, timeout: int) -> BlockInfo:
    latest = _latest_block(endpoint, timeout)
    cache: Dict[int, BlockInfo] = {}

    def get_block(num: int) -> BlockInfo:
        if num not in cache:
            cache[num] = _block_info(endpoint, num, timeout)
        return cache[num]

    low, high = 0, latest
    best = get_block(low)
    while low <= high:
        mid = (low + high) // 2
        info = get_block(mid)
        if info.timestamp < target_ts:
            best = info
            low = mid + 1
        elif info.timestamp > target_ts:
            high = mid - 1
        else:
            return info
    return best


def _read_addresses(path: Path) -> List[str]:
    addresses = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        addresses.append(line)
    return addresses


def _load_json_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_json_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _unique_transfer_key(item: dict) -> Tuple[str, str, str, str, str]:
    return (
        item.get("uniqueId") or "",
        item.get("hash") or "",
        str(item.get("logIndex") or ""),
        item.get("from") or "",
        item.get("to") or "",
    )


def _fetch_transfers(
    endpoint: str,
    address: str,
    from_block: str,
    to_block: str,
    timeout: int,
    max_count_hex: str,
    max_total: int,
) -> List[dict]:
    categories = ["external", "internal", "erc20", "erc721", "erc1155"]
    all_items: List[dict] = []
    seen: set[Tuple[str, str, str, str, str]] = set()

    for direction in ("fromAddress", "toAddress"):
        page_key: Optional[str] = None
        while True:
            params = {
                "fromBlock": from_block,
                "toBlock": to_block,
                "category": categories,
                "withMetadata": True,
                "excludeZeroValue": False,
                "maxCount": max_count_hex,
                "order": "desc",
                direction: address,
            }
            if page_key:
                params["pageKey"] = page_key
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [params],
            }
            response = _post_json(endpoint, payload, timeout)
            result = response.get("result") or {}
            transfers = result.get("transfers") or []
            for item in transfers:
                key = _unique_transfer_key(item)
                if key in seen:
                    continue
                seen.add(key)
                all_items.append(item)
                if len(all_items) >= max_total:
                    break
            page_key = result.get("pageKey")
            if not page_key or len(all_items) >= max_total:
                break
    return _sort_transfers_desc(all_items)[:max_total]


def _parse_iso_timestamp(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        return int(datetime.fromisoformat(cleaned).replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def _format_iso_timestamp(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _int_from_hex_or_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("0x"):
            return int(value, 16)
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _transfer_block_num(item: dict) -> Optional[int]:
    return _int_from_hex_or_int(item.get("blockNum"))


def _transfer_tx_index(item: dict) -> Optional[int]:
    return _int_from_hex_or_int(item.get("transactionIndex") or item.get("txIndex"))


def _transfer_log_index(item: dict) -> Optional[int]:
    return _int_from_hex_or_int(item.get("logIndex"))


def _transfer_timestamp(item: dict) -> Optional[int]:
    direct = _parse_iso_timestamp(item.get("blockTimestamp"))
    if direct is not None:
        return direct
    metadata = item.get("metadata") or {}
    return _parse_iso_timestamp(metadata.get("blockTimestamp"))


def _transfer_sort_key(item: dict) -> Tuple[int, int, int]:
    block_num = _transfer_block_num(item) or 0
    tx_index = _transfer_tx_index(item) or 0
    log_index = _transfer_log_index(item) or 0
    return (block_num, tx_index, log_index)


def _sort_transfers_desc(transfers: List[dict]) -> List[dict]:
    return sorted(transfers, key=_transfer_sort_key, reverse=True)


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


def _light_aggregates(address: str, transfers: List[dict]) -> dict:
    addr = address.lower()
    timestamps: List[int] = []
    total_in = 0.0
    total_out = 0.0
    counterparties: set[str] = set()

    for item in transfers:
        ts = _transfer_timestamp(item)
        if ts is not None:
            timestamps.append(ts)
        from_addr = (item.get("from") or "").lower()
        to_addr = (item.get("to") or "").lower()
        if from_addr and to_addr:
            if from_addr == addr:
                counterparties.add(to_addr)
                total_out += _parse_amount(item.get("value"))
            elif to_addr == addr:
                counterparties.add(from_addr)
                total_in += _parse_amount(item.get("value"))

    if timestamps:
        first_seen = min(timestamps)
        last_seen = max(timestamps)
    else:
        first_seen = None
        last_seen = None

    active_days = len(
        {
            datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            for ts in timestamps
        }
    )

    return {
        "address": address,
        "first_seen": first_seen,
        "first_seen_iso": _format_iso_timestamp(first_seen),
        "last_seen": last_seen,
        "last_seen_iso": _format_iso_timestamp(last_seen),
        "active_days": active_days,
        "tx_count": len(transfers),
        "unique_counterparties": len(counterparties),
        "total_in": total_in,
        "total_out": total_out,
    }


def _fanout_limits(level: int, base_days: int, base_tx: int, decay: float) -> Tuple[int, int]:
    factor = decay ** max(level - 1, 0)
    days = max(1, int(round(base_days * factor)))
    tx_cap = max(1, int(round(base_tx * factor)))
    return days, tx_cap


def _fetch_first_transfer(endpoint: str, address: str, timeout: int) -> Optional[dict]:
    categories = ["external", "internal", "erc20", "erc721", "erc1155"]
    earliest: Optional[dict] = None
    earliest_block: Optional[int] = None

    for direction in ("fromAddress", "toAddress"):
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "category": categories,
                    "withMetadata": True,
                    "excludeZeroValue": False,
                    "maxCount": "0x1",
                    "order": "asc",
                    direction: address,
                }
            ],
        }
        response = _post_json(endpoint, payload, timeout)
        result = response.get("result") or {}
        transfers = result.get("transfers") or []
        if not transfers:
            continue
        candidate = transfers[0]
        candidate_block = _transfer_block_num(candidate)
        if earliest is None:
            earliest = candidate
            earliest_block = candidate_block
            continue
        if candidate_block is not None and earliest_block is not None:
            if candidate_block < earliest_block:
                earliest = candidate
                earliest_block = candidate_block
        elif candidate_block is not None and earliest_block is None:
            earliest = candidate
            earliest_block = candidate_block
    return earliest


def _count_transfers(
    endpoint: str,
    address: str,
    to_block: str,
    timeout: int,
    max_count_hex: str,
    max_pages: int,
) -> Tuple[int, bool]:
    categories = ["external", "internal", "erc20", "erc721", "erc1155"]
    total = 0
    truncated = False

    for direction in ("fromAddress", "toAddress"):
        page_key: Optional[str] = None
        page_count = 0
        while True:
            params = {
                "fromBlock": "0x0",
                "toBlock": to_block,
                "category": categories,
                "withMetadata": False,
                "excludeZeroValue": False,
                "maxCount": max_count_hex,
                direction: address,
            }
            if page_key:
                params["pageKey"] = page_key
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [params],
            }
            response = _post_json(endpoint, payload, timeout)
            result = response.get("result") or {}
            transfers = result.get("transfers") or []
            total += len(transfers)
            page_key = result.get("pageKey")
            page_count += 1
            if not page_key:
                break
            if page_count >= max_pages:
                truncated = True
                break
    return total, truncated


def _fetch_token_balances(endpoint: str, address: str, timeout: int) -> dict:
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenBalances",
        "params": [address],
    }
    response = _post_json(endpoint, payload, timeout)
    result = response.get("result")
    if not result:
        raise RuntimeError("No result in alchemy_getTokenBalances response")
    return result


def _extract_token_addresses(transfers: Iterable[dict], token_balances: dict) -> List[str]:
    addresses: set[str] = set()
    for item in transfers:
        contract = item.get("rawContract") or {}
        address = contract.get("address")
        if address:
            addresses.add(address.lower())
    for item in token_balances.get("tokenBalances") or []:
        address = item.get("contractAddress")
        if address:
            addresses.add(address.lower())
    return sorted(addresses)


def _alchemy_token_metadata(endpoint: str, contract_address: str, timeout: int) -> Optional[dict]:
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenMetadata",
        "params": [contract_address],
    }
    response = _post_json(endpoint, payload, timeout)
    result = response.get("result")
    if not result:
        return None
    return result


def _etherscan_token_metadata(contract_address: str, timeout: int) -> Optional[dict]:
    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        return None
    url = (
        f"{_ETHERSCAN_API}?module=token&action=tokeninfo"
        f"&contractaddress={contract_address}&apikey={api_key}"
    )
    response = _get_json(url, timeout)
    if response.get("status") != "1":
        return None
    result = response.get("result")
    if isinstance(result, list):
        return result[0] if result else None
    if isinstance(result, dict):
        return result
    return None


def _get_token_metadata(
    endpoint: str,
    contract_address: str,
    timeout: int,
) -> Optional[dict]:
    metadata = _alchemy_token_metadata(endpoint, contract_address, timeout)
    if metadata:
        return metadata
    return _etherscan_token_metadata(contract_address, timeout)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _filter_transfers_by_timestamp(transfers: List[dict], min_ts: int) -> List[dict]:
    filtered: List[dict] = []
    for item in transfers:
        ts = _transfer_timestamp(item)
        if ts is None:
            continue
        if ts >= min_ts:
            filtered.append(item)
    return filtered


def _fanout_graph(
    endpoint: str,
    seed_address: str,
    seed_transfers: List[dict],
    seed_window_days: int,
    fanout_levels: int,
    base_days: int,
    base_tx: int,
    decay: float,
    timeout: int,
    max_count_hex: str,
    max_total_transfers: int,
    max_nodes: int,
    max_neighbors_per_node: int,
) -> dict:
    now_ts = int(time.time())
    nodes: Dict[str, dict] = {}
    edges: List[dict] = []

    def add_edge(item: dict) -> None:
        edges.append(
            {
                "from": item.get("from"),
                "to": item.get("to"),
                "hash": item.get("hash"),
                "block_num": _transfer_block_num(item),
                "transaction_index": _transfer_tx_index(item),
                "log_index": _transfer_log_index(item),
                "timestamp": _transfer_timestamp(item),
                "timestamp_iso": _format_iso_timestamp(_transfer_timestamp(item)),
                "category": item.get("category"),
                "asset": item.get("asset"),
            }
        )

    seed_addr = seed_address.lower()
    nodes[seed_addr] = {
        "address": seed_address,
        "level": 0,
        "window_days": seed_window_days,
        "aggregates": _light_aggregates(seed_address, seed_transfers),
    }

    visited: set[str] = {seed_addr}
    queue: List[Tuple[str, int]] = []

    if fanout_levels >= 1:
        days, tx_cap = _fanout_limits(1, base_days, base_tx, decay)
        min_ts = now_ts - days * 24 * 60 * 60
        window_transfers = _filter_transfers_by_timestamp(seed_transfers, min_ts)
        window_transfers = _sort_transfers_desc(window_transfers)[:tx_cap]
        neighbor_set: set[str] = set()
        for item in window_transfers:
            from_addr = (item.get("from") or "").lower()
            to_addr = (item.get("to") or "").lower()
            if not from_addr or not to_addr:
                continue
            add_edge(item)
            other = to_addr if from_addr == seed_addr else from_addr
            if other and other != seed_addr:
                neighbor_set.add(other)
            if len(neighbor_set) >= max_neighbors_per_node:
                break

        for neighbor in sorted(neighbor_set):
            if neighbor in visited:
                continue
            if len(nodes) >= max_nodes:
                break
            visited.add(neighbor)
            nodes[neighbor] = {
                "address": neighbor,
                "level": 1,
                "window_days": days,
                "aggregates": None,
            }
            queue.append((neighbor, 1))

    while queue and len(nodes) < max_nodes:
        current_addr, level = queue.pop(0)
        if level > fanout_levels:
            continue

        days, tx_cap = _fanout_limits(level, base_days, base_tx, decay)
        min_ts = now_ts - days * 24 * 60 * 60
        start_block = _find_block_by_timestamp(endpoint, min_ts, timeout)
        end_block = _block_info(endpoint, _latest_block(endpoint, timeout), timeout)
        window_transfers = _fetch_transfers(
            endpoint,
            current_addr,
            from_block=hex(start_block.number),
            to_block=hex(end_block.number),
            timeout=timeout,
            max_count_hex=max_count_hex,
            max_total=min(tx_cap, max_total_transfers),
        )

        nodes[current_addr]["window_days"] = days
        nodes[current_addr]["aggregates"] = _light_aggregates(current_addr, window_transfers)
        nodes[current_addr]["transfers_truncated"] = len(window_transfers) >= min(
            tx_cap, max_total_transfers
        )

        if level >= fanout_levels:
            continue

        neighbor_set = set()
        for item in window_transfers:
            from_addr = (item.get("from") or "").lower()
            to_addr = (item.get("to") or "").lower()
            if not from_addr or not to_addr:
                continue
            add_edge(item)
            other = to_addr if from_addr == current_addr else from_addr
            if other and other != current_addr:
                neighbor_set.add(other)
            if len(neighbor_set) >= max_neighbors_per_node:
                break

        for neighbor in sorted(neighbor_set):
            if neighbor in visited:
                continue
            if len(nodes) >= max_nodes:
                break
            visited.add(neighbor)
            nodes[neighbor] = {
                "address": neighbor,
                "level": level + 1,
                "window_days": None,
                "aggregates": None,
            }
            queue.append((neighbor, level + 1))

    return {
        "config": {
            "levels": fanout_levels,
            "base_days": base_days,
            "base_tx": base_tx,
            "decay": decay,
            "max_nodes": max_nodes,
            "max_neighbors_per_node": max_neighbors_per_node,
        },
        "nodes": list(nodes.values()),
        "edges": edges,
        "capped": len(nodes) >= max_nodes,
    }


def extract_for_address(
    endpoint: str,
    address: str,
    days: int,
    timeout: int,
    max_count: int,
    include_all_time_count: bool,
    all_time_max_pages: int,
    token_cache_path: Path,
    max_total_transfers: int,
    fanout_levels: int,
    fanout_base_days: int,
    fanout_base_tx: int,
    fanout_decay: float,
    fanout_max_nodes: int,
    fanout_max_neighbors_per_node: int,
) -> dict:
    print(f"[extract] address={address}")
    now_ts = int(time.time())
    target_ts = now_ts - days * 24 * 60 * 60
    start_block = _find_block_by_timestamp(endpoint, target_ts, timeout)
    end_block = _block_info(endpoint, _latest_block(endpoint, timeout), timeout)
    print(
        "[extract] window",
        hex(start_block.number),
        "->",
        hex(end_block.number),
        f"({days}d)",
    )

    transfers = _fetch_transfers(
        endpoint,
        address,
        from_block=hex(start_block.number),
        to_block=hex(end_block.number),
        timeout=timeout,
        max_count_hex=hex(max_count),
        max_total=max_total_transfers,
    )
    transfers = _sort_transfers_desc(transfers)
    print(f"[extract] transfers={len(transfers)} truncated={len(transfers) >= max_total_transfers}")
    token_balances = _fetch_token_balances(endpoint, address, timeout)
    print(
        f"[extract] token_balances={len(token_balances.get('tokenBalances') or [])}"
    )
    first_transfer = _fetch_first_transfer(endpoint, address, timeout)

    token_addresses = _extract_token_addresses(transfers, token_balances)
    token_cache = _load_json_cache(token_cache_path)
    token_metadata: Dict[str, dict] = {}
    updated = False
    cache_hits = 0
    cache_misses = 0
    for contract in token_addresses:
        cached = token_cache.get(contract)
        if cached:
            token_metadata[contract] = cached
            cache_hits += 1
            continue
        metadata = _get_token_metadata(endpoint, contract, timeout)
        cache_misses += 1
        if metadata:
            token_cache[contract] = {
                "contract_address": contract,
                "metadata": metadata,
                "fetched_at": now_ts,
            }
            token_metadata[contract] = token_cache[contract]
            updated = True
    if updated:
        _save_json_cache(token_cache_path, token_cache)
    print(f"[extract] token_meta cache hits={cache_hits} misses={cache_misses}")

    all_time_count = None
    all_time_truncated = None
    if include_all_time_count:
        all_time_count, all_time_truncated = _count_transfers(
            endpoint,
            address,
            to_block=hex(end_block.number),
            timeout=timeout,
            max_count_hex=hex(max_count),
            max_pages=all_time_max_pages,
        )
        print(
            f"[extract] all_time_transfers={all_time_count} truncated={all_time_truncated}"
        )

    result = {
        "address": address,
        "window_days": days,
        "window": {
            "from_block": hex(start_block.number),
            "from_timestamp": start_block.timestamp,
            "from_iso": _format_iso_timestamp(start_block.timestamp),
            "to_block": hex(end_block.number),
            "to_timestamp": end_block.timestamp,
            "to_iso": _format_iso_timestamp(end_block.timestamp),
        },
        "counts": {
            "transfers": len(transfers),
            "token_balances": len(token_balances.get("tokenBalances") or []),
        },
        "transfers_truncated": len(transfers) >= max_total_transfers,
        "first_transfer": {
            "block_num": hex(_transfer_block_num(first_transfer)) if first_transfer else None,
            "timestamp": _transfer_timestamp(first_transfer) if first_transfer else None,
            "iso": _format_iso_timestamp(_transfer_timestamp(first_transfer)) if first_transfer else None,
            "hash": first_transfer.get("hash") if first_transfer else None,
            "category": first_transfer.get("category") if first_transfer else None,
        }
        if first_transfer
        else None,
        "all_time_transfers": all_time_count,
        "all_time_transfers_truncated": all_time_truncated,
        "token_metadata": token_metadata,
        "transfers": transfers,
        "token_balances": token_balances,
        "fetched_at": now_ts,
        "fetched_at_iso": _format_iso_timestamp(now_ts),
    }

    if fanout_levels > 0:
        result["fanout"] = _fanout_graph(
            endpoint=endpoint,
            seed_address=address,
            seed_transfers=transfers,
            seed_window_days=days,
            fanout_levels=fanout_levels,
            base_days=fanout_base_days,
            base_tx=fanout_base_tx,
            decay=fanout_decay,
            timeout=timeout,
            max_count_hex=hex(max_count),
            max_total_transfers=max_total_transfers,
            max_nodes=fanout_max_nodes,
            max_neighbors_per_node=fanout_max_neighbors_per_node,
        )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract transfer data from Alchemy.")
    parser.add_argument(
        "--addresses-path",
        default="test_address.txt",
        help="Path to newline-delimited addresses file.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/extractions",
        help="Directory for JSON output files.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Lookback window in days.",
    )
    parser.add_argument(
        "--max-count",
        type=int,
        default=1000,
        help="Max transfers per page (Alchemy hex, default 1000).",
    )
    parser.add_argument(
        "--max-total-transfers",
        type=int,
        default=1000,
        help="Max total transfers to fetch per address before stopping.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Request timeout in seconds.",
    )
    parser.add_argument(
        "--include-all-time-count",
        action="store_true",
        help="Compute an all-time transfer count (can be slow).",
    )
    parser.add_argument(
        "--all-time-max-pages",
        type=int,
        default=200,
        help="Max pages per direction when counting all-time transfers.",
    )
    parser.add_argument(
        "--token-cache-path",
        default="data/token_metadata_cache.json",
        help="Path to token metadata cache file.",
    )
    parser.add_argument(
        "--fanout-levels",
        type=int,
        default=0,
        help="Depth of fan-out expansion (0 disables).",
    )
    parser.add_argument(
        "--fanout-base-days",
        type=int,
        default=30,
        help="Base lookback days for fan-out level 1.",
    )
    parser.add_argument(
        "--fanout-base-tx",
        type=int,
        default=100,
        help="Base transfer cap for fan-out level 1.",
    )
    parser.add_argument(
        "--fanout-decay",
        type=float,
        default=0.5,
        help="Decay factor per fan-out level.",
    )
    parser.add_argument(
        "--fanout-max-nodes",
        type=int,
        default=300,
        help="Max total nodes in fan-out graph.",
    )
    parser.add_argument(
        "--fanout-max-neighbors-per-node",
        type=int,
        default=100,
        help="Max neighbors per node when expanding.",
    )
    parser.add_argument(
        "--notes-path",
        default=".notes/notes.txt",
        help="Notes file that includes the Alchemy endpoint.",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Override RPC endpoint (optional).",
    )
    args = parser.parse_args()

    endpoint = _alchemy_endpoint(args.endpoint, args.notes_path)
    addresses = _read_addresses(Path(args.addresses_path))
    if not addresses:
        raise SystemExit("No addresses found to process.")

    output_dir = Path(args.output_dir)
    for address in addresses:
        payload = extract_for_address(
            endpoint,
            address,
            days=args.days,
            timeout=args.timeout,
            max_count=args.max_count,
            include_all_time_count=args.include_all_time_count,
            all_time_max_pages=args.all_time_max_pages,
            token_cache_path=Path(args.token_cache_path),
            max_total_transfers=args.max_total_transfers,
            fanout_levels=args.fanout_levels,
            fanout_base_days=args.fanout_base_days,
            fanout_base_tx=args.fanout_base_tx,
            fanout_decay=args.fanout_decay,
            fanout_max_nodes=args.fanout_max_nodes,
            fanout_max_neighbors_per_node=args.fanout_max_neighbors_per_node,
        )
        out_path = output_dir / f"{address.lower()}.json"
        _write_json(out_path, payload)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
