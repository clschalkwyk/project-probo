"""Analysis engine for Probo extraction payloads."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError


@dataclass(frozen=True)
class Reason:
    code: str
    detail: str
    weight: int


@dataclass(frozen=True)
class AnalysisResult:
    address: str
    score: int
    label: str
    reasons: List[Reason]
    features: Dict[str, object]


DEFAULT_DUST_THRESHOLD = 0.001
FRESH_WALLET_DAYS = 7
BURST_TX_COUNT = 20
LOW_DIVERSITY_MAX = 3
CONCENTRATION_MIN = 0.6
STABLECOIN_AGE_DAYS = 90
HIGH_COUNTERPARTY_MIN = 20
CONSISTENT_ACTIVE_DAYS = 10
MIN_SAMPLE_TX = 5
ACCELERATION_RECENT_DAYS = 7
ACCELERATION_BASELINE_DAYS = 14
ACCELERATION_MIN_TOTAL = 25
ACCELERATION_RATIO = 2.0


def load_stablecoins(path: str) -> Dict[str, dict]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    stablecoins = {}
    for item in payload.get("stablecoins", []):
        address = item.get("address")
        if address:
            stablecoins[address.lower()] = item
    return stablecoins


def _get_json(url: str, timeout: int, retries: int = 3, backoff: float = 1.0) -> dict:
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            last_exc = exc
            if attempt >= retries - 1:
                break
            time.sleep(backoff * (2**attempt))
    if last_exc:
        raise last_exc
    return {}


def fetch_etherscan_tx_bounds(
    address: str,
    api_key: str,
    timeout: int = 10,
    retries: int = 3,
    backoff: float = 1.0,
) -> Tuple[Optional[int], Optional[int]]:
    base = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "page": "1",
        "offset": "1",
        "sort": "asc",
        "apikey": api_key,
    }
    url = f"{base}?{urllib.parse.urlencode(params)}"
    earliest = _get_json(url, timeout, retries=retries, backoff=backoff)
    earliest_ts = None
    if earliest.get("status") == "1" and earliest.get("result"):
        earliest_ts = int(earliest["result"][0].get("timeStamp") or 0) or None

    params["sort"] = "desc"
    url = f"{base}?{urllib.parse.urlencode(params)}"
    latest = _get_json(url, timeout, retries=retries, backoff=backoff)
    latest_ts = None
    if latest.get("status") == "1" and latest.get("result"):
        latest_ts = int(latest["result"][0].get("timeStamp") or 0) or None

    return earliest_ts, latest_ts


def _parse_iso_ts(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        return int(datetime.fromisoformat(cleaned).replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None


def _transfer_timestamp(item: dict) -> Optional[int]:
    direct = _parse_iso_ts(item.get("blockTimestamp"))
    if direct is not None:
        return direct
    metadata = item.get("metadata") or {}
    return _parse_iso_ts(metadata.get("blockTimestamp"))


def _parse_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("0x"):
            return int(value, 16)
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _token_decimals(metadata: dict) -> int:
    decimals = metadata.get("decimals")
    if isinstance(decimals, int):
        return decimals
    try:
        return int(decimals)
    except (TypeError, ValueError):
        return 0


def _normalize_balance(raw_balance: object, decimals: int) -> float:
    value = _parse_int(raw_balance)
    if decimals <= 0:
        return float(value)
    return float(value) / (10**decimals)


def _counterparty_counts(address: str, transfers: Sequence[dict]) -> Dict[str, int]:
    addr = address.lower()
    counts: Dict[str, int] = {}
    for item in transfers:
        from_addr = (item.get("from") or "").lower()
        to_addr = (item.get("to") or "").lower()
        if from_addr == addr and to_addr:
            counts[to_addr] = counts.get(to_addr, 0) + 1
        elif to_addr == addr and from_addr:
            counts[from_addr] = counts.get(from_addr, 0) + 1
    return counts


def _total_flow(address: str, transfers: Sequence[dict]) -> tuple[float, float]:
    addr = address.lower()
    total_in = 0.0
    total_out = 0.0
    for item in transfers:
        value = float(item.get("value") or 0)
        from_addr = (item.get("from") or "").lower()
        to_addr = (item.get("to") or "").lower()
        if from_addr == addr:
            total_out += value
        elif to_addr == addr:
            total_in += value
    return total_in, total_out


def _acceleration_stats(timestamps: Sequence[int]) -> tuple[Optional[float], bool]:
    if not timestamps:
        return None, False
    max_ts = max(timestamps)
    max_date = datetime.fromtimestamp(max_ts, tz=timezone.utc).date()
    recent_start = max_date - timedelta(days=ACCELERATION_RECENT_DAYS - 1)
    baseline_start = recent_start - timedelta(days=ACCELERATION_BASELINE_DAYS)

    daily_counts: Dict[datetime.date, int] = {}
    for ts in timestamps:
        day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        daily_counts[day] = daily_counts.get(day, 0) + 1

    recent_total = 0
    baseline_total = 0
    for day, count in daily_counts.items():
        if day >= recent_start:
            recent_total += count
        elif baseline_start <= day < recent_start:
            baseline_total += count

    if recent_total < ACCELERATION_MIN_TOTAL:
        return None, False

    baseline_avg = baseline_total / ACCELERATION_BASELINE_DAYS
    recent_avg = recent_total / ACCELERATION_RECENT_DAYS
    if baseline_avg == 0:
        return None, True
    ratio = recent_avg / baseline_avg
    return ratio, ratio >= ACCELERATION_RATIO


def extract_features(
    payload: dict,
    stablecoins: Dict[str, dict],
    dust_threshold: float = DEFAULT_DUST_THRESHOLD,
) -> Dict[str, object]:
    address = (payload.get("address") or "").lower()
    transfers = payload.get("transfers") or []
    token_balances = payload.get("token_balances") or {}
    token_list = token_balances.get("tokenBalances") or []
    token_metadata = payload.get("token_metadata") or {}

    timestamps = [ts for ts in (_transfer_timestamp(t) for t in transfers) if ts is not None]
    acceleration_ratio, acceleration_flag = _acceleration_stats(timestamps)
    active_days = len(
        {
            datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            for ts in timestamps
        }
    )
    tx_count = len(transfers)
    total_in, total_out = _total_flow(address, transfers)
    in_out_ratio = total_out / max(total_in, 1e-9)

    counterparties = _counterparty_counts(address, transfers)
    unique_counterparties = len(counterparties)
    top_concentration = 0.0
    if tx_count:
        top_concentration = max(counterparties.values(), default=0) / tx_count

    first_seen = None
    if payload.get("first_transfer"):
        first_seen = payload["first_transfer"].get("timestamp")
    if first_seen is None and timestamps:
        first_seen = min(timestamps)
    wallet_age_days = None
    if first_seen:
        wallet_age_days = int((int(payload.get("fetched_at") or max(timestamps)) - first_seen) / 86400)

    erc20_count = 0
    stablecoin_balance_flag = False
    dust_only_flag = False
    normalized_balances = []

    for item in token_list:
        address_hex = item.get("contractAddress")
        if not address_hex:
            continue
        raw_balance = item.get("tokenBalance")
        if _parse_int(raw_balance) == 0:
            continue
        erc20_count += 1
        metadata = token_metadata.get(address_hex.lower(), {}).get("metadata", {})
        decimals = _token_decimals(metadata)
        normalized = _normalize_balance(raw_balance, decimals)
        normalized_balances.append(normalized)
        if address_hex.lower() in stablecoins:
            stablecoin_balance_flag = True

    if normalized_balances:
        dust_only_flag = all(balance <= dust_threshold for balance in normalized_balances)

    contract_interactions = 0
    for item in transfers:
        category = item.get("category")
        if category in {"erc20", "erc721", "erc1155", "internal"}:
            contract_interactions += 1
            continue
        if item.get("rawContract", {}).get("address"):
            contract_interactions += 1
    contract_interaction_density = contract_interactions / tx_count if tx_count else 0.0

    features = {
        "wallet_age_days": wallet_age_days,
        "active_days_30": active_days,
        "tx_count_30": tx_count,
        "in_out_ratio_30": in_out_ratio,
        "unique_counterparties_30": unique_counterparties,
        "top_counterparty_concentration": top_concentration,
        "erc20_count": erc20_count,
        "stablecoin_balance_flag": stablecoin_balance_flag,
        "dust_only_flag": dust_only_flag,
        "fresh_wallet_burst_flag": bool(
            wallet_age_days is not None
            and wallet_age_days <= FRESH_WALLET_DAYS
            and tx_count >= BURST_TX_COUNT
        ),
        "contract_interaction_density": contract_interaction_density,
        "tx_acceleration_ratio": acceleration_ratio,
        "tx_acceleration_flag": acceleration_flag,
        "transfers_truncated": payload.get("transfers_truncated"),
        "low_sample_flag": 0 < tx_count < MIN_SAMPLE_TX,
    }
    return features


def score_features(features: Dict[str, object]) -> tuple[int, List[Reason]]:
    score = 50
    reasons: List[Reason] = []
    tx_count = features.get("tx_count_30") or 0

    if features.get("fresh_wallet_burst_flag"):
        score += 20
        reasons.append(Reason("FRESH_BURST", "Fresh wallet with burst activity", 20))

    if features.get("dust_only_flag"):
        score += 10
        reasons.append(Reason("DUST_ONLY", "Only dust-level token balances", 10))

    if features.get("tx_acceleration_flag"):
        score += 15
        reasons.append(Reason("ACCELERATION", "Rapid increase in activity", 15))

    unique_counterparties = features.get("unique_counterparties_30") or 0
    top_concentration = features.get("top_counterparty_concentration") or 0
    if tx_count >= 5 and unique_counterparties <= LOW_DIVERSITY_MAX and top_concentration >= CONCENTRATION_MIN:
        score += 15
        reasons.append(Reason("LOW_DIVERSITY", "Low diversity and high concentration", 15))

    stablecoin_balance = features.get("stablecoin_balance_flag")
    wallet_age = features.get("wallet_age_days")
    if stablecoin_balance and wallet_age is not None and wallet_age >= STABLECOIN_AGE_DAYS:
        score -= 10
        reasons.append(Reason("STABLE_AGE", "Stablecoin usage and older wallet", -10))

    active_days = features.get("active_days_30") or 0
    if unique_counterparties >= HIGH_COUNTERPARTY_MIN and active_days >= CONSISTENT_ACTIVE_DAYS:
        score -= 10
        reasons.append(Reason("DIVERSE_ACTIVE", "Diverse counterparties and consistent activity", -10))

    if tx_count == 0:
        reasons.append(Reason("NO_RECENT_ACTIVITY", "No recent activity in window", 0))
    elif features.get("low_sample_flag"):
        reasons.append(Reason("LOW_SAMPLE", "Very few transfers; signal quality is limited", 0))

    if features.get("transfers_truncated"):
        reasons.append(Reason("WINDOW_TRUNCATED", "Transfer window hit the fetch cap", 0))

    if features.get("low_sample_flag"):
        score = int(round(50 + (score - 50) * 0.5))

    if not reasons:
        reasons.append(Reason("NO_SIGNALS", "No strong trust signals detected", 0))

    score = max(0, min(100, score))
    reasons = sorted(reasons, key=lambda r: abs(r.weight), reverse=True)[:5]
    return score, reasons


def label_from_score(score: int) -> str:
    if score <= 33:
        return "Low"
    if score <= 66:
        return "Medium"
    return "High"


def analyze_payload(
    payload: dict,
    stablecoins: Dict[str, dict],
    dust_threshold: float = DEFAULT_DUST_THRESHOLD,
) -> AnalysisResult:
    address = payload.get("address") or ""
    features = extract_features(payload, stablecoins, dust_threshold=dust_threshold)
    risk_score, reasons = score_features(features)
    trust_score = 100 - risk_score
    label = label_from_score(trust_score)
    return AnalysisResult(
        address=address,
        score=trust_score,
        label=label,
        reasons=reasons,
        features=features,
    )
