"""Infrastructure behavior detectors (seeder/trap/relay)."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Dict, List, Optional


DUST_ETH = 0.001
DUST_TOKEN_DEFAULT = 1.0
FAST_FORWARD_SECONDS = 30 * 60


@dataclass(frozen=True)
class DetectorResult:
    score: int
    level: str
    reasons: List[str]


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


def _int_from_hex(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if value.startswith("0x"):
        return int(value, 16)
    try:
        return int(value)
    except ValueError:
        return None


def _normalized_value(item: dict, token_metadata: Dict[str, dict]) -> Optional[float]:
    raw_contract = item.get("rawContract") or {}
    raw_value = raw_contract.get("value")
    decimals = raw_contract.get("decimal") or raw_contract.get("decimals")
    if raw_value is not None and decimals is not None:
        raw_int = _int_from_hex(str(raw_value)) if str(raw_value).startswith("0x") else _int_from_hex(str(raw_value)) or int(raw_value)
        if raw_int is None:
            return None
        try:
            dec_int = int(decimals, 16) if isinstance(decimals, str) and decimals.startswith("0x") else int(decimals)
        except (TypeError, ValueError):
            dec_int = 0
        if dec_int <= 0:
            return float(raw_int)
        return float(raw_int) / (10**dec_int)

    value = item.get("value")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rolling_peak(timestamps: List[int], window_seconds: int) -> int:
    if not timestamps:
        return 0
    timestamps = sorted(timestamps)
    peak = 1
    left = 0
    for right in range(len(timestamps)):
        while timestamps[right] - timestamps[left] > window_seconds:
            left += 1
        peak = max(peak, right - left + 1)
    return peak


def _in_to_out_latency(in_times: List[int], out_times: List[int]) -> List[int]:
    if not in_times or not out_times:
        return []
    in_times = sorted(in_times)
    out_times = sorted(out_times)
    latencies = []
    j = 0
    for t_in in in_times:
        while j < len(out_times) and out_times[j] <= t_in:
            j += 1
        if j < len(out_times):
            latencies.append(out_times[j] - t_in)
    return latencies


def extract_features(payload: dict) -> Dict[str, object]:
    address = (payload.get("address") or "").lower()
    transfers = payload.get("transfers") or []
    token_metadata = payload.get("token_metadata") or {}

    in_times: List[int] = []
    out_times: List[int] = []
    timestamps: List[int] = []
    unique_senders = set()
    unique_recipients = set()
    sender_counts: Dict[str, int] = {}
    dust_out_count = 0
    dust_out_unique = set()
    total_in_value = 0.0
    total_out_value = 0.0

    for item in transfers:
        ts = _transfer_timestamp(item)
        if ts is not None:
            timestamps.append(ts)
        from_addr = (item.get("from") or "").lower()
        to_addr = (item.get("to") or "").lower()
        if to_addr == address and from_addr:
            unique_senders.add(from_addr)
            sender_counts[from_addr] = sender_counts.get(from_addr, 0) + 1
            if ts is not None:
                in_times.append(ts)
        if from_addr == address and to_addr:
            unique_recipients.add(to_addr)
            if ts is not None:
                out_times.append(ts)

        value = _normalized_value(item, token_metadata)
        if value is None:
            continue
        is_token = bool(item.get("rawContract", {}).get("address"))
        dust_threshold = DUST_TOKEN_DEFAULT if is_token else DUST_ETH
        if from_addr == address:
            total_out_value += value
            if value <= dust_threshold:
                dust_out_count += 1
                dust_out_unique.add(to_addr)
        elif to_addr == address:
            total_in_value += value

    tx_in_count = len(in_times)
    tx_out_count = len(out_times)
    tx_total = len(transfers)
    unique_counterparties = len(unique_senders.union(unique_recipients))
    active_days = len(
        {
            datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            for ts in timestamps
        }
    )
    peak_tx_per_10m = _rolling_peak(timestamps, 600)
    peak_tx_per_hour = _rolling_peak(timestamps, 3600)
    latencies = _in_to_out_latency(in_times, out_times)
    median_latency = int(median(latencies)) if latencies else None
    fast_forward_ratio = 0.0
    if latencies:
        fast_forward_ratio = sum(1 for l in latencies if l <= FAST_FORWARD_SECONDS) / max(tx_in_count, 1)
    sender_reuse_rate = 0.0
    if unique_senders:
        sender_reuse_rate = sum(1 for count in sender_counts.values() if count > 1) / len(unique_senders)

    return {
        "tx_in_count": tx_in_count,
        "tx_out_count": tx_out_count,
        "tx_total": tx_total,
        "unique_senders": len(unique_senders),
        "unique_recipients": len(unique_recipients),
        "unique_counterparties": unique_counterparties,
        "dust_out_count": dust_out_count,
        "dust_out_unique_recipients": len(dust_out_unique),
        "dust_out_ratio": dust_out_count / max(tx_out_count, 1),
        "total_in_value": total_in_value,
        "total_out_value": total_out_value,
        "in_out_ratio": total_out_value / max(total_in_value, 1e-9),
        "net_flow": total_in_value - total_out_value,
        "active_days": active_days,
        "peak_tx_per_10m": peak_tx_per_10m,
        "peak_tx_per_hour": peak_tx_per_hour,
        "median_in_to_out_seconds": median_latency,
        "fast_forward_ratio": fast_forward_ratio,
        "sender_reuse_rate": sender_reuse_rate,
    }


def _level(score: int) -> str:
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "NONE"


def detect_seeder(features: Dict[str, object]) -> DetectorResult:
    score = 0
    reasons = []
    if features["dust_out_unique_recipients"] >= 50:
        score += 30
        reasons.append("Many unique dust recipients")
    if features["dust_out_ratio"] >= 0.7:
        score += 20
        reasons.append("Most outgoing transfers are dust-sized")
    if features["peak_tx_per_10m"] >= 10:
        score += 20
        reasons.append("Burst activity in short windows")
    if features["unique_recipients"] >= 100:
        score += 10
        reasons.append("High recipient fan-out")
    return DetectorResult(score=score, level=_level(score), reasons=reasons)


def detect_trap(features: Dict[str, object]) -> DetectorResult:
    score = 0
    reasons = []
    if features["unique_senders"] >= 30:
        score += 30
        reasons.append("Many unique senders")
    if features["sender_reuse_rate"] <= 0.1 and features["unique_senders"] >= 10:
        score += 20
        reasons.append("Low sender reuse")
    median_latency = features["median_in_to_out_seconds"]
    if median_latency is not None and median_latency <= 3600:
        score += 30
        reasons.append("Fast forwarding after inbound")
    if features["fast_forward_ratio"] >= 0.5:
        score += 20
        reasons.append("High forward-through ratio")
    return DetectorResult(score=score, level=_level(score), reasons=reasons)


def detect_relay(features: Dict[str, object]) -> DetectorResult:
    score = 0
    reasons = []
    if features["tx_in_count"] >= 10 and features["tx_out_count"] >= 10:
        score += 25
        reasons.append("Meaningful in/out activity")
    median_latency = features["median_in_to_out_seconds"]
    if median_latency is not None and median_latency <= 900:
        score += 35
        reasons.append("Very rapid in→out turnover")
    if features["total_in_value"] > 0:
        net_ratio = abs(features["net_flow"]) / max(features["total_in_value"], 1e-9)
        if net_ratio <= 0.1:
            score += 20
            reasons.append("Net flow near zero")
    if features["unique_counterparties"] >= 30:
        score += 20
        reasons.append("Many counterparties")
    return DetectorResult(score=score, level=_level(score), reasons=reasons)


def summarize_infra(payload: dict, risk_memory: Optional[dict] = None) -> Dict[str, object]:
    features = extract_features(payload)
    seeder = detect_seeder(features)
    trap = detect_trap(features)
    relay = detect_relay(features)

    detectors = {
        "seeder": seeder,
        "trap": trap,
        "relay": relay,
    }
    level = "LOW"
    if any(det.level == "HIGH" for det in detectors.values()):
        level = "HIGH"
    elif any(det.level == "MEDIUM" for det in detectors.values()):
        level = "MEDIUM"

    explain = []
    for det in detectors.values():
        if det.level in {"HIGH", "MEDIUM"}:
            explain.extend(det.reasons[:2])

    risk_memory = risk_memory or {}
    if risk_memory.get("known_phishing"):
        if features["tx_total"] > 0:
            level = "HIGH"
            explain.append("Known phishing label with recent activity")
        else:
            if level == "LOW":
                level = "MEDIUM"
            explain.append("Known phishing label (historical)")

    return {
        "probobility": level,
        "detectors": {
            "seeder": seeder.__dict__,
            "trap": trap.__dict__,
            "relay": relay.__dict__,
        },
        "features": features,
        "explain": explain[:5],
    }
