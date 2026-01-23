"""Compare analysis outputs with recomputed results from extraction payloads."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from probo.analysis import analyze_payload, load_stablecoins


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_analysis_files(analysis_dir: Path) -> Iterable[Path]:
    return sorted(path for path in analysis_dir.glob("*.json") if path.is_file())


def _extract_address(analysis_payload: dict, fallback: Path) -> str:
    if analysis_payload.get("address"):
        return str(analysis_payload["address"]).lower()
    return fallback.stem.lower()


def _resolve_extraction_path(analysis_payload: dict, extractions_dir: Path, address: str) -> Path:
    source_file = analysis_payload.get("source_file")
    if source_file:
        path = Path(source_file)
        if path.is_file():
            return path
    return extractions_dir / f"{address}.json"


def _compare_values(left: Any, right: Any, tol: float) -> Tuple[bool, Optional[float]]:
    if left == right:
        return True, None
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        diff = float(left) - float(right)
        if abs(diff) <= tol:
            return True, diff
        return False, diff
    return False, None


def _diff_features(stored: dict, recomputed: dict, tol: float) -> List[dict]:
    diffs = []
    for key in sorted(set(stored) | set(recomputed)):
        left = stored.get(key)
        right = recomputed.get(key)
        same, delta = _compare_values(left, right, tol)
        if not same:
            diffs.append({"key": key, "stored": left, "recomputed": right, "delta": delta})
    return diffs


def _reason_codes(reasons: Iterable[Any]) -> List[str]:
    codes = []
    for reason in reasons:
        if isinstance(reason, dict):
            code = reason.get("code")
        else:
            code = getattr(reason, "code", None)
        if code:
            codes.append(str(code))
    return sorted(set(codes))


def _summarize_counts(payload: dict) -> dict:
    counts = payload.get("counts") or {}
    return {
        "transfers": counts.get("transfers"),
        "window_transfers_total": counts.get("window_transfers_total"),
        "token_balances": counts.get("token_balances"),
        "transfers_truncated": payload.get("transfers_truncated"),
        "transfers_more_available": payload.get("transfers_more_available"),
    }


def calibrate(
    analysis_dir: Path,
    extractions_dir: Path,
    stablecoins_path: Path,
    dust_threshold: float,
    tolerance: float,
    max_items: Optional[int],
) -> dict:
    stablecoins = load_stablecoins(str(stablecoins_path))
    results = []
    missing_extractions = []
    errors = []

    files = list(_iter_analysis_files(analysis_dir))
    if max_items is not None:
        files = files[:max_items]

    for path in files:
        try:
            analysis_payload = _load_json(path)
            address = _extract_address(analysis_payload, path)
            extraction_path = _resolve_extraction_path(analysis_payload, extractions_dir, address)
            if not extraction_path.exists():
                missing_extractions.append({"address": address, "analysis_file": str(path)})
                continue
            extraction_payload = _load_json(extraction_path)
            recomputed = analyze_payload(
                extraction_payload,
                stablecoins=stablecoins,
                dust_threshold=dust_threshold,
            )
            stored_features = analysis_payload.get("features") or {}
            recomputed_features = recomputed.features
            feature_diffs = _diff_features(stored_features, recomputed_features, tolerance)

            stored_score = analysis_payload.get("score")
            stored_label = analysis_payload.get("label")
            stored_reasons = analysis_payload.get("reasons") or []
            recomputed_reasons = recomputed.reasons

            recomputed_score = recomputed.score
            recomputed_label = recomputed.label

            stored_codes = _reason_codes(stored_reasons)
            recomputed_codes = _reason_codes(recomputed_reasons)

            result = {
                "address": address,
                "analysis_file": str(path),
                "extraction_file": str(extraction_path),
                "score_delta": (recomputed_score - stored_score)
                if stored_score is not None
                else None,
                "label_match": stored_label == recomputed_label,
                "stored_label": stored_label,
                "recomputed_label": recomputed_label,
                "stored_score": stored_score,
                "recomputed_score": recomputed_score,
                "feature_diffs": feature_diffs,
                "reason_codes_missing": sorted(set(stored_codes) - set(recomputed_codes)),
                "reason_codes_added": sorted(set(recomputed_codes) - set(stored_codes)),
                "counts": _summarize_counts(extraction_payload),
            }
            results.append(result)
        except Exception as exc:
            errors.append({"analysis_file": str(path), "error": str(exc)})

    summary = {
        "analysis_files": len(files),
        "matched": len(results),
        "missing_extractions": len(missing_extractions),
        "errors": len(errors),
        "label_mismatch": sum(1 for item in results if not item["label_match"]),
        "feature_mismatch": sum(1 for item in results if item["feature_diffs"]),
    }

    return {
        "summary": summary,
        "results": results,
        "missing_extractions": missing_extractions,
        "errors": errors,
    }


def _print_summary(report: dict) -> None:
    summary = report["summary"]
    print("calibration_summary")
    for key in (
        "analysis_files",
        "matched",
        "missing_extractions",
        "errors",
        "label_mismatch",
        "feature_mismatch",
    ):
        print(f"- {key}: {summary.get(key)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare stored analysis with recomputed results from extraction payloads."
    )
    parser.add_argument("--analysis-dir", default="data/analysis", help="Analysis JSON directory.")
    parser.add_argument("--extractions-dir", default="data/extractions", help="Extraction JSON directory.")
    parser.add_argument("--stablecoins", default="data/stablecoins.json", help="Stablecoins JSON path.")
    parser.add_argument("--dust-threshold", type=float, default=0.001, help="Dust threshold.")
    parser.add_argument("--tolerance", type=float, default=1e-6, help="Float comparison tolerance.")
    parser.add_argument("--max-items", type=int, default=None, help="Limit number of files.")
    parser.add_argument(
        "--output",
        default="data/analysis/calibration_report.json",
        help="Output report JSON path.",
    )
    args = parser.parse_args()

    report = calibrate(
        analysis_dir=Path(args.analysis_dir),
        extractions_dir=Path(args.extractions_dir),
        stablecoins_path=Path(args.stablecoins),
        dust_threshold=args.dust_threshold,
        tolerance=args.tolerance,
        max_items=args.max_items,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    _print_summary(report)
    print(f"report_written: {output_path}")


if __name__ == "__main__":
    main()
