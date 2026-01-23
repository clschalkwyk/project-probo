#!/usr/bin/env python3
"""Backfill DynamoDB with extraction payloads from data/extractions."""
from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
from pathlib import Path
import sys
import time

import boto3
from boto3.dynamodb.types import TypeSerializer


def _compress_payload(payload: dict) -> dict:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    compressed = gzip.compress(raw)
    encoded = base64.b64encode(compressed).decode("ascii")
    return {
        "encoding": "gzip+base64",
        "data": encoded,
        "original_bytes": len(raw),
        "compressed_bytes": len(compressed),
    }


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill DynamoDB with extraction payloads.")
    parser.add_argument("--input-dir", default="data/extractions", help="Extraction JSON directory.")
    parser.add_argument("--table", default=os.getenv("PROBO_DDB_TABLE"), help="DynamoDB table name.")
    parser.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"))
    parser.add_argument("--ttl-days", type=int, default=int(os.getenv("PROBO_DDB_TTL_DAYS", "30")))
    args = parser.parse_args()

    if not args.table:
        raise SystemExit("Missing DynamoDB table name (set PROBO_DDB_TABLE or --table).")

    client = boto3.client("dynamodb", region_name=args.region)
    serializer = TypeSerializer()

    input_dir = Path(args.input_dir)
    files = sorted(path for path in input_dir.glob("*.json") if path.is_file())
    if not files:
        raise SystemExit(f"No extraction files found in {input_dir}")

    now_ts = int(time.time())
    ttl = now_ts + args.ttl_days * 24 * 60 * 60

    total = len(files)
    print(f"[backfill] table={args.table} region={args.region} files={total}", flush=True)

    for path in files:
        start = time.time()
        try:
            print(f"[backfill] loading {path}", flush=True)
            payload = _load_json(path)
            address = (payload.get("address") or path.stem).lower()
            compressed = _compress_payload(payload)
            item = {
                "address": address,
                "record_type": "extraction",
                "source": "backfill",
                "payload": compressed,
                "updated_at": now_ts,
                "ttl": ttl,
            }
            marshalled = {key: serializer.serialize(value) for key, value in item.items()}
            client.put_item(TableName=args.table, Item=marshalled)
            elapsed = time.time() - start
            print(
                f"[backfill] ok address={address} bytes={compressed['original_bytes']} "
                f"compressed={compressed['compressed_bytes']} elapsed={elapsed:.2f}s",
                flush=True,
            )
        except Exception as exc:
            print(f"[backfill] error file={path} err={exc}", file=sys.stderr, flush=True)

    print("[backfill] complete", flush=True)


if __name__ == "__main__":
    main()
