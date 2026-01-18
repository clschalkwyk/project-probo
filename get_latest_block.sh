#!/usr/bin/env bash
set -euo pipefail

notes_file=".notes/notes.txt"
if [[ ! -f "$notes_file" ]]; then
  echo "Missing $notes_file" >&2
  exit 1
fi

endpoint=$(grep -oE 'https?://[^ ]+' "$notes_file" | head -n1 || true)
if [[ -z "$endpoint" ]]; then
  echo "No Ethereum RPC endpoint found in $notes_file" >&2
  exit 1
fi

payload='{"id":1,"jsonrpc":"2.0","method":"eth_blockNumber"}'
response=$(curl -sS "$endpoint" \
  --request POST \
  --header 'accept: application/json' \
  --header 'content-type: application/json' \
  --data "$payload")

python3 - <<'PY' <<<"$response"
import json
import sys

data = json.loads(sys.stdin.read() or '{}')
hex_block = data.get("result")
if not hex_block:
    raise SystemExit("No result in response")

print(f"hex: {hex_block}")
print(f"dec: {int(hex_block, 16)}")
PY
