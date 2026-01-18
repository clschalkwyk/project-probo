"""Ethereum block number queries."""

from __future__ import annotations

import json
import os
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


_URL_RE = re.compile(r"https?://\S+")
_ALCHEMY_KEY_RE = re.compile(r"(https?://[^\\s]*/v2/)([^/?#\\s]+)")


@dataclass(frozen=True)
class BlockNumber:
    hex: str
    dec: int


def _endpoint_from_notes(notes_path: str) -> str:
    _load_dotenv()
    notes_file = Path(notes_path)
    if not notes_file.exists():
        raise FileNotFoundError(f"Missing {notes_path}")

    content = notes_file.read_text(encoding="utf-8")
    match = _URL_RE.search(content)
    if not match:
        raise ValueError(f"No Ethereum RPC endpoint found in {notes_path}")
    endpoint = match.group(0)
    env_key = os.getenv("ALCHEMY_API_KEY")
    if env_key:
        endpoint = endpoint.replace("${ALCHEMY_API_KEY}", env_key).replace("$ALCHEMY_API_KEY", env_key)
        endpoint = _ALCHEMY_KEY_RE.sub(r"\1" + env_key, endpoint)
    return endpoint


def _load_dotenv(path: str = ".env") -> None:
    if os.getenv("ALCHEMY_API_KEY"):
        return
    env_file = Path(path)
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


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


def get_block_number_hex(
    endpoint: Optional[str] = None,
    notes_path: str = ".notes/notes.txt",
    timeout: int = 10,
) -> str:
    """Return the latest block number from Ethereum mainnet as a hex string."""

    url = endpoint or _endpoint_from_notes(notes_path)
    payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_blockNumber"}
    response = _post_json(url, payload, timeout)
    result = response.get("result")
    if not result:
        raise RuntimeError("No result in JSON-RPC response")
    return result


def get_block_number(
    endpoint: Optional[str] = None,
    notes_path: str = ".notes/notes.txt",
    timeout: int = 10,
) -> BlockNumber:
    """Return the latest block number from Ethereum mainnet in hex and decimal."""

    hex_block = get_block_number_hex(endpoint=endpoint, notes_path=notes_path, timeout=timeout)
    return BlockNumber(hex=hex_block, dec=int(hex_block, 16))
