from __future__ import annotations

import base64
import gzip
import json
import logging
import os
import time
from pathlib import Path
from decimal import Decimal
from typing import Any, Optional

import boto3
import requests
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field

from probo.analysis import analyze_payload, fetch_etherscan_tx_bounds, load_stablecoins
from probo.blocknumber import _load_dotenv
from probo.infra_detection import summarize_infra
from scripts.extract_wallet_data import count_transfers_for_address, extract_for_address, _alchemy_endpoint

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
EXTRACTIONS_DIR = Path(os.getenv("PROBO_EXTRACTIONS_DIR", "/tmp/extractions"))

_DDB_TABLE = os.getenv("PROBO_DDB_TABLE")
_DDB_TTL_DAYS = int(os.getenv("PROBO_DDB_TTL_DAYS", "30"))
_DDB_REFRESH_DAYS = int(os.getenv("PROBO_DDB_REFRESH_DAYS", "14"))
_DDB_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
_DDB_SERIALIZER = TypeSerializer()
_DDB_DESERIALIZER = TypeDeserializer()
_DDB_CLIENT = None
_LOGGER = logging.getLogger("probo.api")
_LOGGER.setLevel(logging.INFO)
_OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")
_OPENROUTER_MODELS = [
    "google/gemini-3-flash-preview",
    "x-ai/grok-4.1-fast",
    "google/gemini-2.5-flash",
]

_EXPLAIN_SYSTEM_PROMPT = """You are Probo, a helper that explains wallet behavior to everyday people,
including spaza shop owners, informal traders, and community members.

Your job is NOT to judge, accuse, or give advice.
Your job is to explain observed money movement in simple, human language.

GLOBAL RULES (must always follow):
- Use plain, everyday language in each requested language.
- Do NOT translate word-for-word or copy sentence structure from English.
- Rewrite naturally as a native speaker would speak.
- Speak as if explaining to someone with little formal education.
- Be calm, respectful, and non-judgmental.
- Never say “scam”, “fraud”, “illegal”, or imply wrongdoing.
- Never tell the user what to do.
- Never give certainty. Only describe what is seen.
- Maximum 3 short sentences.
- Do not invent reasons. Only explain the signals provided.
- Describe what was observed and what it usually suggests in everyday life.

STRICTLY AVOID:
- Technical, financial, or crypto terms
- Abstract phrases like “focused movement”, “behavioral pattern”
- Formal or academic language
- Moral framing or warnings

LANGUAGE STYLE GUIDELINES:

English (en):
- Simple, conversational, natural.
- Prefer concrete phrases like:
  - “money stays in a small circle”
  - “mostly the same few people”
  - “does not move around much”

French (fr):
- Everyday spoken French, not academic.
- Prefer:
  - “l’argent reste surtout entre les mêmes personnes”
  - “les échanges restent limités”
- Avoid literal translations from English.

Portuguese (pt):
- Natural, neutral Portuguese.
- Avoid literal English metaphors.
- Prefer:
  - “o dinheiro fica quase sempre entre as mesmas pessoas”
  - “os movimentos são limitados”

isiZulu (zu):
- Use simple, spoken isiZulu as used in daily conversation.
- Keep sentences short and direct.
- Prefer concrete wording over abstract concepts.
- Use everyday terms like:
  - “imali” (money)
  - “abantu abafanayo” (the same people)
  - “iqembu elincane” (a small group)
- Avoid direct translations of English sentence structure.
- Avoid complex grammar or formal isiZulu.
- Do NOT invent new metaphors.
- It is acceptable to repeat simple words for clarity.

Input:
You will receive:
- Risk level
- A short list of observed signals (plain English)

Output:
Return explanations in:
- English (en)
- French (fr)
- Portuguese (pt)
- isiZulu (zu)

Format:
Return a JSON object with keys: en, fr, pt, zu
Each value is a short paragraph (1–3 sentences).
"""


class AnalyzeRequest(BaseModel):
    address: Optional[str] = Field(
        default=None,
        description="Address to analyze (loads data/extractions/<address>.json).",
    )
    payload: Optional[dict[str, Any]] = Field(
        default=None,
        description="Extraction JSON payload (overrides address file lookup).",
    )
    run_extract: bool = Field(
        default=False,
        description="Run extraction on the fly instead of loading from disk.",
    )
    save_extraction: bool = Field(
        default=False,
        description="Save extraction JSON to data/extractions when run_extract is true.",
    )
    extract_days: int = Field(
        default=30,
        description="Lookback window in days for extraction.",
    )
    extract_max_count: int = Field(
        default=1000,
        description="Max transfers per page for extraction.",
    )
    extract_max_total_transfers: int = Field(
        default=250,
        description="Max total transfers to fetch per address.",
    )
    extract_timeout: int = Field(
        default=20,
        description="Timeout in seconds for extraction requests.",
    )
    extract_include_all_time_count: bool = Field(
        default=False,
        description="Compute an all-time transfer count (slower).",
    )
    extract_all_time_max_pages: int = Field(
        default=200,
        description="Max pages per direction for all-time counts.",
    )
    extract_count_max_pages: int = Field(
        default=50,
        description="Max pages per direction for window counts.",
    )
    extract_token_cache_path: str = Field(
        default=str(DATA_DIR / "token_metadata_cache.json"),
        description="Path to token metadata cache file.",
    )
    extract_fanout_levels: int = Field(
        default=0,
        description="Fan-out depth (0 disables).",
    )
    extract_fanout_base_days: int = Field(
        default=30,
        description="Base lookback days for fan-out level 1.",
    )
    extract_fanout_base_tx: int = Field(
        default=100,
        description="Base transfer cap for fan-out level 1.",
    )
    extract_fanout_decay: float = Field(
        default=0.5,
        description="Decay factor per fan-out level.",
    )
    extract_fanout_max_nodes: int = Field(
        default=300,
        description="Max total nodes in fan-out graph.",
    )
    extract_fanout_max_neighbors_per_node: int = Field(
        default=100,
        description="Max neighbors per node when expanding.",
    )
    extract_endpoint: Optional[str] = Field(
        default=None,
        description="Override RPC endpoint for extraction.",
    )
    extract_notes_path: str = Field(
        default=str(PROJECT_ROOT / ".notes" / "notes.txt"),
        description="Notes path containing the default Alchemy endpoint.",
    )
    include_infra: bool = Field(
        default=True,
        description="Include infra-behavior detection output.",
    )
    dust_threshold: float = Field(
        default=0.001,
        description="Dust threshold for normalized token balances.",
    )
    etherscan_enrich: bool = Field(
        default=False,
        description="Fetch earliest/latest tx timestamps from Etherscan.",
    )
    etherscan_timeout: int = Field(
        default=10,
        description="Timeout in seconds for Etherscan requests.",
    )
    etherscan_retries: int = Field(
        default=3,
        description="Retry count for Etherscan requests.",
    )
    etherscan_backoff: float = Field(
        default=1.0,
        description="Backoff seconds for Etherscan retries.",
    )


class ExtractCountRequest(BaseModel):
    address: str = Field(
        description="Address to count transfers for.",
    )
    count_days: int = Field(
        default=30,
        description="Lookback window in days for counting.",
    )
    count_timeout: int = Field(
        default=20,
        description="Timeout in seconds for count requests.",
    )
    count_max_count: int = Field(
        default=1000,
        description="Max transfers per page for counting.",
    )
    count_max_pages: int = Field(
        default=50,
        description="Max pages per direction for window count.",
    )
    count_include_all_time: bool = Field(
        default=False,
        description="Compute all-time transfer count (slower).",
    )
    count_all_time_max_pages: int = Field(
        default=200,
        description="Max pages per direction for all-time counts.",
    )
    count_endpoint: Optional[str] = Field(
        default=None,
        description="Override RPC endpoint for counting.",
    )
    count_notes_path: str = Field(
        default=str(PROJECT_ROOT / ".notes" / "notes.txt"),
        description="Notes path containing the default Alchemy endpoint.",
    )


class ExplainRequest(BaseModel):
    address: Optional[str] = Field(
        default=None,
        description="Address tied to the explanation (enables caching).",
    )
    reasons: list[str] = Field(default_factory=list)
    patterns: list[str] = Field(default_factory=list)
    use_cache: bool = Field(
        default=True,
        description="Use cached explanation when available.",
    )


class ExtractionRequest(BaseModel):
    address: Optional[str] = Field(
        default=None,
        description="Address to extract (loads data/extractions/<address>.json).",
    )
    payload: Optional[dict[str, Any]] = Field(
        default=None,
        description="Extraction JSON payload (overrides address file lookup).",
    )
    run_extract: bool = Field(
        default=True,
        description="Run extraction on the fly instead of loading from disk.",
    )
    save_extraction: bool = Field(
        default=False,
        description="Save extraction JSON to data/extractions when run_extract is true.",
    )
    extract_days: int = Field(
        default=30,
        description="Lookback window in days for extraction.",
    )
    extract_max_count: int = Field(
        default=1000,
        description="Max transfers per page for extraction.",
    )
    extract_max_total_transfers: int = Field(
        default=250,
        description="Max total transfers to fetch per address.",
    )
    extract_timeout: int = Field(
        default=20,
        description="Timeout in seconds for extraction requests.",
    )
    extract_include_all_time_count: bool = Field(
        default=False,
        description="Compute an all-time transfer count (slower).",
    )
    extract_all_time_max_pages: int = Field(
        default=200,
        description="Max pages per direction for all-time counts.",
    )
    extract_count_max_pages: int = Field(
        default=50,
        description="Max pages per direction for window counts.",
    )
    extract_token_cache_path: str = Field(
        default=str(DATA_DIR / "token_metadata_cache.json"),
        description="Path to token metadata cache file.",
    )
    extract_fanout_levels: int = Field(
        default=0,
        description="Fan-out depth (0 disables).",
    )
    extract_fanout_base_days: int = Field(
        default=30,
        description="Base lookback days for fan-out level 1.",
    )
    extract_fanout_base_tx: int = Field(
        default=100,
        description="Base transfer cap for fan-out level 1.",
    )
    extract_fanout_decay: float = Field(
        default=0.5,
        description="Decay factor per fan-out level.",
    )
    extract_fanout_max_nodes: int = Field(
        default=300,
        description="Max total nodes in fan-out graph.",
    )
    extract_fanout_max_neighbors_per_node: int = Field(
        default=100,
        description="Max neighbors per node when expanding.",
    )
    extract_endpoint: Optional[str] = Field(
        default=None,
        description="Override RPC endpoint for extraction.",
    )
    extract_notes_path: str = Field(
        default=str(PROJECT_ROOT / ".notes" / "notes.txt"),
        description="Notes path containing the default Alchemy endpoint.",
    )


def _load_payload_from_file(address: str) -> dict:
    address = address.lower()
    path = EXTRACTIONS_DIR / f"{address}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Extraction not found for {address}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in {path}") from exc


def _resolve_payload(request: AnalyzeRequest) -> tuple[dict, str]:
    if request.payload:
        payload = request.payload
        req_addr = (request.address or "").lower()
        payload_addr = (payload.get("address") or "").lower()
        if req_addr and payload_addr and req_addr != payload_addr:
            raise HTTPException(
                status_code=400,
                detail="Address mismatch between request and payload.",
            )
        return payload, "payload"
    if not request.address:
        raise HTTPException(status_code=400, detail="Provide address or payload.")
    if request.run_extract:
        endpoint = _alchemy_endpoint(request.extract_endpoint, request.extract_notes_path)
        payload = extract_for_address(
            endpoint=endpoint,
            address=request.address,
            days=request.extract_days,
            timeout=request.extract_timeout,
            max_count=request.extract_max_count,
            include_all_time_count=request.extract_include_all_time_count,
            all_time_max_pages=request.extract_all_time_max_pages,
            window_count_max_pages=request.extract_count_max_pages,
            token_cache_path=Path(request.extract_token_cache_path),
            max_total_transfers=request.extract_max_total_transfers,
            fanout_levels=request.extract_fanout_levels,
            fanout_base_days=request.extract_fanout_base_days,
            fanout_base_tx=request.extract_fanout_base_tx,
            fanout_decay=request.extract_fanout_decay,
            fanout_max_nodes=request.extract_fanout_max_nodes,
            fanout_max_neighbors_per_node=request.extract_fanout_max_neighbors_per_node,
        )
        if request.save_extraction:
            EXTRACTIONS_DIR.mkdir(parents=True, exist_ok=True)
            out_path = EXTRACTIONS_DIR / f"{request.address.lower()}.json"
            out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return payload, "extract"
    return _load_payload_from_file(request.address), "file"


def _ddb_client():
    global _DDB_CLIENT
    if _DDB_CLIENT is None:
        _LOGGER.info("ddb init table=%s region=%s", _DDB_TABLE, _DDB_REGION)
        _DDB_CLIENT = boto3.client("dynamodb", region_name=_DDB_REGION)
    return _DDB_CLIENT


def _ddb_put_item(item: dict) -> bool:
    if not _DDB_TABLE:
        return False
    normalized = _normalize_ddb_value(item)
    marshalled = {key: _DDB_SERIALIZER.serialize(value) for key, value in normalized.items()}
    _ddb_client().put_item(TableName=_DDB_TABLE, Item=marshalled)
    return True


def _ddb_get_item(address: str, record_type: str) -> Optional[dict]:
    if not _DDB_TABLE:
        _LOGGER.info("ddb disabled record_type=%s address=%s", record_type, address.lower())
        return None
    _LOGGER.info("ddb get_item start record_type=%s address=%s", record_type, address.lower())
    response = _ddb_client().get_item(
        TableName=_DDB_TABLE,
        Key={
            "address": {"S": address.lower()},
            "record_type": {"S": record_type},
        },
    )
    item = response.get("Item")
    if not item:
        _LOGGER.info("ddb get_item miss record_type=%s address=%s", record_type, address.lower())
        return None
    _LOGGER.info("ddb get_item hit record_type=%s address=%s", record_type, address.lower())
    return {key: _DDB_DESERIALIZER.deserialize(value) for key, value in item.items()}


def _is_fresh(updated_at: Optional[int]) -> bool:
    if not updated_at:
        return False
    age_seconds = int(time.time()) - int(updated_at)
    return age_seconds <= _DDB_REFRESH_DAYS * 24 * 60 * 60


def _normalize_ddb_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_ddb_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_normalize_ddb_value(item) for item in value]
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def _strip_json_fence(text: str) -> str:
    trimmed = text.strip()
    if trimmed.startswith("```"):
        lines = trimmed.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return trimmed


def _try_parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _coerce_explain_summary(summary: Any) -> dict[str, str]:
    if isinstance(summary, dict):
        if summary.keys() == {"summary"}:
            return _coerce_explain_summary(summary.get("summary"))
        cleaned = {
            key: value.strip()
            for key, value in summary.items()
            if isinstance(value, str) and value.strip()
        }
        if cleaned:
            return cleaned
    if isinstance(summary, str):
        parsed = _try_parse_json(_strip_json_fence(summary))
        if parsed is not None:
            return _coerce_explain_summary(parsed)
        return {"en": summary.strip()}
    if summary is None:
        return {"en": ""}
    return {"en": str(summary)}


def _openrouter_explain(reasons: list[str], patterns: list[str]) -> dict[str, str]:
    if not _OPENROUTER_KEY:
        raise HTTPException(status_code=500, detail="Missing OPENROUTER_API_KEY.")
    payload = {
        "messages": [
            {"role": "system", "content": _EXPLAIN_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Behavior reasons:\n"
                    f"{json.dumps(reasons)}\n\n"
                    "Pattern signals:\n"
                    f"{json.dumps(patterns)}"
                ),
            },
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {_OPENROUTER_KEY}",
        "Content-Type": "application/json",
    }
    last_error = None
    for model in _OPENROUTER_MODELS:
        try:
            payload["model"] = model
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=45,
            )
            if response.status_code >= 400:
                _LOGGER.warning(
                    "openrouter error model=%s status=%s body=%s",
                    model,
                    response.status_code,
                    response.text[:300],
                )
                last_error = response.text
                continue
            data = response.json()
            content = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            if content:
                return _coerce_explain_summary(content)
            last_error = "Empty response content"
        except Exception as exc:
            last_error = str(exc)
            _LOGGER.exception("openrouter exception model=%s", model)
    raise HTTPException(status_code=502, detail=f"Explain failed: {last_error}")


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


def _decompress_payload(entry: dict) -> Optional[dict]:
    if not entry:
        return None
    if entry.get("encoding") != "gzip+base64":
        return entry if isinstance(entry, dict) else None
    data = entry.get("data")
    if not data:
        return None
    raw = gzip.decompress(base64.b64decode(data))
    return json.loads(raw.decode("utf-8"))


def _store_extraction_payload(payload: dict, source: str) -> None:
    if not _DDB_TABLE:
        return
    address = (payload.get("address") or "").lower()
    if not address:
        return
    now_ts = int(time.time())
    ttl = now_ts + _DDB_TTL_DAYS * 24 * 60 * 60
    item = {
        "address": address,
        "record_type": "extraction",
        "source": source,
        "payload": _compress_payload(payload),
        "updated_at": now_ts,
        "ttl": ttl,
    }
    _ddb_put_item(item)


def _store_analysis_result(address: str, result: dict) -> None:
    if not _DDB_TABLE:
        return
    if not address:
        return
    now_ts = int(time.time())
    ttl = now_ts + _DDB_TTL_DAYS * 24 * 60 * 60
    item = {
        "address": address.lower(),
        "record_type": "analysis",
        "result": result,
        "updated_at": now_ts,
        "ttl": ttl,
    }
    _ddb_put_item(item)


def _load_cached_analysis(address: str) -> Optional[dict]:
    if not _DDB_TABLE:
        return None
    item = _ddb_get_item(address, "analysis")
    if not item:
        return None
    if not _is_fresh(item.get("updated_at")):
        return None
    return item.get("result")


def _load_cached_extraction(address: str) -> Optional[dict]:
    if not _DDB_TABLE:
        return None
    item = _ddb_get_item(address, "extraction")
    if not item:
        return None
    if not _is_fresh(item.get("updated_at")):
        return None
    payload = item.get("payload")
    if isinstance(payload, dict):
        return _decompress_payload(payload)
    return None


def _store_explain_result(address: str, summary: str) -> None:
    if not _DDB_TABLE:
        return
    if not address:
        return
    now_ts = int(time.time())
    ttl = now_ts + _DDB_TTL_DAYS * 24 * 60 * 60
    item = {
        "address": address.lower(),
        "record_type": "explain",
        "result": {"summary": summary},
        "updated_at": now_ts,
        "ttl": ttl,
    }
    _ddb_put_item(item)


def _load_cached_explain(address: str) -> Optional[dict]:
    if not _DDB_TABLE:
        return None
    item = _ddb_get_item(address, "explain")
    if not item:
        return None
    if not _is_fresh(item.get("updated_at")):
        return None
    return item.get("result")


app = FastAPI(
    title="Probo API",
    version="0.1",
    root_path=os.getenv("PROBO_ROOT_PATH", ""),
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_http_request(request, call_next):
    _LOGGER.info(
        "http request method=%s path=%s client=%s",
        request.method,
        request.url.path,
        request.client.host if request.client else "unknown",
    )
    response = await call_next(request)
    _LOGGER.info("http response status=%s path=%s", response.status_code, request.url.path)
    return response

_load_dotenv()
_STABLECOINS = load_stablecoins(str(DATA_DIR / "stablecoins.json"))
_ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY")


@app.on_event("startup")
def _log_startup() -> None:
    _LOGGER.info(
        "startup ddb_table=%s ddb_region=%s refresh_days=%s ttl_days=%s",
        _DDB_TABLE,
        _DDB_REGION,
        _DDB_REFRESH_DAYS,
        _DDB_TTL_DAYS,
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ping")
def ping() -> dict:
    return {"status": "ok"}


@app.post("/analyze")
def analyze(req: AnalyzeRequest) -> dict:
    _LOGGER.info(
        "analyze start address=%s run_extract=%s include_infra=%s",
        (req.address or "").lower(),
        req.run_extract,
        req.include_infra,
    )
    _LOGGER.info("analyze ddb_enabled=%s", bool(_DDB_TABLE))
    if req.payload:
        payload, payload_source = _resolve_payload(req)
    else:
        address = (req.address or "").lower()
        if not address:
            raise HTTPException(status_code=400, detail="Provide address or payload.")
        cached_analysis = _load_cached_analysis(address)
        if cached_analysis:
            _LOGGER.info("analyze cache_hit=analysis address=%s", address)
            return cached_analysis
        cached_extraction = _load_cached_extraction(address)
        if cached_extraction:
            _LOGGER.info("analyze cache_hit=extraction address=%s", address)
            payload = cached_extraction
            payload_source = "cache"
        else:
            payload, payload_source = _resolve_payload(req)
            _LOGGER.info("analyze payload_source=%s address=%s", payload_source, address)

    try:
        _store_extraction_payload(payload, payload_source)
    except Exception:
        _LOGGER.exception("Failed to store extraction payload in DynamoDB")
    _LOGGER.info("analyze running analysis address=%s", (payload.get("address") or "").lower())
    result = analyze_payload(
        payload,
        stablecoins=_STABLECOINS,
        dust_threshold=req.dust_threshold,
    )
    output = {
        "address": result.address,
        "score": result.score,
        "label": result.label,
        "reasons": [reason.__dict__ for reason in result.reasons],
        "features": result.features,
    }
    if req.include_infra:
        output["infra"] = summarize_infra(payload)
    if req.etherscan_enrich and _ETHERSCAN_KEY:
        earliest_ts, latest_ts = fetch_etherscan_tx_bounds(
            result.address,
            api_key=_ETHERSCAN_KEY,
            timeout=req.etherscan_timeout,
            retries=req.etherscan_retries,
            backoff=req.etherscan_backoff,
        )
        output["etherscan"] = {
            "earliest_tx_ts": earliest_ts,
            "latest_tx_ts": latest_ts,
        }
    try:
        _store_analysis_result(result.address, output)
    except Exception:
        _LOGGER.exception("Failed to store analysis result in DynamoDB")
    _LOGGER.info("analyze complete address=%s score=%s label=%s", result.address, result.score, result.label)
    return output


@app.post("/explain")
def explain(req: ExplainRequest) -> dict:
    reasons = [item for item in req.reasons if item]
    patterns = [item for item in req.patterns if item]
    address = (req.address or "").lower()
    if req.use_cache and address:
        cached = _load_cached_explain(address)
        if cached:
            _LOGGER.info("explain cache_hit=explain address=%s", address)
            return cached
    summary = _openrouter_explain(reasons, patterns)
    try:
        _store_explain_result(address, summary)
    except Exception:
        _LOGGER.exception("Failed to store explain result in DynamoDB")
    return {"summary": summary}


@app.post("/extraction")
def extraction(req: ExtractionRequest) -> dict:
    _LOGGER.info("extraction start address=%s run_extract=%s", (req.address or "").lower(), req.run_extract)
    if req.payload:
        payload, payload_source = _resolve_payload(req)
    else:
        address = (req.address or "").lower()
        if not address:
            raise HTTPException(status_code=400, detail="Provide address or payload.")
        cached_extraction = _load_cached_extraction(address)
        if cached_extraction:
            _LOGGER.info("extraction cache_hit=extraction address=%s", address)
            return {"address": address, "source": "cache", "payload": cached_extraction}
        payload, payload_source = _resolve_payload(req)
        _LOGGER.info("extraction payload_source=%s address=%s", payload_source, address)

    try:
        _store_extraction_payload(payload, payload_source)
    except Exception:
        _LOGGER.exception("Failed to store extraction payload in DynamoDB")
    _LOGGER.info("extraction complete address=%s", (payload.get("address") or "").lower())
    return {"address": payload.get("address"), "source": payload_source, "payload": payload}


@app.post("/extract-count")
def extract_count(req: ExtractCountRequest) -> dict:
    _LOGGER.info("extract-count start address=%s days=%s", req.address.lower(), req.count_days)
    endpoint = _alchemy_endpoint(req.count_endpoint, req.count_notes_path)
    result = count_transfers_for_address(
        endpoint=endpoint,
        address=req.address,
        days=req.count_days,
        timeout=req.count_timeout,
        max_count=req.count_max_count,
        max_pages=req.count_max_pages,
        include_all_time_count=req.count_include_all_time,
        all_time_max_pages=req.count_all_time_max_pages,
    )
    _LOGGER.info(
        "extract-count complete address=%s window_count=%s all_time=%s",
        req.address.lower(),
        result.get("window_count"),
        result.get("all_time_count"),
    )
    return result


_MANGUM_HANDLER = Mangum(app)


def handler(event, context):
    request_context = event.get("requestContext", {}) if isinstance(event, dict) else {}
    http_ctx = request_context.get("http", {}) if isinstance(request_context, dict) else {}
    _LOGGER.info(
        "lambda event method=%s path=%s stage=%s source=%s",
        http_ctx.get("method"),
        event.get("rawPath") if isinstance(event, dict) else None,
        request_context.get("stage"),
        http_ctx.get("sourceIp"),
    )
    return _MANGUM_HANDLER(event, context)
