# Data Acquisition Spec (Ethereum Mainnet)

This document defines the explicit data requirements for Probo MVP address scoring,
and where to retrieve them. Alchemy is the primary source; Etherscan is fallback only.

## Configuration

- Alchemy HTTP endpoint:
  `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}`
- Etherscan API endpoint (fallback):
  `https://api.etherscan.io/api?apikey=${ETHERSCAN_API_KEY}`

## Time Window

- Default analysis window: last 30 days (configurable).
- Compute `fromBlock` using Alchemy `eth_getBlockByNumber` with a binary search on
  block timestamps.
- Fallback: use Etherscan `block` / `getblocknobytime` if Alchemy is insufficient.

## Data Requirements and Sources

### 1) Current block and timestamps

- Alchemy:
  - `eth_blockNumber`
  - `eth_getBlockByNumber`
- Etherscan fallback:
  - `proxy` / `eth_blockNumber`
  - `proxy` / `eth_getBlockByNumber`
  - `block` / `getblocknobytime`

### 2) Transfers (all categories, 30-day window)

Use every category available from Alchemy.

- Alchemy: `alchemy_getAssetTransfers`
  - Categories: `external`, `internal`, `erc20`, `erc721`, `erc1155`
  - Pagination via `pageKey`
  - `withMetadata: true` to include `blockTimestamp`

- Etherscan fallback:
  - External: `account` / `txlist`
  - Internal: `account` / `txlistinternal`
  - ERC-20: `account` / `tokentx`
  - ERC-721: `account` / `tokennfttx`
  - ERC-1155: `account` / `token1155tx`

### 3) Token balances snapshot

- Alchemy: `alchemy_getTokenBalances`
- Etherscan fallback: `account` / `tokenbalance` (one token per call)

### 4) Contract interaction density (all transfers)

- For all transfer counterparties, check contract bytecode:
  - `eth_getCode` (`"0x"` => EOA; non-empty => contract)
  - Cache results per address for the duration of the request

### 5) Known bad exposure

- Compare all counterparties against a flagged list supplied by the user.
- Optional log queries against known bad contracts:
  - Alchemy: `eth_getLogs`
  - Etherscan fallback: `logs` / `getLogs`

## Alchemy Request Payloads (examples)

### `alchemy_getAssetTransfers` (all categories)

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "alchemy_getAssetTransfers",
  "params": [{
    "fromBlock": "0xSTART",
    "toBlock": "0xEND",
    "fromAddress": "0xADDR",
    "category": ["external","internal","erc20","erc721","erc1155"],
    "withMetadata": true,
    "excludeZeroValue": false,
    "maxCount": "0x3e8"
  }]
}
```

### `alchemy_getTokenBalances`

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "alchemy_getTokenBalances",
  "params": ["0xADDR"]
}
```

### `eth_getCode`

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "eth_getCode",
  "params": ["0xADDR", "latest"]
}
```

### `eth_getBlockByNumber`

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "eth_getBlockByNumber",
  "params": ["0xBLOCK", false]
}
```
