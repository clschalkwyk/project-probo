"""CLI for Project Probo."""

from __future__ import annotations

import argparse
import json

from .blocknumber import get_block_number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="probo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    block_parser = subparsers.add_parser("blocknumber", help="Fetch latest Ethereum block number")
    block_parser.add_argument("--endpoint", help="Override RPC endpoint URL")
    block_parser.add_argument(
        "--notes-path",
        default=".notes/notes.txt",
        help="Path to notes file containing the RPC endpoint",
    )
    block_parser.add_argument("--timeout", type=int, default=10, help="Request timeout in seconds")
    block_parser.add_argument(
        "--json",
        action="store_true",
        help="Print as JSON (hex and dec)",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "blocknumber":
        result = get_block_number(
            endpoint=args.endpoint,
            notes_path=args.notes_path,
            timeout=args.timeout,
        )
        if args.json:
            print(json.dumps({"hex": result.hex, "dec": result.dec}))
        else:
            print(f"hex: {result.hex}")
            print(f"dec: {result.dec}")
        return 0

    parser.error("Unknown command")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
