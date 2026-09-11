"""Provides the small command-line interface backed by the public SDK."""

import argparse
import asyncio
from dataclasses import asdict

import orjson

from marketstream.sdk import SUPPORTED_EVENTS, MarketStream


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketstream")
    commands = parser.add_subparsers(dest="command", required=True)
    stream = commands.add_parser("stream", help="Stream normalized Binance market events.")
    stream.add_argument("--symbols", nargs="+", required=True)
    stream.add_argument("--events", nargs="+", choices=SUPPORTED_EVENTS, default=["trade"])
    return parser


async def stream_events(symbols: list[str], events: list[str]) -> None:
    stream = MarketStream(symbols=symbols, events=events)

    try:
        async for event in stream:
            print(orjson.dumps(asdict(event)).decode())
    finally:
        await stream.aclose()


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    try:
        asyncio.run(stream_events(args.symbols, args.events))
    except KeyboardInterrupt:
        pass
