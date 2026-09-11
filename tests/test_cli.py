"""Verifies that CLI arguments map to the public streaming command."""

import pytest

from marketstream.cli import build_parser


def test_stream_command_arguments():
    args = build_parser().parse_args(
        ["stream", "--symbols", "BTCUSDT", "ETHUSDT", "--events", "trade", "depth"]
    )

    assert args.command == "stream"
    assert args.symbols == ["BTCUSDT", "ETHUSDT"]
    assert args.events == ["trade", "depth"]


def test_stream_command_rejects_unknown_event():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["stream", "--symbols", "BTCUSDT", "--events", "unknown"])
