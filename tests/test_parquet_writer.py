import pyarrow.parquet as pq

from fast_orderbook.storage.parquet import BronzeParquetWriter


def test_write_trade_batch(tmp_path):
    writer = BronzeParquetWriter(str(tmp_path))

    events = [
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": 1,
            "price": 50000.0,
            "quantity": 0.1,
            "event_time_ms": 1000,
            "trade_time_ms": 1001,
            "is_buyer_maker": False,
        },
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": 2,
            "price": 50001.0,
            "quantity": 0.2,
            "event_time_ms": 1002,
            "trade_time_ms": 1003,
            "is_buyer_maker": True,
        },
    ]

    path = writer.write_batch(
        "market.trades",
        events,
    )

    assert path.exists()

    table = pq.read_table(path)

    assert table.num_rows == 2
    assert table.column("symbol").to_pylist() == [
        "BTCUSDT",
        "BTCUSDT",
    ]


def test_write_depth_batch(tmp_path):
    writer = BronzeParquetWriter(str(tmp_path))

    events = [
        {
            "exchange": "binance",
            "symbol": "ETHUSDT",
            "event_time_ms": 2000,
            "first_update_id": 10,
            "final_update_id": 11,
            "bids": [[2400.0, 1.5]],
            "asks": [[2401.0, 2.0]],
        }
    ]

    path = writer.write_batch(
        "market.depth",
        events,
    )

    table = pq.read_table(path)

    assert table.num_rows == 1
    assert table.column("symbol")[0].as_py() == "ETHUSDT"


def test_dataset_directory_isolated(tmp_path):
    writer = BronzeParquetWriter(str(tmp_path))

    event = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "price": 50000.0,
        "quantity": 0.1,
        "event_time_ms": 1000,
        "trade_time_ms": 1001,
        "is_buyer_maker": False,
    }

    path = writer.write_batch(
        "market.trades",
        [event],
    )

    assert "trades" in path.parts
    assert "BTCUSDT" in path.parts