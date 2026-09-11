# MarketStream

```bash
pip install marketstream
```

Stream trades from Python:

```python
import asyncio

from marketstream import MarketStream


async def main():
    async for trade in MarketStream("BTCUSDT").trades():
        print(trade)


asyncio.run(main())
```

Stream selected events from the command line:

```bash
marketstream stream --symbols BTCUSDT ETHUSDT --events trade depth
```

Supported event types are `trade`, `depth`, `bookTicker`, `ticker`, and `kline`.

## Run the full platform

Requirements: Python 3.11, OpenJDK 17, Docker with Docker Compose, and `curl`.

```bash
git clone https://github.com/cemalgurselkar/fast_orderbook.git
cd fast_orderbook
./setup.sh
```

After startup:

- Streamlit UI: http://localhost:8501
- FastAPI docs: http://localhost:8000/docs
- Airflow: http://localhost:8080

## Development

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check .
```

Architecture notes are available in [`project_architecture`](project_architecture/README.md).
