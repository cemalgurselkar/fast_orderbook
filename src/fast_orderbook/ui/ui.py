"""Displays live API snapshots and historical Gold metrics in Streamlit."""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

from fast_orderbook.config import settings

DEFAULT_API_URL = os.getenv(
    "MARKETSTREAM_API_URL",
    os.getenv("FAST_ORDERBOOK_API_URL", "http://localhost:8000"),
)
SYMBOLS = tuple(symbol.upper() for symbol in settings.symbols)


def fetch_json(base_url: str, path: str, params: dict | None = None):
    url = f"{base_url.rstrip('/')}{path}"

    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(url, headers={"Accept": "application/json"})

    try:
        with urlopen(request, timeout=5) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"API HTTP {error.code} hatası döndürdü.") from error
    except URLError as error:
        raise RuntimeError(f"API bağlantısı kurulamadı: {error.reason}") from error
    except (json.JSONDecodeError, TimeoutError) as error:
        raise RuntimeError(f"API yanıtı okunamadı: {error}") from error


def fetch_summary(base_url: str, symbol: str, limit: int) -> list[dict]:
    result = fetch_json(
        base_url,
        f"/markets/{symbol}/summary",
        {"limit": limit},
    )

    if not isinstance(result, list):
        raise TypeError("API özet endpoint'i beklenen listeyi döndürmedi.")

    return result


def fetch_live(base_url: str, symbol: str) -> dict:
    result = fetch_json(base_url, f"/markets/{symbol}/live")

    if not isinstance(result, dict):
        raise TypeError("API live endpoint'i beklenen nesneyi döndürmedi.")

    return result


def prepare_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)

    if frame.empty:
        return frame

    frame["window_start"] = pd.to_datetime(
        frame["window_start"],
        errors="coerce",
        utc=True,
    )

    numeric_columns = (
        "trade_count",
        "total_quantity",
        "quote_volume",
        "min_price",
        "max_price",
        "vwap",
    )

    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    return frame.dropna(subset=["window_start"]).sort_values("window_start")


def format_price(value) -> str:
    return f"{value:,.6f}" if value is not None else "—"


def render_live_market(snapshot: dict, fallback_price=None) -> None:
    latest_price = snapshot.get("latest_price") or fallback_price
    price, bid, ask = st.columns(3)
    price.metric("Son fiyat", format_price(latest_price))
    bid.metric("En iyi alış", format_price(snapshot.get("best_bid")))
    ask.metric("En iyi satış", format_price(snapshot.get("best_ask")))

    latest_event_at_ms = snapshot.get("latest_event_at_ms")

    if latest_event_at_ms is not None:
        timestamp = pd.to_datetime(latest_event_at_ms, unit="ms", utc=True)
        st.caption(
            f"Son canlı veri: {timestamp.strftime('%Y-%m-%d %H:%M:%S.%f UTC')} · "
            f"{snapshot.get('latest_event_type')}"
        )
    else:
        st.info("Seçili sembol için canlı Kafka verisi henüz alınmadı.")

    trades, order_book = st.columns(2)

    with trades:
        st.subheader("Son işlemler")
        recent_trades = snapshot.get("recent_trades", [])

        if recent_trades:
            trade_frame = pd.DataFrame(recent_trades)
            columns = ["price", "quantity", "is_buyer_maker", "trade_time_ms"]
            st.dataframe(trade_frame[columns], hide_index=True, use_container_width=True)
        else:
            st.caption("Henüz işlem verisi yok.")

    with order_book:
        st.subheader("Order book")
        bids = snapshot.get("bids", [])
        asks = snapshot.get("asks", [])
        row_count = max(len(bids), len(asks))

        if row_count:
            rows = []

            for index in range(row_count):
                bid_row = bids[index] if index < len(bids) else (None, None)
                ask_row = asks[index] if index < len(asks) else (None, None)
                rows.append(
                    {
                        "bid_price": bid_row[0],
                        "bid_quantity": bid_row[1],
                        "ask_price": ask_row[0],
                        "ask_quantity": ask_row[1],
                    }
                )

            st.dataframe(rows, hide_index=True, use_container_width=True)
        else:
            st.caption("Henüz depth verisi yok.")


def render_metrics(frame: pd.DataFrame) -> None:
    latest = frame.iloc[-1]
    previous_vwap = frame.iloc[-2]["vwap"] if len(frame) > 1 else None
    delta = latest["vwap"] - previous_vwap if previous_vwap is not None else None

    price, trades, quantity, volume = st.columns(4)

    price.metric(
        "VWAP",
        f"{latest['vwap']:,.4f}",
        f"{delta:+,.4f}" if delta is not None else None,
    )
    trades.metric("İşlem sayısı", f"{int(latest['trade_count']):,}")
    quantity.metric("Toplam miktar", f"{latest['total_quantity']:,.4f}")
    volume.metric("Quote hacmi", f"{latest['quote_volume']:,.2f}")

    st.caption(f"Son Gold penceresi: {latest['window_start'].strftime('%Y-%m-%d %H:%M:%S UTC')}")


def render_charts(frame: pd.DataFrame) -> None:
    st.subheader("Fiyat akışı")
    st.line_chart(
        frame,
        x="window_start",
        y=["vwap", "min_price", "max_price"],
        height=360,
    )

    trades, volume = st.columns(2)

    with trades:
        st.subheader("Dakikalık işlem sayısı")
        st.bar_chart(frame, x="window_start", y="trade_count", height=280)

    with volume:
        st.subheader("Dakikalık quote hacmi")
        st.bar_chart(frame, x="window_start", y="quote_volume", height=280)


def main() -> None:
    st.set_page_config(
        page_title="MarketStream",
        page_icon="📈",
        layout="wide",
    )

    st.title("MarketStream")
    st.caption("Kafka live-serving ve PostgreSQL Gold verilerinin hafif gösterim arayüzü")

    with st.sidebar:
        st.header("Ayarlar")
        api_url = st.text_input("API adresi", value=DEFAULT_API_URL)
        symbol = st.selectbox("Sembol", SYMBOLS)
        limit = st.slider("Gösterilecek dakika", min_value=10, max_value=240, value=60)
        manual_refresh = st.button("Refresh Live Data", type="primary", use_container_width=True)
        auto_refresh = st.toggle("Otomatik yenile", value=True)
        refresh_seconds = st.slider(
            "Yenileme aralığı (saniye)",
            min_value=1,
            max_value=30,
            value=2,
            disabled=not auto_refresh,
        )

    if manual_refresh:
        st.rerun()

    run_every = refresh_seconds if auto_refresh else None

    @st.fragment(run_every=run_every)
    def live_dashboard() -> None:
        try:
            health = fetch_json(api_url, "/health")

            if health.get("status") != "ok":
                st.warning("API çalışıyor ancak sağlık durumu beklenenden farklı.")

            live_snapshot = fetch_live(api_url, symbol)
        except (RuntimeError, TypeError) as error:
            st.error(str(error))
            st.info(
                "FastAPI'yi `uvicorn fast_orderbook.serving.api:app --reload` "
                "komutuyla başlatıp API adresini kontrol edin."
            )
            return

        try:
            frame = prepare_frame(fetch_summary(api_url, symbol, limit))
        except (RuntimeError, TypeError):
            frame = pd.DataFrame()

        fallback_price = frame.iloc[-1]["vwap"] if not frame.empty else None
        st.subheader(f"{symbol} canlı piyasa")
        render_live_market(live_snapshot, fallback_price)

        if frame.empty:
            st.warning(f"{symbol} için henüz tarihsel Gold verisi bulunmuyor.")
            return

        st.divider()
        st.subheader("Tarihsel Gold özeti")
        render_metrics(frame)
        render_charts(frame)

        with st.expander("Son kayıtlar"):
            st.dataframe(
                frame.sort_values("window_start", ascending=False),
                hide_index=True,
                use_container_width=True,
            )

    live_dashboard()


if __name__ == "__main__":
    main()
