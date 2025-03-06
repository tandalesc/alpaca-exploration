from alpaca.data.models import Bar
from alpaca.data.live import CryptoDataStream
import mplfinance as mpf
import pandas as pd


def get_crypto_stream():
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")
    return CryptoDataStream(api_key, api_secret)


values: list[Bar] = []


async def handler(bar: Bar):
    values.append(bar)
    data = [
        {
            "low": b.low,
            "high": b.high,
            "open": b.open,
            "close": b.close,
            "volume": b.volume,
        }
        for b in values
    ]
    index = [b.timestamp for b in values]
    df = pd.DataFrame(data, index=index).dropna()
    mpf.plot(df)


stream = get_crypto_stream()
stream.subscribe_bars(handler, "BTC/USD")
stream.run()
