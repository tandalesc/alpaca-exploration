from datetime import datetime
from alpaca.data.models import Bar
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.live import CryptoDataStream
from alpaca.data.timeframe import TimeFrame
import mplfinance as mpf
import pandas as pd


def get_recent_crypto_data(symbol: str) -> pd.DataFrame:
    # No keys required for crypto data
    client = CryptoHistoricalDataClient()
    # Creating request object
    request_params = CryptoBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=datetime.now() - pd.Timedelta(minutes=2),
        end=datetime.now(),
    )
    # Retrieve daily bars for Bitcoin in a DataFrame and printing it
    btc_bars = client.get_crypto_bars(request_params)
    # Convert to dataframe
    # Update indexes to use only timestamp
    df = btc_bars.df.droplevel("symbol").dropna()
    # df.index = df.index.tz_convert("US/Eastern")
    return df


def dataframe_from_bar(bar: Bar) -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "low": bar.low,
                "high": bar.high,
                "open": bar.open,
                "close": bar.close,
                "volume": bar.volume,
            }
        ],
        index=[bar.timestamp],
    )
    # df.index = df.index.tz_convert("US/Eastern")
    return df


def get_crypto_client():
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")
    return CryptoStreamClient(api_key, api_secret)


class CryptoStreamClient:
    stream: CryptoDataStream
    dataframes: dict[str, pd.DataFrame] = {}

    def __init__(self, api_key: str, api_secret: str):
        self.stream = CryptoDataStream(api_key, api_secret)

    async def _handler(self, bar: Bar):
        sym = bar.symbol
        new_chunk = dataframe_from_bar(bar)
        if sym not in self.dataframes:
            self.dataframes[sym] = new_chunk
        else:
            self.dataframes[sym] = pd.concat([self.dataframes[sym], new_chunk])

    def show_stream(self, symbol: str):
        if symbol not in self.dataframes:
            raise ValueError(f"Symbol {symbol} is not available.")
        mpf.plot(self.dataframes[symbol], type="candlestick")

    def start_stream(self, symbol: str):
        self.stream.subscribe_bars(self._handler, symbol)
        if symbol not in self.dataframes:
            self.dataframes[symbol] = get_recent_crypto_data(symbol)

    def stop_stream(self, symbol: str):
        self.stream.unsubscribe_bars(symbol)


client = get_crypto_client()

client.start_stream("BTC/USD")
# client.show_stream("BTC/USD")

client.stream.run()

client.show_stream("BTC/USD")
