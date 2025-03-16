from datetime import datetime, timezone
from alpaca.data.models import Bar
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.live import CryptoDataStream
from alpaca.data.timeframe import TimeFrame
import mplfinance as mpf
import pandas as pd
from bollinger_bands import BollingerBandSignals

eastern = timezone(pd.Timedelta(hours=-4), "US/Eastern")


def get_recent_crypto_data(symbol: str, buffer_hours: int) -> pd.DataFrame:
    # No keys required for crypto data
    client = CryptoHistoricalDataClient()
    time_period = pd.Timedelta(hours=buffer_hours)
    time_period_end = datetime.now(tz=eastern)
    time_period_start = datetime.now(tz=eastern) - time_period
    # Creating request object
    request_params = CryptoBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=time_period_start,
        end=time_period_end,
    )
    # Retrieve daily bars for Bitcoin in a DataFrame and printing it
    btc_bars = client.get_crypto_bars(request_params)
    # Convert to dataframe
    # Update indexes to use only timestamp
    df = btc_bars.df.droplevel("symbol").dropna()
    df.index = df.index.tz_convert("US/Eastern")
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


class DataFrameManager:
    dataframes: dict[str, pd.DataFrame] = {}

    def __init__(self):
        self.dataframes = {}

    def add(self, symbol: str, df: pd.DataFrame):
        if symbol not in self.dataframes:
            self.dataframes[symbol] = df
        else:
            self.dataframes[symbol] = pd.concat([self.dataframes[symbol], df])

    def get(self, symbol: str) -> pd.DataFrame:
        return self.dataframes.get(symbol, None)

    def get_start_time(self, symbol: str) -> pd.Timestamp:
        return self.dataframes[symbol].index.min()

    def contains_symbol(self, symbol: str) -> bool:
        return symbol in self.dataframes


class CryptoStreamClient:
    dataframes = DataFrameManager()
    stream: CryptoDataStream

    def __init__(self, api_key: str, api_secret: str):
        self.stream = CryptoDataStream(api_key, api_secret)

    async def _handler(self, bar: Bar):
        sym = bar.symbol
        new_chunk = dataframe_from_bar(bar)
        self.dataframes.add(sym, new_chunk)

    def show_stream(self, symbol: str):
        if not self.dataframes.contains_symbol(symbol):
            raise ValueError(f"Symbol {symbol} is not available.")
        df = self.dataframes.get(symbol)
        bb_signals = BollingerBandSignals(df)

        # Trimming the signal allows us to skip the gap from moving avg and bb calculations.
        def trim_signal(sig):
            start_time = self.dataframes.get_start_time(symbol) + pd.Timedelta(hours=1)
            return sig.truncate(before=start_time)

        # Create subplots for the Bollinger Bands, bb high, and bb low markers
        bb_subplot = mpf.make_addplot(trim_signal(bb_signals.bands))
        bb_high_marker_subplot = mpf.make_addplot(
            trim_signal(bb_signals.high),
            type="scatter",
            markersize=20,
            marker="v",
            color="red",
            panel=0,
        )
        bb_low_marker_subplot = mpf.make_addplot(
            trim_signal(bb_signals.low),
            type="scatter",
            markersize=20,
            marker="^",
            color="green",
            panel=0,
        )

        # Plot the data with the added subplots
        subplts = [bb_subplot, bb_high_marker_subplot, bb_low_marker_subplot]
        mpf.plot(
            data=trim_signal(df),
            figratio=(10, 5),
            addplot=subplts,
            type="line",
            xlabel="Date",
            ylabel="Price",
        )

    def start_stream(self, symbol: str):
        self.stream.subscribe_bars(self._handler, symbol)
        self.dataframes.add(symbol, get_recent_crypto_data(symbol, buffer_hours=4))

    def stop_stream(self, symbol: str):
        if not self.dataframes.contains_symbol(symbol):
            raise ValueError(f"Symbol {symbol} is not available.")
        self.stream.unsubscribe_bars(symbol)
