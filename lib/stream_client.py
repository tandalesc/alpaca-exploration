from typing import Optional
from alpaca.data.models import Bar, BarSet
from alpaca.data.live import StockDataStream, CryptoDataStream
import pandas as pd

from lib.stream_analyzer import MetricType, StreamAnalyzer


def dataframe_from_bars(bar: Bar | BarSet) -> pd.DataFrame:
    df = bar.df
    # Update indexes to use only timestamp
    df = df.droplevel("symbol").dropna()
    df.index = df.index.tz_convert("US/Eastern")
    return df


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


class StreamClient:
    dataframes = DataFrameManager()
    stream: StockDataStream | CryptoDataStream

    def __init__(self, data_stream: StockDataStream | CryptoDataStream):
        self.stream = data_stream

    async def _handler(self, bar: Bar):
        sym = bar.symbol
        new_chunk = dataframe_from_bars(bar)
        self.dataframes.add(sym, new_chunk)

    def graph_stream(self, symbol: str):
        if not self.dataframes.contains_symbol(symbol):
            raise ValueError(f"Symbol {symbol} is not available.")
        df = self.dataframes.get(symbol)
        analyzer = StreamAnalyzer(df)
        analyzer.graph(metrics=[MetricType.BB])

    def start_stream(self, symbol: str, initial_data: Optional[pd.DataFrame] = None):
        if self.dataframes.contains_symbol(symbol):
            raise ValueError(
                f"Symbol {symbol} is already streaming. Recommendation: Stop the stream before starting it again."
            )
        self.stream.subscribe_bars(self._handler, symbol)
        if initial_data is not None:
            self.dataframes.add(symbol, initial_data)

    def stop_stream(self, symbol: str):
        if not self.dataframes.contains_symbol(symbol):
            raise ValueError(
                f"Symbol {symbol} is not streaming. Recommendation: Start the stream before stopping it."
            )
        self.stream.unsubscribe_bars(symbol)
