from datetime import datetime, timezone
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.live import CryptoDataStream
from alpaca.data.timeframe import TimeFrame
import pandas as pd

from lib.stream_client import StreamClient, dataframe_from_bars

eastern = timezone(pd.Timedelta(hours=-4), "US/Eastern")


def get_recent_crypto_data(
    symbol: str, buffer_hours: int, timezone: timezone = eastern
) -> pd.DataFrame:
    # No keys required for crypto data
    client = CryptoHistoricalDataClient()
    time_period = pd.Timedelta(hours=buffer_hours)
    time_period_end = datetime.now(tz=timezone)
    time_period_start = datetime.now(tz=timezone) - time_period
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
    df = dataframe_from_bars(btc_bars)
    return df


class CryptoStreamClient(StreamClient):
    def __init__(self, api_key: str, api_secret: str):
        StreamClient.__init__(self, CryptoDataStream(api_key, api_secret))

    def start_stream(self, symbol: str, buffer_hours: int):
        recent_data = get_recent_crypto_data(symbol, buffer_hours=buffer_hours)
        StreamClient.start_stream(self, symbol, initial_data=recent_data)
