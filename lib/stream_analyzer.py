from typing import cast
import mplfinance as mpf
import pandas as pd

from lib.bollinger_bands import BollingerBandSignals


class MetricType:
    BB = "bb"


marker_plot = {
    "type": "scatter",
    "markersize": 20,
    "panel": 0,
}


class StreamAnalyzer:
    df: pd.DataFrame
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    bb_signals: BollingerBandSignals

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.start_time = self.df.index.min()
        self.end_time = self.df.index.max()
        self.bb_signals = BollingerBandSignals(self.df)

    def graph(self, trim_hours: int = 1, metrics=[MetricType.BB]):
        # Trimming the signal allows us to skip the gap from moving avg and bb calculations.
        def trim_signal(sig: pd.DataFrame | pd.Series):
            start_time = self.start_time + pd.Timedelta(hours=trim_hours)
            return sig.truncate(before=start_time)

        subplots = []
        # Create subplots for the Bollinger Bands, bb high, and bb low markers if selected
        if MetricType.BB in metrics:
            bb_bands = trim_signal(self.bb_signals.bands)
            bb_low = trim_signal(self.bb_signals.low)
            bb_high = trim_signal(self.bb_signals.high)
            bb_plots = [
                mpf.make_addplot(bb_bands),
                mpf.make_addplot(bb_high, **marker_plot, marker="^", color="red"),
                mpf.make_addplot(bb_low, **marker_plot, marker="v", color="green"),
            ]
            subplots.extend(bb_plots)

        # Plot the data with the added subplots
        mpf.plot(
            data=trim_signal(self.df),
            figratio=(10, 5),
            addplot=subplots,
            type="line",
            xlabel="Date",
            ylabel="Price",
        )
