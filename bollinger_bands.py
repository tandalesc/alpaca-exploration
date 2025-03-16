import numpy as np
import pandas as pd
from ta.volatility import BollingerBands


class BollingerBandSignals:
    bands: pd.DataFrame
    high: pd.Series
    low: pd.Series

    def __init__(self, df: pd.DataFrame):
        indicator_bb = BollingerBands(close=df.close, window=15, window_dev=2.4)
        self.bands = pd.DataFrame(
            {
                "upper": indicator_bb.bollinger_hband(),
                "lower": indicator_bb.bollinger_lband(),
                "middle": indicator_bb.bollinger_mavg(),
            }
        )
        self.high = high_bb_signal(df.close, indicator_bb.bollinger_hband())
        self.low = low_bb_signal(df.close, indicator_bb.bollinger_lband())


def high_bb_signal(series: pd.Series, upper_bb: pd.Series) -> list[float]:
    skip = False  # skip repeated markers
    signal = []
    comparison = upper_bb
    for date, value in series.items():
        comparison_value = comparison.loc[date]
        if value > comparison_value and skip is False:
            signal.append(value * 1.002)
            skip = True
        else:
            signal.append(np.nan)
            if skip is True:
                skip = False
    return pd.Series(signal, index=series.index)


def low_bb_signal(series: pd.Series, lower_bb: pd.Series) -> list[float]:
    skip = False  # skip repeated markers
    signal = []
    comparison = lower_bb
    for date, value in series.items():
        comparison_value = comparison.loc[date]
        if value < comparison_value and skip is False:
            signal.append(value * 0.998)
            skip = True
        else:
            signal.append(np.nan)
            if skip is True:
                skip = False
    return pd.Series(signal, index=series.index)
