import numpy as np
import pandas as pd
from ta.volatility import BollingerBands


class BollingerBandSignals:
    bands: pd.DataFrame
    high: pd.Series
    low: pd.Series

    def __init__(self, df: pd.DataFrame):
        indicator_bb = BollingerBands(close=df.close, window=20, window_dev=2)
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
    raw_volatility = (series.max() - series.min()) / series.median()
    signal_offset = 0.05 * raw_volatility
    skip = False  # skip repeated markers
    signal = []
    comparison = upper_bb
    for date, value in series.items():
        comparison_value = comparison.loc[date]
        if value > comparison_value and skip is False:
            # TODO: move this to analysis class, leave signals raw
            signal.append(value * (1 + signal_offset))
            skip = True
        else:
            signal.append(np.nan)
            if skip is True:
                skip = False
    return pd.Series(signal, index=series.index)


def low_bb_signal(series: pd.Series, lower_bb: pd.Series) -> list[float]:
    raw_volatility = (series.max() - series.min()) / series.median()
    signal_offset = 0.08 * raw_volatility
    skip = False  # skip repeated markers
    signal = []
    comparison = lower_bb
    for date, value in series.items():
        comparison_value = comparison.loc[date]
        if value < comparison_value and skip is False:
            # TODO: move this to analysis class, leave signals raw
            signal.append(value * (1 - signal_offset))
            skip = True
        else:
            signal.append(np.nan)
            if skip is True:
                skip = False
    return pd.Series(signal, index=series.index)
