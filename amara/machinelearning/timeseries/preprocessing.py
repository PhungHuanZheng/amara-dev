"""
This module provides functionality to preprocess data before being used
as inputs for a time series forecasting machine learning model, e.g.:
`ARIMA`, `SARIMAX`, `ExponentialSmooting`, etc.
"""


from __future__ import annotations

import pandas as pd
import numpy as np


def create_datetime_index(data: pd.DataFrame, datetime_col: str, drop: bool = True) -> pd.DataFrame:
    """
    Creates a datetime index for the pandas DataFrame passed.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        DataFrame to create a datetime index for.
    `datetime_col` : `str`
        Name of the datetime column in the DataFrame passed.
    `drop` : `bool`, `default=True`
        Whether or not to drop the remaining datetime column.

    Returns
    -------
    `pd.DataFrame`
        Pandas DataFrame with datetime index.
    """

    # call by value
    data = data.copy(deep=True)

    # set datetime columm
    data.index = pd.DatetimeIndex(data[datetime_col], freq=data[datetime_col].dt.freq)

    # drop
    if drop:
        data.drop(datetime_col, axis=1, inplace=True)

    return data
