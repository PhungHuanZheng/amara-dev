"""
This module provides functionality to preprocess data before being used
as inputs for a time series forecasting machine learning model, e.g.:
`ARIMA`, `SARIMAX`, `ExponentialSmooting`, etc.
"""


from __future__ import annotations

from typing import Literal

import pandas as pd
import numpy as np


def create_datetime_index(data: pd.DataFrame, datetime_col: str, format: str | Literal['auto'] = None, drop: bool = True) -> pd.DataFrame:
    """
    Creates a datetime index for the pandas DataFrame passed.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        DataFrame to create a datetime index for.
    `datetime_col` : `str`
        Name of the datetime column in the DataFrame passed.
    `format` : `str`, `default=None`
        Datetime format of the datetime column if passed as string, `None` if already `datetime` type.
    `drop` : `bool`, `default=True`
        Whether or not to drop the remaining datetime column.

    Returns
    -------
    `pd.DataFrame`
        Pandas DataFrame with datetime index.
    """

    # call by value
    data = data.copy(deep=True)

    # parse datetime column
    if format is not None:
        if format == 'auto':
            datetime_index = pd.to_datetime(data[datetime_col], infer_datetime_format=True, dayfirst=True)
        else:
            datetime_index = pd.to_datetime(data[datetime_col], format=format, dayfirst=True)
    else:
        datetime_index = data[datetime_col]
    
    data.index = pd.DatetimeIndex(datetime_index, freq=datetime_index.dt.freq)

    # drop
    if drop:
        data.drop(datetime_col, axis=1, inplace=True)

    return data
