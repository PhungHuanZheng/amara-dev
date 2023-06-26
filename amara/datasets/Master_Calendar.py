"""
This module provides functionality for the creation and customization of the Master
Calendar dataset, which provides a common dates table for inter-relation with all 
other tables.
"""


from __future__ import annotations

import warnings; warnings.filterwarnings('ignore')

import math
from datetime import datetime

import pandas as pd


class MasterCalendar:
    """
    Handles creation of common dates table.

    Attributes
    ----------
    `data` : `pd.DataFrame`
        Generates calendar data as a Pandas DataFrame object based on its `min_date` 
        and `max_date`

    Methods
    -------
    :func:`update_date_range`
        Updates the date range of the `MasterCalendar` object
    """

    def __init__(self) -> None:
        """
        Instantiates an instance of `amara.datasets.Master_Calendar.MasterCalendar`.
        """

        self.__min_date = datetime.max
        self.__max_date = datetime.min

    def update_date_range(self, new_min: str, new_max: str) -> None:
        """
        Updates the current earliest and latest dates.

        Parameters
        ----------
        `new_min` : str
            Datetime string to be parsed and compared against current `min_date`.
        `new_max` : str
            Datetime string to be parsed and compared against current `max_date`.
        """

        # parse date strings
        new_min: pd.Timestamp = pd.to_datetime(new_min, infer_datetime_format=True, dayfirst=True).to_pydatetime()
        new_max: pd.Timestamp = pd.to_datetime(new_max, infer_datetime_format=True, dayfirst=True).to_pydatetime()

        # update if larger bounds
        if new_min < self.__min_date:
            self.__min_date = new_min

        if new_max > self.__max_date:
            self.__max_date = new_max

    @property
    def data(self) -> pd.DataFrame:
        """
        Relevant calendar data.
        """

        date_range_df = pd.DataFrame({'Date': pd.date_range(self.__min_date, self.__max_date, freq='D')})
        date_range_df['Year'] = date_range_df['Date'].apply(lambda dt: dt.year)
        date_range_df['Quarter'] = date_range_df['Date'].apply(lambda dt: math.ceil(dt.month / 3))
        date_range_df['Month'] = date_range_df['Date'].apply(lambda dt: dt.month)
        date_range_df['Month Name'] = date_range_df['Date'].apply(lambda dt: dt.month_name()[:3])
        date_range_df['Day'] = date_range_df['Date'].apply(lambda dt: dt.day)
        date_range_df['Day Name'] = date_range_df['Date'].apply(lambda dt: dt.day_name()[:3])
        date_range_df['Date'] = date_range_df['Date'].dt.strftime('%d-%m-%Y')

        return date_range_df