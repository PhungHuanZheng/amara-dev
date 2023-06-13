"""
This module provides functionality for the creation and customization of the Events
dataset, which handles event weights for Machine Learning and time series forecasting.
"""


from __future__ import annotations

import os
import math
from datetime import datetime, timedelta

import pandas as pd
import numpy as np


class WeightedOccupancyCalendar:
    """
    Handles creation and customization of an Events dataset with the date, event name and 
    event weight information to be used in time series forecasting

    Methods
    -------
    :func:`normal`
        Returns a range of 200 values with normal distribution, used to generate weights for days 
        on and around the event.
    :func:`add_weighted_event`
        Adds an event to the dataset with the `event_name`, `start_date`, `end_date` and 
        `weight_dist` passed to determine its weight distribution and nature.
    :func:`to_dataframe`
        Returns its events and their weights as a Pandas DataFrame object with all dates in between.
    :func:`to_csv`
        Saves the event data as a `.csv` file at the `filepath`.
    :func:`to_hdf`
        Saves the event data as a `.h5` file at the `filepath`.

    See Also
    --------
    :func:`pd.date_range` : Pandas function to generate a range of dates between 2 dates passed.
    """
    
    def __init__(self, start_year: int, end_year: int, weight_min: int | float = 0, weight_max: int | float = 100, dampening: int = 3) -> None:
        """
        Instantiates an instance of `amara.datasets.Events.WeightedOccupancyCalendar`.

        Parameters
        ----------
        `start_year` : `int`
            Start year of the calendar, starts at `01-01-[start_year]`.
        `end_year` : `int`
            End year of the calendar, ends at `31-12-[end_year]`.
        `weight_min` : `int | float`, `default=0`
            Minimum weight of events added.
        `weight_max` : `int | float`, `default=100`
            Maximum weight of events added.
        `dampening` : `int`, `default=3`
            Cells around main event date(s) to also apply weighting to.

        Examples
        --------
        >>> calendar = WeightedOccupancyCalendar(2020, 2022, 0, 100, 3)
        """

        self.__years = list(range(start_year, end_year + 1))
        self.__dates = pd.date_range(datetime(start_year, 1, 1), datetime(end_year, 12, 31)).to_pydatetime()

        self.__weight_min = weight_min
        self.__weight_max = weight_max

        self.__dampening = dampening

        self.__date_weights = pd.DataFrame({
            'Date': self.__dates,
            'Events/Holidays': [np.nan for _ in range(len(self.__dates))],
            'Weight': [0] * len(self.__dates)
        })
        self.__date_weights['Date'] = self.__dates

        self.__repeating_cache: dict[str, np.ndarray[datetime, datetime]] = {}
        self.__repeating_multiplier = 0.5

        # function to return normal distribution of values
        self.__normal_bounds = 10
        self.__normal_distfn = lambda x: ((1 / (1 * math.sqrt(2 * math.pi))) * math.e) ** ((-1 / 2) * ((x - 0) / 1) ** 2) 

    def normal(self, offset_ratio: float = 0, multiplier: float = 1) -> list[float]:
        """
        Returns a range of 200 values with normal distribution, used to generate weights for days 
        on and around the event.

        Parameters
        ----------
        `offset_ratio` : `float`, `default=0`
            How far left or right to offset the normal distribution by.
        `multiplier` : `float`, `default=1`
            Multipler applied to all values in the normal distribution.

        Returns
        -------
        `list[float]`
            List of values with normal distribution

        Examples
        --------
        >>> dist = WeightedOccupancyCalendar().normal(0.5, 2)
        """

        # create normal distribution with passed offset ratio and multiplier
        value_bounds = np.arange(
            (-self.__normal_bounds) - (offset_ratio * self.__normal_bounds), 
            (self.__normal_bounds + 0.1) - (offset_ratio * self.__normal_bounds), 
            0.1
        )

        # create and return distribution
        return [((multiplier * self.__normal_distfn(i)) / 1) * (self.__weight_max + self.__weight_min) for i in value_bounds]
    
    def __add_weights(self, event_name: str, start_date: datetime, end_date: datetime, weight_dist: list[int | float]) -> None:
        # make actual dates affected one before and one after the given date range for "smoothing"
        start_date -= timedelta(days=self.__dampening)
        end_date += timedelta(days=self.__dampening)
        date_range = pd.date_range(start_date, end_date)
        dist_position_count = len(date_range) + 1

        # iterate over dates in dataframe
        for i, date in enumerate(pd.date_range(start_date, end_date), start=1):
            # get weight from position of date in distribution
            weight = weight_dist[int(round(i / dist_position_count * len(weight_dist)))]

            try:
                # get index of date position within calendar df
                index = self.__date_weights.loc[self.__date_weights['Date'] == date].index[0]
            
            # if no valid date for this (no next year)
            except IndexError:
                continue

            # set value of that index to weight found and name passed
            self.__date_weights.at[index, 'Weight'] += weight

            if pd.isna(self.__date_weights.at[index, 'Events/Holidays']):
                self.__date_weights.at[index, 'Events/Holidays'] = event_name

            else:
                self.__date_weights.at[index, 'Events/Holidays'] += f' | {event_name}'

    def add_weighted_event(self, event_name: str, start_date: datetime, end_date: datetime, weight_dist: list[int | float], repeating: bool) -> None:
        """
        Adds an event to the dataset with the `event_name`, `start_date`, `end_date` and 
        `weight_dist` passed to determine its weight distribution and nature.

        Parameters
        ----------
        `event_name` : `str`
            Name of event, used as an identifier and shadow weighting.
        `start_date` : `datetime`
            Starting date of the event to be added.
        `end_date`` : `datetime`
            Ending date of the event to be added.
        `weight_dist` : `list[int | float]`
            Distribution of values for the weights corresponding to the event date(s), can
            be derived from `WeightedOccupancyCalendar.normal`.
        `repeating` : bool
            Whether the event added should be cached and used to create shadow weights the
            next year this event appears in.

        Notes
        -----
        Shadow weights are added the next year the same event, identified by `event_name` appears in, but
        with a lower weight of `weight * multiplier (0.5)`
        """
        
        # if repeated event, add to cache of next few years
        if repeating:
            # check if cache contains a previously cached event
            if event_name in self.__repeating_cache:
                # add weights for this year same dates
                cached_dates = self.__repeating_cache[event_name]
                shadow_start_date = cached_dates[0].replace(year=cached_dates[0].year + 1)
                shadow_end_date = cached_dates[-1].replace(year=cached_dates[-1].year + 1)
                self.__add_weights(f'Shadow {event_name}', shadow_start_date , shadow_end_date, np.array(weight_dist) * self.__repeating_multiplier)

            # add/override this event to cache if not already there
            self.__repeating_cache[event_name] = pd.date_range(start_date, end_date).to_pydatetime()

        # add weights to dates in range passed
        self.__add_weights(event_name, start_date, end_date, weight_dist)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Returns its events and their weights as a Pandas DataFrame object with all dates in between.

        Returns
        -------
        `pd.DataFrame`
            Event weights data.
        """
        
        return self.__date_weights

    def to_csv(self, filepath: os.PathLike | str) -> None:
        """
        Saves the event data as a `.csv` file at the `filepath`.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath at which to save the data to.
        """

        self.__date_weights.to_csv(filepath, index=False)

    def to_hdf(self, filepath: os.PathLike | str) -> None:
        """
        Saves the event data as a `.h5` file at the `filepath`.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath at which to save the data to.
        """

        self.__date_weights.to_hdf(filepath, key='xdd', index=False)


def single_date_event_map(single_date: datetime) -> int:
    """
    Helper function mapper to determine the multiplier for the normal distribution. See
    'Notes' column in the Events initiation file.

    Parameters
    ----------
    `single_date` : `datetime`
        Datetime object to check against conditions

    Returns
    -------
    `int`
        The weight multiplier for the day.
    """

    # get day name from datetime 
    day_name = single_date.strftime('%a')

    # map conditions
    if day_name in ['Fri', 'Sat', 'Sun']:
        return 1
    
    if day_name in ['Mon', 'Thu']:
        return -1
    
    return 0