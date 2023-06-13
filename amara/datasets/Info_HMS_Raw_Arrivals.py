"""
This module provides functionality for the creation and customization of the Info HMS 
Raw Arrivals dataset, which holds data for arrivals and departures of guests.
"""


from __future__ import annotations

import calendar
from datetime import timedelta

import pandas as pd
import numpy as np


def mend_arrival_departure_dates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Splits the `Arrival Date` and `Departure Date` columns of the dataframe passed
    into individual months.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Info HMS Raw Arrivals Dataset

    Returns
    -------
    `pd.DataFrame`
        Info HMS Raw Arrivals dataset with mended dates and added columns for splitting.

    Notes
    -----
    Original: 
    >>> '15-01-2020' -> '23-03-2020'
    Mended:
    >>> '15-01-2020' -> '31-01-2020'
    ... '01-02-2020' -> '28-02-2020'
    ... '01-03-2020' -> '23-03-2020'
    
    """

    # call df by value, create new columns
    data = data.copy(deep=True)
    data['Split Rate Grand Total'] = data['Rate Grand Total'].tolist()
    data['Split Nights'] = data['Nights'].tolist()

    # grab datapoints where Arrival Date and Departure Date year or month dont match
    diff_month_data = data.loc[(data['Arrival Date'].dt.month != data['Departure Date'].dt.month) | (data['Arrival Date'].dt.year != data['Departure Date'].dt.year)]
    diff_month_ids = diff_month_data.index

    # drop ids from data
    to_mend = data.iloc[diff_month_ids]
    data.drop(diff_month_ids, inplace=True)

    # grab date range by month
    for i, row in to_mend.iterrows():
        # get date range
        date_range = pd.date_range(row['Arrival Date'], row['Departure Date'], freq='M', inclusive='both').tolist()
        date_range: list[pd.Timestamp] = [row['Arrival Date']] + date_range + [row['Departure Date']]

        # add first day of next month for added intervals
        for i, date in enumerate(date_range):
            # ignore if last day
            if i == len(date_range) - 1:
                break

            # if last day of that month
            month_days = calendar.monthrange(date.year, date.month)[1]
            if date.day == month_days and i != 0:
                date_range.insert(i + 1, date + timedelta(days=1))

        # get date pairs
        date_pairs = np.array([date_range[::2], date_range[1::2]]).T

        # create template df from datapoint
        template_df = pd.concat([pd.DataFrame(row).T for _ in range(len(date_pairs[:,0]))]).reset_index(drop=True)
        template_df['Arrival Date'] = date_pairs[:,0]
        template_df['Departure Date'] = date_pairs[:,1]

        # split room nights
        template_df['Split Nights'] = [(pair[1] - pair[0]).days for pair in date_pairs]
        for i, row in template_df.iterrows():
            # adjust split nights for continuous months
            if i != len(template_df) - 1:
                template_df.at[i, 'Split Nights'] += 1
               
            # divide rate grand total accordingly
            template_df.at[i, 'Split Rate Grand Total'] = (template_df.at[i, 'Split Nights'] / template_df.at[i, 'Nights']) * template_df.at[i, 'Rate Grand Total']

        # concat template to end of original
        data = pd.concat([data, template_df])

    return data.reset_index(drop=True)

