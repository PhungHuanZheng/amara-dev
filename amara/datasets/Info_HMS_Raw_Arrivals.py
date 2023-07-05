"""
This module provides functionality for the creation and customization of the Info HMS 
Raw Arrivals dataset, which holds data for arrivals and departures of guests.
"""


from __future__ import annotations

import calendar
from datetime import timedelta, datetime
from joblib.parallel import Parallel, delayed
from itertools import chain

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


def generate_pickup_report(data: pd.DataFrame, trend_range: int) -> pd.DataFrame:
    """
    Dynamic pickup calculation for Info HMS Raw Arrivals after preprocessing. Column
    `['Split Nights']` must exist in the `data` passed. Call `amara.datasets.Info_HMS_Raw_Arrivals.
    mend_arrival_departure_dates` on the data before passing it here.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Info HMS Raw Arrivals dataset after `mend_arrival_departure_dates` is called on it and column
        `['Split Nights']` is set.
    `trend_range` : `int`, `default=60`
        Days before query date to use to calculate pickup for. Resulting `Dataframe` will contain
        about `unique_arrival_dates * trend_range` rows.

    Returns
    -------
    `pd.DataFrame`
        DataFrame object with `Pickup Trend` and `RNs Picked Up` columns.

    See Also
    --------
    `amara.datasets.Info_HMS_Raw_Arrivals.mend_arrival_departure_dates` : Preprocessing function to call
    before data can be passed here.
    """

    # validate n_day_pickups param
    if not isinstance(trend_range, int):
        raise ValueError(f'Expecting positive integer for parameter "trend_range", got "{trend_range}" instead.')
    if trend_range <= 0:
        raise ValueError(f'Expecting positive integer for parameter "trend_range", got "{trend_range}" instead.')
    
    # build dataframe skeleton
    pickup_df = {'Arrival Date': [], 'Days Before': [], 'Query Date': [], 'Pickup Trend': [], 'Room Nights PU': [], 'Bookings': []}
    pickup_df = {'Arrival Date': [], 'Days Before': [], 'Query Date': [], 'Cumulative Bookings': [], 'Pickup': []}

    # get arrival dates in data
    arrival_dates = np.unique(data['Arrival Date'].sort_values())
    trend_range = trend_range + 1

    # build super scuffed df but it works
    bookings_and_pickup = Parallel(n_jobs=-1, verbose=0)(delayed(_arrival_date_bookings)(data.loc[data['Arrival Date'] == arrival_date], arrival_date, trend_range) for arrival_date in arrival_dates)
    bookings_and_pickup = np.array(bookings_and_pickup).T

    cumulative_bookings = bookings_and_pickup[:, 0, :].T
    pickup = bookings_and_pickup[:, 1, :].T

    pickup_df['Arrival Date'] += list(chain.from_iterable([[arrival_date] * trend_range for arrival_date in arrival_dates]))
    pickup_df['Days Before'] += list(range(trend_range)) * len(arrival_dates)
    pickup_df['Query Date'] += list(chain.from_iterable([[arrival_date - timedelta(days=days) for days in range(trend_range)] for arrival_date in arrival_dates]))
    pickup_df['Cumulative Bookings'] += list(chain.from_iterable(cumulative_bookings))
    pickup_df['Pickup'] += list(chain.from_iterable(pickup))

    for k, v in pickup_df.items():
        print(k, len(v))

    # stringify dates
    pickup_df = pd.DataFrame(pickup_df)
    pickup_df['Arrival Date'] = pd.to_datetime(pickup_df['Arrival Date'], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')
    pickup_df['Query Date'] = pd.to_datetime(pickup_df['Query Date'], format='%Y-%m-%d').dt.strftime('%d-%m-%Y')

    return pickup_df

def _arrival_date_bookings(data: pd.DataFrame, arrival_date: datetime, trend_range: int) -> tuple[list[int], list[int]]:
    # get cumsum of bookings made in 0 - "trend_range" days
    trend_dates = [arrival_date - timedelta(days=days) for days in range(trend_range)]
    query_subdfs = [data.loc[data['Created On'] == booking_date] for booking_date in trend_dates]
    bookings_made = [len(subdf) for subdf in query_subdfs]
    
    # cumulative bookings for booking trend
    cumulative_bookings = np.cumsum(bookings_made[::-1]).tolist()[::-1]

    # room nights pickup
    cumulative_RNs = np.cumsum([subdf['Split Nights'].sum() for subdf in query_subdfs][::-1]).tolist()[::-1]
    RNs_pickup = [cumulative_RNs[0] - RNs for RNs in cumulative_RNs]

    return (cumulative_bookings, RNs_pickup)
        