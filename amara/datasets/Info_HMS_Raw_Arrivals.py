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


def _cumulative_bookings_and_pickup(data: pd.DataFrame, report_date: datetime, trend_range: int) -> list[int]:
    # set up list for cumsum
    booking_trend: list[int] = []
    
    # data to passed is already filtered for the report date, get days between report date and created date
    booking_windows = np.array([report_date] * len(data)) - data['Created On'].dt.to_pydatetime()
    booking_windows = [window.days for window in booking_windows]
    data['DOR Booking Window'] = booking_windows

    # group bookings by window
    for days in range(trend_range):
        booking_trend.append(len(data.loc[data['DOR Booking Window'] == days]))

    # get and mend cumsum to include bookings out of trend range
    booking_trend = np.cumsum(booking_trend[::-1])[::-1]
    booking_trend += len(data) - booking_trend[0]

    # use cumulative bookings to get pickup within trend range
    pickup = [booking_trend[0] - booking_trend[i] for i in range(trend_range)]

    return booking_trend, pickup

def generate_pickup_report(data: pd.DataFrame, *, trend_range: int) -> pd.DataFrame:
    # validate n_day_pickups param
    if not isinstance(trend_range, int):
        raise ValueError(f'Expecting positive integer for parameter "trend_range", got "{trend_range}" instead.')
    if trend_range <= 0:
        raise ValueError(f'Expecting positive integer for parameter "trend_range", got "{trend_range}" instead.')
    
    # get report dates in data
    report_dates = pd.to_datetime(np.unique(data['Arrival Date'].sort_values()), format='%Y-%m-%d')
    trend_range = trend_range + 1

    # get cumulative bookings for each report date
    bookings_and_pickup = Parallel(n_jobs=-1, verbose=0)(delayed(_cumulative_bookings_and_pickup)(
        data.loc[
            (data['Arrival Date'] <= report_date) & 
            (data['Departure Date'] > report_date) &
            (~data['Status'].isin(['Cancelled', 'No Show']))
        ].copy(deep=True), pd.to_datetime(report_date, format='%Y-%m-%d'), trend_range
    ) for report_date in report_dates)

    # separate and flatten groupings and pickup
    booking_trend = np.array(bookings_and_pickup)[:, 0, :].flatten()
    pickup = np.array(bookings_and_pickup)[:, 1, :].flatten()

    # build and return pickup df
    return pd.DataFrame({
        'Report Date': list(chain.from_iterable([[report_date] * trend_range for report_date in report_dates])),
        'Query Date': list(chain.from_iterable([[report_date - timedelta(days=days) for days in range(trend_range)] for report_date in report_dates])),
        'Days Before': list(range(trend_range)) * len(report_dates),
        'Cumulative Bookings': booking_trend,
        'Pickup': pickup,
    })
