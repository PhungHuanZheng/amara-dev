"""
This module provides functionality for the creation and customization of the dStar Reports/
STR dataset, which holds data for arrivals and departures of guests.
"""


from __future__ import annotations

from datetime import datetime

import pandas as pd


def merge_summary_compsets(summary_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merges and renames the compset columns in dStar Summary Report DataFrames. If there's 
    only 1 DataFrame in the list passed, columns will just be renamed, e.g.: 'Comp Set: Competitors'
    to 'Comp Set 1: Competitors' and this function will act as a utility function for renaming. 
    If there are 2 or more DataFrames in the list passed, all other compset columns are concatenated 
    to the first DataFrame. 

    Parameters
    ----------
    `summary_dfs` : `list[pd.DataFrame]`
        List of dStar Summary Reports after being run through `amara.core.extraction.
        dStarSummary_extract_raw_data`

    Returns
    -------
    `pd.DataFrame`
        Single `DataFrame` object with merged Comp Sets.

    Examples
    --------
    Comp Set Merge Renaming
    >>> merged = merge_summary_compsets([df1, df2])
    >>> merged.columns
    [col1, col2, ..., Comp Set 1: ..., Comp Set 1: ..., Comp Set 2: ..., Comp Set 2: ...]
    """

    # iterate over and rename compset columns
    for i, df in enumerate(summary_dfs):
        # call by value
        summary_dfs[i] = df.copy(deep=True)

        # get compset columns and rename
        compset_cols = [str(col) for col in df if 'Comp Set' in col]
        renamed_cols = [col.replace(': ', f' {i + 1}:') for col in compset_cols]
        summary_dfs[i].rename(columns=dict(zip(compset_cols, renamed_cols)), inplace=True)

    # if only 1 df in list passed, return renamed
    if len(summary_dfs) != 1:
        # else iterate over every other df
        for i, df in enumerate(summary_dfs[1:]):
            compset_cols = [str(col) for col in df if 'Comp Set' in col]
            for col in compset_cols:
                summary_dfs[0][col] = df[col]

    return summary_dfs[0]

def merge_monthly_compsets(monthly_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merges and renames the compset columns in dStar Monthly Report DataFrames. If there's 
    only 1 DataFrame in the list passed, columns will just be renamed, e.g.: 'Comp Set'
    to 'Comp Set 1' and this function will act as a utility function for renaming. 
    If there are 2 or more DataFrames in the list passed, all other compset columns are concatenated 
    to the first DataFrame. 

    Parameters
    ----------
    `monthly_dfs` : `list[pd.DataFrame]`
        List of dStar Monthly Reports after being run through `amara.core.extraction.
        dStarMonthly_extract_raw_data`

    Returns
    -------
    `pd.DataFrame`
        Single `DataFrame` object with merged Comp Sets.

    Examples
    --------
    Comp Set Merge Renaming
    >>> merged = merge_monthly_compsets([df1, df2])
    >>> merged.columns
    [col1, col2, ..., Comp Set 1: ..., Comp Set 1: ..., Comp Set 2: ..., Comp Set 2: ...]
    """

    # iterate over and rename compset columns
    for i, df in enumerate(monthly_dfs):
        # call by value
        monthly_dfs[i] = df.copy(deep=True)

        # get compset columns and rename
        compset_cols = [str(col) for col in df if 'Comp Set' in col]
        renamed_cols = [col.replace('Comp Set', f'Comp Set {i + 1}') for col in compset_cols]
        monthly_dfs[i].rename(columns=dict(zip(compset_cols, renamed_cols)), inplace=True)

    # if only 1 df in list passed, ignore merge
    if len(monthly_dfs) != 1:
        # else iterate over every other df
        for i, df in enumerate(monthly_dfs[1:]):
            compset_cols = [str(col) for col in df if 'Comp Set' in col]
            for col in compset_cols:
                monthly_dfs[0][col] = df[col]

    # stringify dates
    monthly_dfs[0]['Date'] = monthly_dfs[0]['Date'].apply(lambda date: datetime.strftime(date, '%d-%m-%Y'))

    return monthly_dfs[0]

def merge_daily_compsets(daily_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merges and renames the compset columns in dStar Daily Report DataFrames. If there's 
    only 1 DataFrame in the list passed, columns will just be renamed, e.g.: 'Comp Set'
    to 'Comp Set 1' and this function will act as a utility function for renaming. 
    If there are 2 or more DataFrames in the list passed, all other compset columns are concatenated 
    to the first DataFrame. 

    Parameters
    ----------
    `daily_dfs` : `list[pd.DataFrame]`
        List of dStar Daily Reports after being run through `amara.core.extraction.
        dStarDaily_extract_raw_data`

    Returns
    -------
    `pd.DataFrame`
        Single `DataFrame` object with merged Comp Sets.

    Examples
    --------
    Comp Set Merge Renaming
    >>> merged = merge_daily_compsets([df1, df2])
    >>> merged.columns
    [col1, col2, ..., Comp Set 1: ..., Comp Set 1: ..., Comp Set 2: ..., Comp Set 2: ...]
    """
    
    # iterate over and rename compset columns
    for i, df in enumerate(daily_dfs):
        # call by value
        daily_dfs[i] = df.copy(deep=True)

        # get compset columns and rename
        compset_cols = [str(col) for col in df if 'Comp Set' in col]
        renamed_cols = [col.replace('Comp Set', f'Comp Set {i + 1}') for col in compset_cols]
        daily_dfs[i].rename(columns=dict(zip(compset_cols, renamed_cols)), inplace=True)

    # if only 1 df in list passed, return renamed
    if len(daily_dfs) != 1:
        # else iterate over every other df
        for i, df in enumerate(daily_dfs[1:]):
            compset_cols = [str(col) for col in df if 'Comp Set' in col]
            for col in compset_cols:
                daily_dfs[0][col] = df[col]

    # stringify dates
    daily_dfs[0]['Date'] = daily_dfs[0]['Date'].apply(lambda date: datetime.strftime(date, '%d-%m-%Y'))

    return daily_dfs[0]