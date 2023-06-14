"""
This module provides functionality for basic and intermmediate exploratory data 
analytics for datasets maintained by Amara Hotels.
"""


from __future__ import annotations

from typing import Any

import pandas as pd


def analysis(data: pd.DataFrame, filler: Any = '') -> pd.DataFrame:
    """
    Provides an intermmediate statistical analysis of the DataFrame object
    passed, returning results as a DataFrame object.

    Parameters
    ----------
    `data` : `pd.DataFrame`
        Raw dataset to be analysed
    `filler` : `Any`, `default=''`
        Value to use if analysis for that column's data is not valid, e.g.: mean
        value for a string column.
    
    Returns
    -------
    `pd.DataFrame`
        Results of analysis of `data` passed
    """

    # store results in a dict to be converted later
    results: dict[str, list[str | float]] = {'column name': [], 'data type': [], 
                                             'unique count': [], 'null count': [], 
                                             'null %': [], 'mode': [], 'mode count': [], 
                                             'mode %': [], 'mean': [], 'median': [], 
                                             'minimum': [], 'maximum': []}

    # iterate over columns of dataframe
    for col_name in data.columns:

        # get column name and data type
        results['column name'].append(col_name)
        results['data type'].append(data[col_name].dtype.name)

        # get unique count
        counts = len(data[col_name].unique())
        results['unique count'].append(counts)

        # get null count and percentage
        null_count = len(data.loc[data[col_name].isna()])
        null_percentage = null_count / len(data[col_name])

        results['null count'].append(null_count)
        results['null %'].append(f'{null_percentage * 100:.3f}%')

        # get mode count and percentage
        results['mode'].append(f'{data[col_name].mode()[0]}')
        results['mode count'].append(len(data.loc[data[col_name] == data[col_name].mode()[0]]))
        results['mode %'].append(f'{results["mode count"][-1] / len(data) * 100:.3f}')

        # if column dtype is numerical
        if any([num_type in data[col_name].dtype.name for num_type in ['int', 'float']]):
            # get mean, median and mode
            results['mean'].append(f'{data[col_name].mean():.3f}')
            results['median'].append(f'{data[col_name].median():.3f}')
            
            # get min and max
            results['minimum'].append(f'{data[col_name].min():.3f}')
            results['maximum'].append(f'{data[col_name].max():.3f}')

        # else if categorical/string dtype
        else:
            # use filler characters
            for result in ['mean', 'median', 'minimum', 'maximum']:
                results[result].append(filler)

    # return as pandas DataFrame   
    return pd.DataFrame(results)