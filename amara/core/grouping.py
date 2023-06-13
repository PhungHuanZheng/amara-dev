"""
This module provides functionality for grouping values in array-like objects,
both numerical and categorical.
"""


from __future__ import annotations

import pandas as pd


def group_categories(column: pd.Series | list, map_dict: dict[str, list[str]], filler = None) -> list:
    """
    Groups a column of categorical/non-numerical values into groups specified
    by the `map_dict` parameter passed. Returns a python list of the same length.

    Parameters
    ----------
    `column` : `pd.Series | list`
        Array-like iterable with non-numerical values.
    `map_dict` : `dict[str, list[str]]`
        Dictionary of groups to group the column values into.
    `filler` : `Any`, `default=None`
        Value to use if the array value does not belong to any of the categories.

    Returns
    -------
    `list`
        List of new grouped values.

    Examples
    --------
    >>> groups = {
    ...     'A' : ['A1', 'A2', 'A3'],
    ...     'B' : ['B1', 'B2', 'B3'],
    ...     'C' : ['C1', 'C2', 'C3'],
    ... }
    ... values = ['A1', 'A2', 'A3', 'C1', 'C2', 'C3', 'B1', 'B2', 'B3', 'D3', 'A10']
    ... 
    ... grouped_values = group_categories(values, groups)
    ... >>> ['A', 'A', 'A', 'C', 'C', 'C', 'B', 'B', 'B', None, None]

    See Also
    --------
    :func:`amara.core.grouping.group_categories` : Grouping for numerical values.
    """

    # grouped values
    grouped_values = []

    for value in column:
        for key, matches in map_dict.items():
            if value in matches:
                grouped_values.append(key)
                break

        # if no match, append filler
        else:
            grouped_values.append(filler)

    return grouped_values

def group_thresholds(column: pd.Series | list, thresholds: dict[str, tuple[float, float]], filler = None) -> list:
    """
    Groups a column of numerical values into groups specified by the `thresholds` 
    parameter passed. Returns a python list of the same length.

    Parameters
    ----------
    `column` : `pd.Series | list`
        Array-like iterable with numerical values.
    `thresholds` : `dict[str, tuple[float, float]]`
        Dictionary of thresholds to group the column values into. Thresholds are INCLUSIVE -- `>=`, `<=`
    `filler` : `Any`, `default=None`
        Value to use if the array value does not belong within any of the thresholds.

    Returns
    -------
    `list`
        List of new grouped values.

    Examples
    --------
    >>> thresholds = {
    ...     'A' : (0, 15),
    ...     'B' : (16, 30),
    ...     'C' : (31, 50),
    ... }
    ... values = [2, 6, 2, 22, 75, 19, 37, 68, 19, 26, 31]
    ... 
    ... grouped_values = group_thresholds(values, thresholds)
    ... >>> ['A', 'A', 'A', 'B', None, 'B', 'C', None, 'B', 'B', 'C']

    See Also
    --------
    :func:`amara.core.grouping.group_categories` : Grouping for non-numerical values.
    """

    # grouped values
    grouped_values = []

    for value in column:
        for bin, range_ in thresholds.items():
            if value >= range_[0] and value <= range_[1]:
                grouped_values.append(bin)
                break

        # if no match, append filler
        else:
            grouped_values.append(filler)

    return grouped_values