"""
This module provides functionality for parallel processing using the
built-in joblib module
"""


from __future__ import annotations

import os
from typing import Any, Callable, TypeVar
from joblib.parallel import Parallel, delayed

import pandas as pd


def processor_loop(filepath: os.PathLike | str, sheet_names: list[str] = None, processor: Callable[[pd.DataFrame], pd.DataFrame] = None) -> pd.DataFrame:
    """
    Loop function to be used with the `joblib.Parallel` class for data processing
    with Pandas.

    Parameters
    ----------
    `filepath` : `os.PathLike | str`
        Filepath to file to be processed.
    `sheet_names` : `list[str]`, `default=None`
        Sheetnames in the Excel file passed to be passed to the `processor`.
    `processor` : `Callable[[pd.DataFrame], pd.DataFrame]`, `default=None`
        Processor to be used on the DataFrame extracted before returning.

    Returns
    -------
    `pd.DataFrame`
        Extracted and processed data as pandas DataFrame.

    Examples
    --------
    >>> dfs = Parallel(n_jobs=-1, verbose=0)(delayed(processor_loop)(filepath, None, data_processor) for filepath in filepaths)

    See Also
    --------
    :func:`joblib.parallel` : built-in module to parallelize processes.
    """

    if processor is None:
        processor = lambda df: df

    if sheet_names is None:
        return processor(pd.read_excel(filepath))

    dfs: dict[str, pd.DataFrame] = pd.read_excel(filepath, sheet_name=sheet_names)
    return pd.concat([processor(df) for _, df in dfs.items()])


