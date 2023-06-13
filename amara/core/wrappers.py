"""
This module provides further functionality for pre-existing data structures, built-in or
provided by other modules, e.g.: Pandas, Numpy, etc
"""


from __future__ import annotations

import os
import pickle
from typing import Callable, Any, TypeVar
from joblib import Parallel, delayed

import pandas as pd
import numpy as np

T = TypeVar('T')


class DataFrameWrapper:
    """
    Wrapper for a Pandas DataFrame object.

    Methods
    -------
    :func:`to_chunks`
        Splits a pandas DataFrame object into chunks based on the `chunk_size` passed.

    Examples
    --------
    >>> wrapper = DataFrameWrapper(df)

    See Also
    --------
    `pandas.DataFrame` : Core DataFrame object provided by Pandas.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        """
        Instantiates a DataFrameWrapper object with a Pandas DataFrame.

        Parameters
        ----------
        `data` : `pd.DataFrame`
            Pandas DataFrame object to use methods on.
        """

        self.__data = data

    def to_chunks(self, chunk_size: int = 5000):
        """
        Splits a pandas DataFrame object into chunks based on the `chunk_size` passed.

        Parameters
        ----------
        `chunk_size` : `int`
            Size of each chunk of the split dataframe object.

        Returns
        -------
        `list[pd.DataFrame]`
            List of chunks from the original dataframe based on `chunk_size`.
        """


        chunk_bounds = np.array([start_ids := list(range(self.__data.shape[0]))[::chunk_size], start_ids[1:] + [None]])
        chunks = [self.__data.iloc[pair[0]: pair[1]] for pair in chunk_bounds.T]

        return [chunk.reset_index(drop=True) for chunk in chunks]


class ExcelFileWrapper:
    """
    Wrapper for an Excel filepath
    
    Methods
    -------
    :func:`save_single`
        Saves a single Pandas DataFrame object to the filepath.
    :func:`save_multiple`
        Saves multiple Pandas DataFrame objects to the filepath under different `sheet_names`.

    Examples
    --------
    >>> wrapper = ExcelFileWrapper('path/to/file.xlsx')

    See Also
    --------
    :func:`pd.DataFrame.to_excel` : Saves a single DataFrame object to an Excel file

    :func:`pd.ExcelWriter` : Interface to sheet saving with Python and Pandas
    """

    def __init__(self, filepath: os.PathLike) -> None:
        """
        Instantiates an ExcelFileWrapper object with a system filepath.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath containing the Excel file to access.
        """
        self.__filepath = filepath

    def save_single(self, data: pd.DataFrame, sheet_name: str = 'Sheet 1') -> None:
        """
        Saves a single Pandas DataFrame object to the filepath.

        Parameters
        ----------
        `data` : `pd.DataFrame`
            Pandas DataFrame object to save to the Excel file
        `sheet_name` : `str`, `default='Sheet 1'`
            Sheet name of DataFrame object to be saved

        See Also
        --------
        :func:`pd.DataFrame.to_excel` : Saves a single DataFrame object to an Excel file
        """

        data.to_excel(self.__filepath, sheet_name=sheet_name, index=False)

    def save_multiple(self, datas: list[pd.DataFrame], sheet_names: list[str]) -> None:
        """
        Saves multiple Pandas DataFrame objects to the filepath under different `sheet_names`.

        Parameters
        ----------
        `datas` : `list[pd.DataFrame]`
            List of DataFrame objects to be saved. Their sheet names follow an associated
            list with `sheet_names` passed.
        `sheet_names` : `list[str]`
            List of sheet names to save the DataFrame objects under. Must have the same
            number of elements as `datas`.

        Examples
        --------
        >>> wrapper.save_multiple(
        ...     datas=[df1, df2, df3],
        ...     sheet_names=['data1', 'data2', 'data3']
        ... )
        
        See Also
        --------
        :func:`pd.ExcelWriter` : Interface to sheet saving with Python and Pandas
        """

        # open excel file with engine
        with pd.ExcelWriter(self.__filepath, engine='xlsxwriter') as writer:
            for i, data in enumerate(datas):
                data.to_excel(writer, sheet_name=sheet_names[i], index=False) 


class DirectoryWrapper:
    """
    Wrapper for a directory path

    Attributes
    ----------
    `files` : `list[os.PathLike]`
        List of relative filepaths from the directory `root`.
    `size` : `int`
        Count of files in the directory `root`.

    Methods
    -------
    :func:`apply`
        Applies a `__callback` on all filepaths in `files`.
    :func:`cache`
        Saves all filenames as a pickle object at the `filepath` passed.

    Examples
    --------
    >>> wrapper = DirectoryWrapper('path/to/folder')
    """

    def __init__(self, root: os.PathLike) -> None:
        """
        Instantiates a DirectoryWrapper object with a system filepath.

        Parameters
        ----------
        `root` : `os.PathLike`
            Filepath to a folder.
        """

        self.__root = root

        # get files in root passed
        self.__files = []

        for root, _, filenames in os.walk(self.__root):
            for filename in filenames:
                self.__files.append(os.path.join(root, filename))

    @property
    def files(self) -> list[os.PathLike]:
        """
        Files in directory root
        """

        return self.__files
    
    @property
    def size(self) -> int:
        """
        Count of files in directory root
        """

        return len(self.__files)
    
    def apply(self, __callback: Callable[[os.PathLike], T]) -> list[T]:
        """
        Applies a `__callback` on all filepaths in `files`.

        Parameters
        ----------
        `__callback` : `Callable[[os.PathLike], T]`
            Function that takes in a filepath

        Returns
        -------
        `list[T]`
            List of the outputs from the `__callback` passed after applying to all `files`.
        """

        return Parallel(n_jobs=-1, verbose=0)(delayed(__callback)(filename) for filename in self.files)
    
    def cache(self, filepath: os.PathLike) -> None:
        """
        Saves all filenames as a pickle object at the `filepath` passed.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath to save pickled filenames to.
        """

        filenames = [filename.split('\\')[-1] for filename in self.files]
        
        with open(filepath, 'wb') as file:
            pickle.dump(filenames, file)

