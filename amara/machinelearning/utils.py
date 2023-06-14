"""
This module adds functionality for user-friendly storage of machine learning
predictions and forecasts in an easy to access data structure
"""


from __future__ import annotations

import os
import pickle
from typing import Any
from datetime import datetime
from dataclasses import dataclass


class PredictionStorage:
    """
    This class provides storage for machine learning predictions, forecasts and
    results stored in a single pickle file. Allows for reading and writing sets 
    of inter-related data with a customizable storage format using the 
    `amara.machinelearning.utils._Cache` storage object.

    Attributes
    ----------
    `latest` : `_Cache`
        Latest cached data storage object.
    `history` : `list[_Cache]`
        All cached data storage objects.


    Methods
    -------
    :func:`add_cache`
        Builds and adds a `_Cache` object to the current storage. If no previous `_Cache` 
        objects were found, initiate the signature to base all other `_Cache` objects off.
        If at least one other `_Cache` object was found, compares its attributes and data 
        types to the current signature.
    :func:`display`
        Displays the current storage structure in a more human-readable form.
    :func:`to_pickle`
        Pickles the whole `PredictionStorage` and saves it at the `filepath` specified.
    :func:`from_pickle` 
        Loads a `PredictionStorage` stored at the filepath specified.
    """
    
    def __init__(self, max_caches: int = 5) -> None:
        """
        Instantiates an instance of `PredictionStorage`.

        Parameters
        ----------
        `max_caches` : `int`
            Maximum number of `_Cache` objects that can be stored before they are to be 
            pruned.
        """

        self.__storage: list[_Cache] = []
        self.__max_caches = max_caches

        self.__cache_signature: dict[str, type] = None

    @property
    def latest(self) -> _Cache:
        """
        Latest cached data storage object.
        """

        return self.__storage[-1]
    
    @property
    def history(self) -> list[_Cache]:
        """
        All cached data storage objects.
        """

        return self.__storage
    
    def add_cache(self, **kwargs: dict[str, Any]) -> None:
        r"""
        Builds and adds a `_Cache` object to the current storage. If no previous `_Cache` 
        objects were found, initiate the signature to base all other `_Cache` objects off.
        If at least one other `_Cache` object was found, compares its attributes and data 
        types to the current signature.

        Parameters
        ----------
        `kwargs` : `dict[str, Any]`
            Set of keyword arguments to build the `_Cache` object

        Examples
        --------
        Cache signature functionality
        >>> storage = PredictionStorage()
        >>> storage.add_cache(name='Jimmy', age=10)
        >>> storage.add_cache(name='Timmy', age=70)
        >>> storage.add_cache(name='Himmy', colour='blue')
        ValueError: Keyword argument "colour" does not exist in this PredictionStorage object's cache signature. Valid keyword arguments are: ['name', 'age'].
        
        Max caches functionality
        >>> storage = PredictionStorage(max_caches=2)
        >>> for i in range(5):
        ...     storage.add_cache(number=i)
        >>> print(len(storage.history))
        2
        """

        # if no signature set, get signature from first cache
        if self.__cache_signature is None:
            # if kwargs is empty
            if len(kwargs) == 0:
                raise ValueError(f'kwargs to generate _Cache object cannot be empty.')
            
            # if valid kwargs, create _Cache object from it, update signature
            self.__storage.append(_Cache(**kwargs))
            self.__cache_signature = {key: type(value) for key, value in kwargs.items()}
        
        # if signature set, check kwargs against signature
        else:
            for key, value in kwargs.items():
                # check attribute name
                if key not in list(self.__cache_signature.keys()):
                    raise ValueError(f'Keyword argument "{key}" does not exist in this PredictionStorage object\'s cache signature. Valid keyword arguments are: {list(self.__cache_signature.keys())}.')

                # check value type
                if not isinstance(value, self.__cache_signature[key]):
                    raise ValueError(f'Expected a value of type "{self.__cache_signature[key].__name__}" for keyword argument "{key}", got value of type "{type(value).__name__}" instead.')

            # if signatures match, add
            self.__storage.append(_Cache(**kwargs))

        # prune caches if exceed max caches
        if len(self.__storage) > self.__max_caches:
            self.__storage.pop(0)

    def display(self) -> None:
        """
        Displays the current storage structure in a more human-readable form.
        """
        
        # if no cache set yet, silent return
        if self.__cache_signature is None:
            return 
        
        # get spacing formatting
        key_spacing = len(max(list(self.__cache_signature.keys()), key=len))

        # iterate over caches
        for i, cache in enumerate(self.__storage):
            print(f'[{cache.creation_date}] Cache {i + 1}')

            for key, value in cache.__dict__.items():
                # ignore private/protected
                if key.startswith('_'): 
                    continue

                print(f'\t{key: <{key_spacing}} | {value}')
            print()
    
    def to_pickle(self, filepath: os.PathLike) -> None:
        """
        This function is a wrapper for the built-in module pickle's `pickle.dump` 
        function. Pickles the whole `PredictionStorage` and saves it at the `filepath` 
        specified.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath to save `PredictionStorage` object to.

        Examples
        --------
        >>> storage = PredictionStorage(max_caches=2)
        >>> storage.to_pickle('path/to/pickle.pkl')

        See Also
        --------
        `pickle.dump` : Saves a python object to a file.
        """

        with open(filepath, 'wb') as file:
            pickle.dump(self, file)

    @classmethod
    def from_pickle(cls, filepath: os.PathLike) -> PredictionStorage:
        """
        This function is a wrapper for the built-in module pickle's `pickle.load` 
        function. Loads the `PredictionStorage` saved at the filepath and returns
        it.

        Parameters
        ----------
        `filepath` : `os.PathLike`
            Filepath where the `PredictionStorage` object is located.

        Returns
        -------
        `PredictionStorage`
            `PredictionStorage` object saved at the pickle `filepath` passed.

        Examples
        --------
        >>> storage = PredictionStorage.from_pickle('path/to/pickle.pkl')

        See Also
        --------
        `pickle.dump` : Saves a python object to a file.
        """

        with open(filepath, 'rb') as file:
            return pickle.load(file)


class _Cache:
    """
    Helper class for `PredictionStorage`, acts as main storage object within
    `PredictionStorage.history`
    """

    def __init__(self, **kwargs) -> None:
        self.__created_on = datetime.today().strftime('%d-%m-%Y %H:%M:%S')

        # set value of cache by keyword args passed
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def creation_date(self) -> str:
        return self.__created_on