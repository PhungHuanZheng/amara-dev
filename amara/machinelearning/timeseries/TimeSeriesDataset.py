from __future__ import annotations

from typing import Literal, Callable, TypeVar
from datetime import datetime
from dataclasses import dataclass

import pandas as pd

from amara.machinelearning.timeseries.preprocessing import create_datetime_index
from amara._errors import NotInitiated

T = TypeVar('T', pd.Series, pd.DataFrame)

@dataclass
class _DateRange:
    start_date: datetime
    end_date: datetime

class TimeSeriesDataset:
    """
    Handles creation of a time series dataset suitable for time series forecasting 
    with `statsmodels` python package, including unification of date ranges and
    creation of datetime index.

    Attributes
    ----------
    `date_range` : `_DateRange`
        Unified date range of all DataFrames passed to `__init__`. Has attributes 
        `start_date` and `end_date`.

    Methods
    -------
    `last_valid_year`:
        Returns the last valid year of the dataset id passed to it based on all valid
        years for that dataset.
    `apply`
        Applies a callback passed on any number of datasets specifed by their ids. Returns
        data altered/modified by the callback.
    """

    def __init__(self, datasets: list[pd.DataFrame], datetime_cols: list[str], removed_years: tuple[int] = None) -> None:
        """
        Creates an instance of `TimeSeriesDataset`.

        Parameters
        ----------
        `datasets` : `list[pd.DataFrame]`
            List of DataFrames whose data is used to create the time series dataset.
        `datetime_cols` : `list[str]`
            List of datetime columns of the datasets passed. Associated list where the first
            datetime column corresponds to the first DataFrame passed.
        `removed_years` : `tuple[int]`, `default=None`
            Tuple of years to ignore, be it because of anomalous data or undesirable distribution
            of values.
        """
        
        # basic init
        self.__initial_datasets = [df.copy(deep=True) for df in datasets]
        self.__datasets = [df.copy(deep=True) for df in datasets]
        self.__datetime_cols = datetime_cols
        self.__valid_years: list[list[int]] = []

        # consolidated data
        self.__consolidated_data = None

        # make datetime columns for datasets passed, remove years if needed
        for i, data in enumerate(self.__datasets):
            self.__datasets[i] = create_datetime_index(data, self.__datetime_cols[i], format='auto', drop=True)
            self.__initial_datasets[i] = create_datetime_index(data, self.__datetime_cols[i], format='auto', drop=True)

            if removed_years is not None:
                for year in removed_years:
                    self.__datasets[i] = self.__datasets[i].loc[self.__datasets[i].index.year != year]
                    self.__initial_datasets[i] = self.__initial_datasets[i].loc[self.__initial_datasets[i].index.year != year]

            self.__valid_years.append(self.__datasets[i].index.year.unique().tolist())

        # unify date range for all datasets
        self.__date_range = _DateRange(datetime.min, datetime.max)
        for data in self.__datasets:
            # get date range
            min_date = data.index[0]
            max_date = data.index[-1]

            # compare against current, change if necessary
            if min_date > self.__date_range.start_date:
                self.__date_range.start_date = min_date

            if max_date < self.__date_range.end_date:
                self.__date_range.end_date = max_date

        # slice datasets to only include dates in date range
        for i, data in enumerate(self.__datasets):
            self.__datasets[i] = data.loc[(data.index >= self.__date_range.start_date) & (data.index <= self.__date_range.end_date)]
            
    @property
    def data(self) -> pd.DataFrame:
        """
        Consolidated data suitable for input to a time series forecasting model. Only
        available after call to `TimeSeriesDataset.consolidate`.
        """

        # check if data has been consolidated
        if self.__consolidated_data is None:
            class_name = self.__class__.__name__
            raise NotInitiated(f'{class_name}.data has not been initialised. Call {class_name}.consolidated with appropriate arguments.')
        
        return self.__consolidated_data

    @property
    def date_range(self) -> _DateRange:
        """
        Unified date range of all DataFrames passed to `__init__`. Has attributes 
        `start_date` and `end_date`.
        """

        return self.__date_range
    
    def last_valid_year(self, dataset_id: int, columns: list[str] = None) -> pd.DataFrame | pd.Series:
        """
        Returns the last valid year of the dataset id passed to it based on all valid
        years for that dataset.

        Parameters
        ----------
        `dataset_id` : `int`
            Index of DataFrame to derive last valid year of.
        `columns` : `list[str]`, `default=None`
            List of columns to keep from the dataframe queried. `None` to keep and
            return all columns.
        """

        # pull info for dataset queried
        data = self.__initial_datasets[dataset_id].copy(deep=True)
        data_valid_years = self.__valid_years[dataset_id][::-1]

        LY_start_year = self.__date_range.start_date.year - 1
        LY_end_year = self.__date_range.end_date.year - 1

        # get date bounds
        valid_bounds_found: bool = False
        for _ in range(len(data_valid_years)):
            # go back one year for min and max unified date till valid pair
            LY_start_year -= 1
            LY_end_year -= 1

            if LY_start_year - 1 in data_valid_years and LY_end_year - 1 in data_valid_years:
                # apply minus 1
                LY_start_year -= 1
                LY_end_year -= 1

                valid_bounds_found = True
                break

        if not valid_bounds_found:
            raise Exception(f'No last valid year found for unified boundaries "{self.__date_range.start_date.year}" - "{self.__date_range.end_date.year}"')
        
        # get data for that date range
        LY_date_range = _DateRange(start_date=self.__date_range.start_date.replace(year=LY_start_year),
                                   end_date=self.__date_range.end_date.replace(year=LY_end_year))
        LY_data = data.loc[(data.index >= LY_date_range.start_date) & (data.index <= LY_date_range.end_date)]

        # return columns if passed
        if columns is not None:
            return LY_data[columns]
        return LY_data
    
    def apply(self, __callback: Callable[..., T], input_ids: list[int], use_initial: bool = False, unify: bool = True) -> T:
        """
        Applies a function over any number of available datasets. Can choose 
        whether to use datasets before or after date range unification. Output 
        is the same as the `__callback` passed. Note that DataFrames passed to 
        `__callback` have datetime indexes. To access their dates, use `dataset.index`.

        Parameters
        ----------
        `__callback` : `Callable[..., T]`
            Callback function called on dataframes chosen as inputs. Takes in any number of
            dataframes as arguments and returns a `pd.Series` or `pd.DataFrame` object after
            processing.
        `input_ids` : `list[int]`
            Specify which datasets are to be input to the `__callback` passed. Indexes passed
            as integers are in the same ordered when passed to `__init__`.
        `use_initial` : `bool`, `default=False`
            Whether to use datasets before or after date range unification.
        `unify` : `bool`, `default=True`
            Whether to unify the date range of the output of `__callback` before returning. If
            `True`, output of `__callback` must have a datetime index.

        Returns
        -------
        `T`
            Generic type, same type returned by `__callback` passed.
        """

        datasets_used = self.__initial_datasets if use_initial else self.__datasets
        datasets = [datasets_used[id_] for id_ in input_ids]

        if not unify:
            return __callback(*datasets)
        
        return_value = __callback(*datasets)
        return return_value.loc[(return_value.index >= self.__date_range.start_date) & (return_value.index <= self.__date_range.end_date)]
        
    def consolidate(self, dataset_ids: list[int], columns: list[list[str]]) -> None:
        """
        Consolidates unified datasets passed in `__init__`, indexed by `datasets_id` into
        one DataFrame. Specifiy columns by passing lists of column names, an associative
        list with `dataset_ids`.

        Parameters
        ----------
        `dataset_ids` : `list[int]`
            Indexes of datasets to use in consolidation. Follows order in which the datasets 
            were passed in `__init__`.
        `columns` : `list[list[str]]`
            2D list of column names specifying which columns to extract from which dataset. 
            Associative list where the first list in `columns` corresponds to the first dataset
            in `dataset_ids`.

        Returns
        -------
        `pd.DataFrame`
            Single consolidated DataFrame object with datetime index.

        Examples
        --------
        >>> TSDataset = TimeSeriesDataset(datasets=[df1, df2], datetime_cols=['date1', 'date2'])
        >>> TSDataset_consolidated = TSDataset.consolidate(dataset_ids=[1, 2], 
        ...                                                columns=[
        ...                                                    ['df1_col1', 'df1_col2', 'df1_col4'],
        ...                                                    ['df2_col1', 'df2_col5']
        ...                                                ])
        """

        # init return dataframe as dict
        consolidated_df: dict[str, list[float]] = {}

        # iterate over datasets
        for i, id_ in enumerate(dataset_ids):
            dataset = self.__datasets[id_]
            
            # iterate over columns
            for column in columns[i]:
                # add to consolidated dict
                consolidated_df[column] = dataset[column]

        self.__consolidated_data = pd.DataFrame(consolidated_df)

    def append(self, name: str, data: pd.Series) -> None:
        """
        Adds a new column name and data to consolidated data. Only available after 
        call to `TimeSeriesDataset.consolidate`.
        """
            
        # check if data has been consolidated
        if self.__consolidated_data is None:
            class_name = self.__class__.__name__
            raise NotInitiated(f'{class_name}.data has not been initialised. Call {class_name}.consolidated with appropriate arguments.')
        
        # add new column
        self.__consolidated_data[name] = data.values



    

        