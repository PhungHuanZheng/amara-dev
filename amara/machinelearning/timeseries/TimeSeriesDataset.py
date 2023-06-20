from __future__ import annotations

import warnings
from typing import Literal, Callable, TypeVar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
from calendar import isleap

import pandas as pd
from statsmodels.tsa.statespace.tools import diff
from statsmodels.tsa.stattools import adfuller

from amara.machinelearning.timeseries.preprocessing import create_datetime_index
from amara._errors import NotInitialisedError

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
    `data_` : `pd.DataFrame`
        Consolidated data with unified date range, provided by a call to `TimeSeriesDataset.consolidate`
        with appropriate arguments

    Methods
    -------
    `last_valid_year`:
        Returns the last valid year of the dataset id passed to it based on all valid
        years for that dataset.
    `apply`:
        Applies a callback passed on any number of datasets specifed by their ids. Returns
        data altered/modified by the callback.
    `consolidate`:
        Consolidates unified datasets passed in `__init__`, indexed by `datasets_id` into
        one DataFrame. Specifiy columns by passing lists of column names, an associative
        list with `dataset_ids`. Provides access to `TimeSeriesDataset.data_`, `TimeSeriesDataset.append`
        and `TimeSeriesDataset.split`.
    `set_target`:
        Internally sets the target column for the `TimeSeriesDataset` object. Only available after 
        call to `TimeSeriesDataset.consolidate`.
    `append`:
        Appends a new data column to previously consolidated data. Data passed must be a `pd.Series`
        object with a datetime index.
    `split`
        Splits the consolidate data into train and forecast on `split_date` passed. Train data
        is inclusive of `split_date` and forecast is exclusive of `split_date`. Provides access to 
        `TimeSeriesDataset.data_`, `TimeSeriesDataset.train_data_`, `TimeSeriesDataset.forecast_data_` and
        `TimeSeriesDataset.forecast_date_`.
    `auto_diff`
        Auto differences the data based on their p-values returned by the `adfuller` test. Pass a 
        boolean mask to control whether a column is to be differenced.

    Notes
    -----
    Attributes with a trailing underscore, e.g.: "data_", are attributes that are not initialised during
    `__init__`. Read their docstrings about how to initialise and access them.
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

        self.__class_name = self.__class__.__name__
        
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

        self.__forecast_date = None
        self.__train_data = None
        self.__forecast_data = None

    @property
    def data_(self) -> pd.DataFrame:
        """
        Consolidated data suitable for input to a time series forecasting model. Only
        available after call to `TimeSeriesDataset.consolidate`.

        Raises
        ------
        `NotInitialisedError`:
            Only available after call to `TimeSeriesDataset.consolidate`.
        """

        # check if data has been consolidated
        if self.__consolidated_data is None:
            raise NotInitialisedError(f'{self.__class_name}.data_ has not been initialised. Call {self.__class_name}.consolidated with appropriate arguments.')
        
        return self.__consolidated_data.copy(deep=True)

    @property
    def date_range(self) -> _DateRange:
        """
        Unified date range of all DataFrames passed to `__init__`. Has attributes 
        `start_date` and `end_date`.
        """

        return self.__date_range
    
    @property
    def train_data_(self) -> pd.DataFrame:
        """
        Period of unified data used for model training. Only available after calls to 
        `TimeSeriesDataset.consolidate` and `TimeSeriesDataset.split`

        Raises
        ------
        `NotInitialisedError`:
            Only available after calls to `TimeSeriesDataset.consolidate` and `TimeSeriesDataset.split`
        """

        if self.__train_data is None:
            raise NotInitialisedError(f'{self.__class_name}.train_data_ has not been initialised. Call {self.__class_name}.consolidated and {self.__class_name}.split with appropriate arguments.')
        
        return self.__train_data

    @property
    def forecast_data_(self) -> pd.DataFrame:
        """
        Period of unified data used for model training. Only available after calls to 
        `TimeSeriesDataset.consolidate` and `TimeSeriesDataset.split`.

        Raises
        ------
        `NotInitialisedError`:
            Only available after calls to `TimeSeriesDataset.consolidate` and `TimeSeriesDataset.split`
        """

        if self.__forecast_data is None:
            raise NotInitialisedError(f'{self.__class_name}.forecast_data_ has not been initialised. Call {self.__class_name}.consolidated and {self.__class_name}.split with appropriate arguments.')

        return self.__forecast_data

    @property
    def forecast_date_(self) -> str:
        """
        Returns the date the data is split on as a string value in the format `%d-%m-%Y` or
        `DD-MM-YYYY`. Only available after calls to `TimeSeriesDataset.consolidate` and 
        `TimeSeriesDataset.split`.
        """

        if self.__consolidated_data is None or self.__train_data is None or self.__forecast_data is None:
            raise NotInitialisedError(f'{self.__class_name}.forecast_date_ has not been initialised. Call {self.__class_name}.consolidated and {self.__class_name}.split with appropriate arguments.')

        return self.__forecast_date.strftime('%d-%m-%Y')

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
        
    def consolidate(self, dataset_ids: list[int], columns: list[list[str]], as_names: list[list[str]] = None) -> None:
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
        `as_names` : `list[list[str]]`, `default=None`
            2D list of strings as new column names, acts as associative list with `columns` passed.

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

        # mend as_names
        if as_names is None:
            as_names = columns

        # init return dataframe as dict
        consolidated_df: dict[str, list[float]] = {}

        # iterate over datasets
        for i, id_ in enumerate(dataset_ids):
            dataset = self.__datasets[id_]
            
            # iterate over columns
            for j, column in enumerate(columns[i]):
                # add to consolidated dict
                consolidated_df[as_names[i][j]] = dataset[column]

        self.__consolidated_data = pd.DataFrame(consolidated_df)

    def set_target(self, target_col: str) -> None:
        """
        Internally sets the target column for the `TimeSeriesDataset` object. Only available after 
        call to `TimeSeriesDataset.consolidate`.

        Parameters
        ----------
        `target_col` : `str`
            A column in the consolidated dataframe `TimeSeriesDataset.data_`.
        """

        self.__target = target_col

    def append(self, name: str, data: pd.Series) -> None:
        """
        Adds a new column name and data to consolidated data. Only available after 
        call to `TimeSeriesDataset.consolidate`.

        Parameters
        ----------
        `name` : `str`
            Name of new column.
        `data` : `pd.Series`
            Pandas Series object with new data. Must be of the same length as the 
            current consolidated date range.

        Raises
        ------
        `NotInitiatedError`:
            Only available after call to `TimeSeriesDataset.consolidate`.

        See Also
        --------
        `TimeSeriesDataset.data` : Consolidated time series data
        """
            
        # check if data has been consolidated by trying to call TimeSeriesDataset.data
        self.data_

        # add new column
        self.__consolidated_data[name] = data.values

    def split(self, split_date: datetime, train_months: int, forecast_months: int) -> None:
        """
        Splits the consolidated data into train and forecast data, split on `split_date`
        passed, usually today's date. Only available after call to `TimeSeriesDataset.consolidate`.
        Train data is inclusive of `split_date`.

        Parameters
        ----------
        `split_date` : `datetime`
            "Center" date for splitting consolidated data.
        `train_months` : `int`
            How many months ago till `split_date` to use as training data
        `forecast_months` : `int`
            How many months after from today to use as forecast data

        Returns
        -------
        `pd.DataFrame`
            Train data.
        `pd.DataFrame`
            Forecast/test data.

        Raises
        ------
        `NotInitiatedError`:
            Only available after call to `TimeSeriesDataset.consolidate`.

        Warnings
        --------
        `UserWarning`:
            If full train or forecast months are not available in unified consolidated data.

        See Also
        --------
        `TimeSeriesDataset.data` : Consolidated time series data
        """

        # get date boundaries
        train_start = split_date - relativedelta(months=train_months)
        forecast_end = split_date + relativedelta(months=forecast_months)

        # split data while also checking if consolidated
        train_data = self.data_.loc[(self.data_.index >= train_start) & (self.data_.index <= split_date)]
        forecast_data = self.data_.loc[(self.data_.index > split_date) & (self.data_.index <= forecast_end)]

        # check if split data is actually at the requested date bounds
        train_days_off = (train_data.index[0] - train_start).days
        forecast_days_off = (forecast_end - forecast_data.index[-1]).days

        # send warnings if doesnt match exact date range specified
        if train_days_off != 0:
            warnings.warn(f'Available unified dates for training are less than requested by {train_days_off} days.', UserWarning)
        if forecast_days_off != 0:
            warnings.warn(f'Available unified dates for forecasting are less than requested by {forecast_days_off} days.', UserWarning)

        # initialize values
        self.__forecast_date = split_date
        self.__train_data = train_data
        self.__forecast_data = forecast_data
    
    def auto_diff(self, bool_mask: list[bool], force: bool = False, inplace: bool = False) -> pd.DataFrame | None:
        """
        Auto differences the data based on their p-values returned by the `adfuller` test. Pass a 
        boolean mask to control whether a column is to be differenced. Note that the `bool_mask` 
        follows the same order in which the consolidated data `TimeSeriesDataset.data` was created
        through `TimeSeriesDataset.consolidate` and `TimeSeriesDataset.append`.
        
        Parameters
        ----------
        `bool_mask` : `list[bool]`
            Boolean mask to control whether a column is to be differenced.
        `force` : `bool`, `default=False`
            Boolean to control if differencing should be forced by the `boolean_mask` passed.
        `inplace` : `bool`, `default=False`
            Boolean to control if new differenced data should replace the current data or 
            return a copy

        Returns
        -------
        `pd.DataFrame | None`
            `pd.DataFrame` if not `inplace` else `None`
        """

        # check that bool mask passed is the same length as columns
        if len(bool_mask) != len(self.data_.columns):
            raise ValueError(f'Boolean mask ({len(bool_mask)}) and consolidated data\'s columns ({len(self.data_.columns)}) are of different lengths.')

        target_df = self.__consolidated_data.copy(deep=True)

        # iterate over columns
        for i, column in enumerate(self.data_):
            p_value = adfuller(self.data_[column])[1]

            # if more than 0.05 and want to diff
            if (force or p_value > 0.05) and bool_mask[i] is True:
                column_data = self.data_[column]

                while adfuller(column_data)[1] > 0.05:
                    column_data = diff(column_data)
                    column_data = [None] + column_data.tolist()
                    column_data = pd.Series(column_data, index=self.data_.index).interpolate('time')
                    column_data.fillna(column_data.mean(), inplace=True)

                target_df[column] = column_data

        if not inplace:
            return target_df
        self.__consolidated_data = target_df
