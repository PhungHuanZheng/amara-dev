from __future__ import annotations

from typing import Literal, Callable, TypeVar
from datetime import datetime
from dataclasses import dataclass

import pandas as pd

from amara.machinelearning.timeseries.preprocessing import create_datetime_index

T = TypeVar('T', pd.Series, pd.DataFrame)

@dataclass
class _DateRange:
    start_date: datetime
    end_date: datetime

class TimeSeriesDataset:
    def __init__(self, datasets: list[pd.DataFrame], datetime_cols: list[str], removed_years: tuple[int] = None) -> None:
        # basic init
        self.__initial_datasets = [df.copy(deep=True) for df in datasets]
        self.__datasets = [df.copy(deep=True) for df in datasets]
        self.__datetime_cols = datetime_cols
        self.__valid_years: list[list[int]] = []

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
    def date_range(self) -> _DateRange:
        return self.__date_range
    
    def last_valid_year(self, dataset_id: int, columns: list[str] = None) -> pd.DataFrame | pd.Series:
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
    
    def apply(self, __callback: Callable[..., T], input_ids: list[int], use_initial: bool = False) -> T:
        """
        Applies a function over any number of available datasets. Can choose 
        whether to use datasets before or after date range unification. Output 
        is the same as the `__callback` passed.

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

        Returns
        -------
        `T`
            Generic type, same type returned by `__callback` passed.
        """

        datasets = [self.__datasets[id_] for id_ in input_ids]
        return __callback(*datasets)

        
    def consolidate(self) -> pd.DataFrame:
        pass


    

        