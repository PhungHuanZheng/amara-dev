from __future__ import annotations

from typing import Literal
from datetime import datetime
from dataclasses import dataclass

import pandas as pd

from amara.machinelearning.timeseries.preprocessing import create_datetime_index

@dataclass
class _DateRange:
    start_date: datetime
    end_date: datetime

class TimeSeriesDataset:
    def __init__(self, datasets: list[pd.DataFrame], datetime_cols: list[str]) -> None:
        # basic init
        self.__datasets = list(datasets)
        self.__datetime_cols = datetime_cols

        # make datetime columns for datasets passed
        for i, data in enumerate(self.__datasets):
            self.__datasets[i] = create_datetime_index(data, self.__datetime_cols[i], format='auto', drop=True)

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

    @property
    def date_range(self) -> _DateRange:
        return self.__date_range
    

        