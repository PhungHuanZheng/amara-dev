from __future__ import annotations

from typing import Literal
from datetime import datetime

import pandas as pd

from amara.machinelearning.timeseries.preprocessing import create_datetime_index


class TimeSeriesDataset:
    def __init__(self, datasets: tuple[pd.DataFrame], datetime_cols: tuple[str]) -> None:
        # basic init
        self.__datasets = datasets
        self.__datetime_cols = datetime_cols

        # make datetime columns for datasets passed
        for i, data in enumerate(self.__datasets):
            self.__datasets[i] = create_datetime_index(data, self.__datetime_cols[i], drop=True)

        # unify date range for all datasets
        self.__date_range = [datetime.min, datetime.max]
        for data in self.__datasets:
            # get date range
            min_date = data.index[0]
            max_date = data.index[-1]

            # compare against current, change if necessary
            if min_date > self.__date_range[0]:
                self.__date_range[0] = min_date

            if max_date > self.__date_range[1]:
                self.__date_range[1] = max_date

        print(self.__date_range)

        