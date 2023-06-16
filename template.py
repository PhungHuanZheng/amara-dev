"""
This file serves as a template and example on the usage of the various data
extraction and processing functions and classes provided by the module. This
template contains a full showcase of such functionality, and also acts as a 
convenient source to copy paste (Ctrl C Ctrl V) from.
"""


# Imports
from __future__ import annotations

import os
import time
import multiprocessing
from joblib.parallel import Parallel, delayed

"""
Add amara directory to system path and import. Optionally if you're using the 
Visual Studio Code editor, consider adding this:
{
    "python.analysis.extraPaths": [
        "./amara-dev"
    ]
}
to your .vscode/settings.json folder in your working directory to enable syntax
highlighting 
"""
from importlib import reload
import sys; sys.path.append('../amara-dev')
import amara; reload(amara)

import pandas as pd
import numpy as np

# import data extraction functions
from amara.core.extraction import *

# import specific dataset processing functions
from amara.datasets.Events import WeightedOccupancyCalendar, single_date_event_map
from amara.datasets.Info_HMS_Raw_Arrivals import mend_arrival_departure_dates
from amara.datasets.Master_Calendar import MasterCalendar

# import helper wrappers and functions
from amara.core.wrappers import DirectoryWrapper, DataFrameWrapper, ExcelFileWrapper
from amara.core.parallel import processor_loop

# fun visuals
from amara.visuals.progress import SingleProgressBar

# dummy variable to simulate filepaths
_agilysis_path = '../Raw Data/Agilysis' 
_events_path = '../Raw Data/Events'
_fnb_budget_path = '../Raw Data/FnB Budget'
_hms_flash_report_path = '../Raw Data/HMS Flash Report'
_info_hms_raw_arrivals_path = '../Raw Data/Info_HMS_Raw_Arrivals'
_market_segment_occupancy_budget_path = '../Raw Data/Market Segment Occupancy Budget'
_occupancystatistic_path = '../Raw Data/Occupancy Statistic'
_pnl_actual_path = '../Raw Data/PnL Actual'
_pnl_budget_path = '../Raw Data/PnL Budget'
_pnl_forecast_path = '../Raw Data/PnL Forecast'
_str_path = '../Raw Data/STR'
_str_daily_path = '../Raw Data/STR_Daily'

# progress tracker visual
tracker = SingleProgressBar(steps='auto', bar_length=100)


def main():
    """
    Main script file, denoted by the function name "main" and called at the end of 
    the file with:
    >>> if __name__ == '__main__':
    ...     main()

    Contains all data processing and similar logic.
    """

    """Agilysis Files [Raw Data/Agilysis/*.xlsx]"""
    # extract and process file data
    agilysis_filepaths = DirectoryWrapper(_agilysis_path).files
    dfs: list[tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = Parallel(n_jobs=-1, verbose=0)(delayed(processor_loop)(filepath, None, Agilysis_extract_raw_data) for filepath in agilysis_filepaths)

    # Agilysis_extract_raw_data returns a tuple of 3 dfs, separate
    dfs = np.array(dfs, dtype=object)
    revenue_df = pd.concat(dfs[:, 0]).reset_index(drop=True).fillna(0)
    settlement_df = pd.concat(dfs[:, 1]).reset_index(drop=True).fillna(0)
    department_df = pd.concat(dfs[:, 2]).reset_index(drop=True).fillna(0)

    ExcelFileWrapper('aaa.xlsx').save_multiple(datas=[revenue_df, settlement_df, department_df], sheet_names=['Revenue', 'Settlement', 'Department'])
    tracker.update()


    

if __name__ == '__main__':
    start = time.perf_counter()
    main()
    print(f'Run Time: {time.perf_counter() - start:.2f}s')