from __future__ import annotations

import os
from importlib import reload
import sys; sys.path.append(os.path.abspath('../../amara-dev'))
import amara; reload(amara)

import pandas as pd
import numpy as np

from amara.core.extraction import PnL_extract_raw_data, STR_extract_raw_data, Agilysis_extract_raw_data
from amara.core.extraction import FnB_Budget_extract_raw_data, Forecast_Summary_extract_raw_data, HMS_Flash_Report_extract_raw_data
from amara.core.extraction import OccupancyStatistic_extract_raw_data, Forecast_MarketSegment_extract_raw_data