"""
This module provides functionality for model selection through exhaustive means or 
otherwise. Supports time series forecasting models provided by `statsmodels`. The name
`model_wrapper` may be misleading as this module provides wrappers for the object `type` 
and not its `instances`.
"""


from __future__ import annotations

import pandas as pd

from statsmodels.tsa.arima.model import ARIMA


class ARIMAWrapper:
    """
    Wrapper for the `ARIMA` class and its functionality provided by `statsmodels.tsa.arima.model`.

    
    """

    def __init__(self) -> None:
        pass