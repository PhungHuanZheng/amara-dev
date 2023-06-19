"""
This module provides functionality for model selection through exhaustive means or 
otherwise. Supports time series forecasting models provided by `statsmodels`. The name
`model_wrapper` may be misleading as this module provides wrappers for the object `type` 
and not its `instances`.
"""


from __future__ import annotations

import time

import warnings; from statsmodels.tsa.base.tsa_model import ValueWarning
warnings.filterwarnings(action='ignore', category=UserWarning)
warnings.filterwarnings(action='ignore', category=ValueWarning)

import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

from amara.visuals.progress import SingleProgressBar


class ARIMAWrapper:
    """
    Wrapper for the `ARIMA` class and its functionality provided by `statsmodels.tsa.arima.model`.
    """

    def __init__(self, train: pd.DataFrame, forecast: pd.DataFrame, target: str) -> None:
        """
        Creates an instance of `ARIMAWrapper`. Wraps the `ARIMA` class and provides
        extra functionality surrounding it. `train` and `forecast` must both have a 
        datetime index and exogenous variables but `forecast` does not need the target
        variable as a column.

        Parameters
        ----------
        `train` : `pd.DataFrame`
            Training data including exogenous variables.
        `forecast` : `pd.DataFrame`
            Forecast data including exogenous variables.
        `target` : `str`
            Target of the forecasting. 
        """
        
        self.__train = train
        self.__forecast = forecast

        self.__train_target = train[target]
        self.__train_exog = train.drop(target, axis=1)

        if target in target:
            self.__forecast_target = forecast[target]
            self.__forecast_exog = forecast.drop(target, axis=1)
        else:
            self.__forecast_target = None
            self.__forecast_exog = forecast

    def exhaustive_search(self, p_values: list[int], d_values: list[int], q_values: list[int]) -> dict[tuple[int, int, int], list[float]]:
        # init progress tracker
        steps_count = len(p_values) * len(d_values) * len(q_values)
        tracker = SingleProgressBar(steps_count, bar_length=100)
        passes, failures = 0, 0

        # track time taken
        start = time.perf_counter()

        # exhaustive search over values
        for p in p_values:
            for d in d_values:
                for q in q_values:
                    
                    # in case of ARIMA fitting error
                    try:
                        # build model
                        model = ARIMA(self.__train_target, exog=self.__train_exog, order=(p, d, q), freq='D', enforce_invertibility=True, enforce_stationarity=True)
                        model_fit = model.fit(method='innovations_mle')

                        insample_pred = model_fit.predict()
                        outsample_fc = model_fit.get_forecast(len(self.__forecast), exog=self.__forecast_exog)
                        full_pred = pd.concat([insample_pred, outsample_fc.predicted_mean])

                        passes += 1

                    except Exception:
                        failures += 1
                        pass

                    tracker.update()

        # print status report
        print(F'Passes: {passes} | Failures: {failures} | Time Taken: {time.perf_counter() - start:.2f}s')