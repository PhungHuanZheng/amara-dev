from __future__ import annotations

from typing import Literal
import time

import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator

from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.svm import SVC, SVR
from sklearn.naive_bayes import GaussianNB
from sklearn.linear_model import LogisticRegression, SGDClassifier, SGDRegressor, ElasticNet, BayesianRidge

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error, r2_score


class SupervisedModelSelector:
    __classification_models = [KNeighborsClassifier(), DecisionTreeClassifier(), RandomForestClassifier(), GradientBoostingClassifier(), LogisticRegression(), GaussianNB(), SGDClassifier(), SVC()]
    __regression_models = [KNeighborsRegressor(), DecisionTreeRegressor(), GradientBoostingRegressor(), RandomForestRegressor(), SVR(), LogisticRegression(), SGDRegressor(), ElasticNet(), BayesianRidge()]

    __classification_metrics = [accuracy_score, precision_score, recall_score, f1_score]
    __regression_metrics = [mean_absolute_error, mean_squared_error, mean_absolute_percentage_error, r2_score]

    def __init__(self, *, goal: Literal['classification', 'regression'], preprocessor: Pipeline | ColumnTransformer = None) -> None:
        # load valid models
        if goal == 'classification':
            self.__models = self.__classification_models
            self.__metrics = self.__classification_metrics
        elif goal == 'regression':
            self.__models = self.__regression_models
            self.__metrics = self.__regression_metrics
        else:
            raise ValueError(f'Keyword argument "goal" must be either "classification" or "regression", got "{goal}" instead.')
        
        # load preprocessor
        self.__preprocessor = preprocessor

        # storage for selection
        self.__best_base_model = None
        self.__best_base_model_scores = None
        
    def get_model_results(self, X_train, X_test, y_train, y_test, *, models: list[BaseEstimator] = None) -> pd.DataFrame:
        """
        
        """

        # get models
        if models is None:
            models = self.__models

        # results as dataframe
        results_df: dict[str, str | float] = {}

        # iterate over models
        for i, model in enumerate(models):
            # build pipeline with model
            pipeline = Pipeline([
                ('preprocessor', self.__preprocessor),
                ('estimator', model)
            ])

            # skip model if unable to train/fit/get metrics
            try:
                # record time taken to fit
                start = time.perf_counter()

                # fit to data
                pipeline = pipeline.fit(X_train, y_train)
                y_pred = pipeline.predict(X_test)
                time_taken = time.perf_counter() - start

                # get metrics
                metrics = [str(model)] + [metric(y_test, y_pred, average='macro') for metric in self.__metrics] + [f'{time_taken:.2f}s']
                results_df[i] = metrics

            except Exception as e:
                raise e
            

        return results_df

            