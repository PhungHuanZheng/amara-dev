from __future__ import annotations

from typing import Literal

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

from sklearn.model_selection import train_test_split


class SupervisedModelSelector:
    __classification_models = [KNeighborsClassifier, DecisionTreeClassifier, RandomForestClassifier, GradientBoostingClassifier, LogisticRegression, GaussianNB, SGDClassifier, SVC]
    __regression_models = [KNeighborsRegressor, DecisionTreeRegressor, GradientBoostingRegressor, RandomForestRegressor, SVR, LogisticRegression, SGDRegressor, ElasticNet, BayesianRidge]

    def __init__(self, *, goal: Literal['classification', 'regression'], preprocessor: Pipeline | ColumnTransformer = None) -> None:
        # load valid models
        if goal == 'classification':
            self.__models = self.__classification_models
        elif goal == 'regression':
            self.__models = self.__regression_models
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
        pass
