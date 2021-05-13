"""main module"""
import os
import pickle
import pandas as pd
import numpy as np
from src.model.postprocessing import postprocessing
from src.model.preprocessing.preprocessing import transform_features, encode_features

CURRENT_DIRECTORY = os.path.dirname(__file__)


class TitanicClassificationModel:
    """Classification model"""

    def __init__(self, df_pred: pd.DataFrame = None, df_train: pd.DataFrame = None):
        """Initialization math classification model

        :param df_train: Pandas dataframe with people for model training
        :param df_pred: Pandas dataframe with people for classification
        """
        self.df_pred = df_pred
        self.df_train = df_train

    @property
    def input_columns(self):
        """Columns required for training"""
        return {'PassengerId', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin',
                'Embarked', 'lat', 'lng'}

    def train(self):
        """Train math model"""
        raise NotImplementedError

    @property
    def model(self):
        """Load model"""
        return self.restored_workspace['model']

    @property
    def encoders(self) -> dict:
        """Load model"""
        return self.restored_workspace['encoder']

    @property
    def restored_workspace(self):
        """Restore workspace from dump"""
        file_path = os.path.join(CURRENT_DIRECTORY, 'dumps/workspace.dump')
        return pickle.load(open(file_path, 'rb'))

    def __prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for classification"""
        if set(df.columns) != self.input_columns:
            raise BrokenPipeError(f'Passed dataframe contained inappropriate columns. Required: {self.input_columns}; '
                                  f'passed: {set(df.columns)}')
        df = transform_features(df)
        df = encode_features([df], self.encoders)[0]
        return df

    def __predict_data(self) -> np.array:
        prepared_df = self.__prepare_data(self.df_pred)
        return self.model.predict(prepared_df.drop('PassengerId', axis=1))

    def predict(self) -> pd.DataFrame:
        """Predict data"""
        predictions = self.__predict_data()
        return postprocessing.generate_output_df(self.df_pred, predictions)
