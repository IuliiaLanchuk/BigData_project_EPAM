"""module for generation output template"""
import pandas as pd
import numpy as np


def generate_output_df(df: pd.DataFrame, predictions: np.array) -> pd.DataFrame:
    """Joining input dataframe with predicted values"""
    return df.join(pd.DataFrame(predictions, columns=['predictions']), )
