import os

import pandas as pd

from weather_analysis.application.enriching_data.get_weather_df import get_city_center_coordinates


def test_get_city_center_coordinates():
    df_base = pd.read_csv(os.path.dirname(__file__) + '/data_to_test_get_center_coords.csv').groupby(["City"])
    df = get_city_center_coordinates(df_base)
    assert [lat for lat in df['Latitude'].values] == [4.0, 1.5]
    assert [long for long in df['Longitude'].values] == [2.5, 3.0]
