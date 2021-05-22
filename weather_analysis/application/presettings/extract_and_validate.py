import re
import zipfile
from typing import Any

import pandas as pd


def get_extracted_grouped_data(input_folder: str) -> pd.DataFrame:
    """
    Extract csv files from zip archive and concatenate grouped data from files to one dataframe.
    :param input_folder: Path to archive with files.
    :return: Pandas dataframe with data about cities which have maximum hotels.
    """
    print('Extracting ZIP.')
    archive = zipfile.ZipFile(input_folder, 'r')
    archive.extractall()
    print('ZIP Extracted.')
    archive.close()
    all_data = (data_cleaning_from_invalid(pd.read_csv(file_info.filename, sep=',', verbose=True, encoding="utf-8",
                                                        index_col='Id')) for file_info in archive.infolist())

    return pd.concat(all_data)


def data_cleaning_from_invalid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Getting rip from null values and invalid latitude and longitude values from file and return filtered dataframe.
    :param df: Dataframe with nulls and invalid longitude and latitude values.
    :return: Pandas dataframe with filtered data.
    """
    df = df.dropna()
    df_valid_lat = df.loc[df.loc[:, 'Latitude'].apply(regex_filter, parameter='Latitude')]
    df_valid_lat_long = df_valid_lat.loc[
        df_valid_lat.loc[:, 'Longitude'].apply(regex_filter, parameter='Longitude')]
    df_valid_lat_long = df_valid_lat_long.assign(Latitude=pd.to_numeric(df_valid_lat_long.loc[:, 'Latitude']))
    df_valid_lat_long = df_valid_lat_long.assign(Longitude=pd.to_numeric(df_valid_lat_long.loc[:, 'Longitude']))
    return df_valid_lat_long


def regex_filter(value: Any, parameter: str) -> bool:
    """
    Return True if longitude or latitude value corresponds to regular expression, otherwise False.
    :param value: Longitude or latitude value.
    :param parameter: Longitude or latitude type object.
    :return: True or False.
    """
    limit_value = 90.00 if parameter == 'Latitude' else 180.00
    regexp = r"^(-?\d{1,3}\.\d{1,10})"
    if re.match(regexp, str(value)):
        try:
            float(value)
        except ValueError:
            return False
        else:
            return -limit_value <= float(value) <= limit_value
    return False
