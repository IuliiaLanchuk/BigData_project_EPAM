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
    archive.extractall('.')
    print('ZIP Extracted.')
    archive.close()
    csv_files = (file_info.filename for file_info in archive.infolist())
    all_data = (data_cleaning_from_invalid(pd.read_csv(csv_file, sep=',', verbose=True, encoding="utf-8",
                                                        index_col='Id')) for csv_file in csv_files)
    return get_top_cities_with_max_hotels(pd.concat(all_data))


def data_cleaning_from_invalid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Getting rip from null values and invalid latitude and longitude values from file and return filtered dataframe.
    :param df: Dataframe with nulls and invalid longitude and latitude values.
    :return: Pandas dataframe with filtered data.
    """
    df = df.dropna()
    long = 'Longitude'
    lat = 'Latitude'
    df_valid_lat = df[df[lat].apply(regex_filter, parameter=lat)]
    df_valid_lat_long = df_valid_lat[df_valid_lat[long].apply(regex_filter, parameter=long)]
    # df_valid_lat_long[lat] = float(df_valid_lat_long.loc[:, lat].values[0])
    # df_valid_lat_long[long] = float(df_valid_lat_long.loc[:, long].values[0])
    df_valid_lat_long['Latitude'] = df_valid_lat_long['Latitude'].apply(lambda lat: float(lat))
    df_valid_lat_long['Longitude'] = df_valid_lat_long['Longitude'].apply(lambda long: float(long))
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


def get_top_cities_with_max_hotels(df: pd.DataFrame) -> pd.DataFrame:
    """
    According data grouped by country, city finds one city in every country with the biggest amount of hotels in it. Return
    dataframe with full info about top cities, hotel parameters.
    :param df: Pandas dataframe with data to group.
    :return: Pandas dataframe with data.
    """
    df_grouped = df.groupby(['Country', 'City']).size().to_frame('Size').reset_index().drop_duplicates(
        ['Country', 'Size'])
    top_cities_with_max_hotels_df = df_grouped.sort_values('Size', ascending=False).drop_duplicates(['Country'])
    return df.loc[df['City'].isin(top_cities_with_max_hotels_df["City"].values)]
