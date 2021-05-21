import os
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import pandas as pd

from googlegeocoder import GoogleGeocoder
from pandas import DataFrame


def multithreading_data_enrichment_with_address(output_folder: str, df: DataFrame, n_threads: int) -> pd.DataFrame:
    """
    Do hotels data enrichment with address in terms of concurrency.
    :param output_folder: Path to save data results.
    :param df: Pandas dataframe with data.
    :param n_threads: Amount of threads for task execution concurrently.
    :return: Pandas dataframe with enriched hotels data with address.
    """
    geocoder = GoogleGeocoder(os.environ.get("API_KEY")) or "No access to API_KEY"
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        df['Address'] = df.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1) \
            .apply(lambda coordinates: pool.submit(geocoder.get, coordinates)) \
            .apply(lambda future_result: future_result.result()[0])
        print('Data about hotels addresses was collected.')
        hotels_city_data_split_by_100_before_save(output_folder, df)
        return df


def hotels_city_data_split_by_100_before_save(output_folder: str, df: pd.DataFrame) -> None:
    """
    Split data from csv files by 100 rows in each file and save data in output_folder.
    :param output_folder: Path to save data results.
    :param df: Pandas dataframe with data.
    :return: None.
    """
    top_cities = df['City'].drop_duplicates().values
    for city in top_cities:
        frame = df.loc[df['City'].values == city]
        counter = 1
        i = 0
        while i != len(frame):
            rest_df = frame.iloc[i:len(frame)]
            if len(rest_df) < 100:
                save_hotels_info_in_csv(output_folder, rest_df, city, counter)
                i += len(rest_df)
            else:
                df_100_elements = frame.iloc[i:i + 99]
                save_hotels_info_in_csv(output_folder, df_100_elements, city, counter)
                counter += 1
                i += 100


def save_hotels_info_in_csv(output_folder: str, df: pd.DataFrame, city: str, counter: int) -> None:
    """
    Save split data from csv files output_folder.
    :param output_folder: Path to save data results.
    :param df: Pandas dataframe with data.
    :param city: City name from data.
    :param counter: File number of a specific city.
    :return: None.
    """
    country = df['Country'].values[0]
    file_path = Path(f"{output_folder}/{country}/{city}")
    file_path.mkdir(exist_ok=True, parents=True)
    df.to_csv(path_or_buf=file_path / f"{city}_hotels_info_{counter}.csv", index=True, header=True, sep=',')
    print(
        f"Data for hotels in {city}, {country} was saved in file '{output_folder}/{country}/{city}_hotels_info_{counter}.csv'")
