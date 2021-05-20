import os
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import pandas as pd

from googlegeocoder import GoogleGeocoder
from pandas import DataFrame


def multithreading_data_enrichment_with_address(output_folder: str, df: DataFrame, n_threads: int) -> pd.DataFrame:
    geocoder = GoogleGeocoder(os.environ.get("API_KEY")) or "No access to API_KEY"

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        df['Address'] = df.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1) \
            .apply(lambda coordinates: pool.submit(geocoder.get, coordinates)) \
            .apply(lambda future_result: future_result.result()[0])
        hotels_city_data_split_by_100_before_save(output_folder, df)
        return df


def hotels_city_data_split_by_100_before_save(output_folder: str, df: pd.DataFrame) -> None:
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


def save_hotels_info_in_csv(output_folder, df, city, counter):
    country = df['Country'].values[0]
    file_path = Path(f"{output_folder}/{country}/{city}")
    file_path.mkdir(exist_ok=True, parents=True)
    df.to_csv(path_or_buf=file_path / f"{city}_hotels_info_{counter}.csv", index=True, header=True, sep=',')
    print(
        f"Data for hotels in {city}, {country} was saved in file '{output_folder}/{country}/{city}_hotels_info_{counter}.csv'")
