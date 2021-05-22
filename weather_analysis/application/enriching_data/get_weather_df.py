from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta, date
from pathlib import Path
from typing import List

import pandas as pd
import requests
from pandas import DataFrame
import matplotlib.pyplot as plt

DAYS_AMOUNT = 11


def get_weather_forecast(df: pd.DataFrame, output_folder: str, n_threads: int) -> pd.DataFrame:
    """
    Return dataFrame of weather parameters of center cities for period of 5 days before today, today and 5 days next.
    :param df: Pandas dataframe with data without weather info.
    :param output_folder: Path to save data results.
    :param n_threads: Amount of threads.
    :return: Pandas dataframe with data with weather info.
    """
    center_coords = get_city_center_coordinates(df.groupby(["City"]))
    lat_long_pair = center_coords.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1)
    all_centres_ids = get_all_centers_ids(lat_long_pair, n_threads)
    weather_all_cities_11_days = get_weather_for_previous_5_days_today_and_next_5_days(all_centres_ids['Woeid'].values,
                                                                                       date.today(), n_threads)
    df_center_coords_and_ids = pd.concat([center_coords, all_centres_ids], axis=1)
    df_weather_and_coord_ids = df_center_coords_and_ids.merge(weather_all_cities_11_days, on=["Woeid"])
    save_center_info(df_weather_and_coord_ids, output_folder)
    return df_weather_and_coord_ids


def save_center_info(df: pd.DataFrame, output_folder: str) -> None:
    """
    Save country center data to csv file in output_folder.
    :param output_folder: Path to save data results.
    :param df: Pandas dataframe with data.
    """
    for country in df['Country'].drop_duplicates().values:
        data = df.loc[df['Country'].values == country]
        city = data['City'].values[0]
        file_path = Path(f"{output_folder}/{country}/{city}")
        file_path.mkdir(exist_ok=True, parents=True)
        data.to_csv(path_or_buf=file_path / f"center_info.csv", index=False, header=True, sep=',')
        print(
            f"Data for center in {country} was saved in file '{output_folder}/{country}/{city}/center_info.csv'")


def get_city_center_coordinates(data: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe of center latitude, longitude values and country name."""
    lat_max = data["Latitude"].max().sort_index()
    long_max = data["Longitude"].max().sort_index()
    lat_min = data["Latitude"].min().sort_index()
    long_min = data["Longitude"].min().sort_index()
    data = {'Latitude': ((lat_min + lat_max) / 2).values, 'Longitude': ((long_min + long_max) / 2).values,
            "Country": (i[1].values[0] for i in data['Country'])}
    return DataFrame(data)


def get_all_centers_ids(lat_long: pd.Series, n_threads: int) -> pd.DataFrame:
    """
    Return dataframe of center woeids and center city name.
    :param lat_long: Pandas Series object with latitude and longitude pair values.
    :param n_threads: Amount of threads.
    :return: Pandas dataframe of woeids and city names.
    """
    urls = [f'https://www.metaweather.com//api/location/search/?lattlong={i[0]},{i[1]}' for i in lat_long.values]
    data = {'City': [], 'Woeid': []}
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        responses = pool.map(requests.get, urls)
    for req in responses:
        if req.status_code == 200:
            response = req.json()[0]
            data['Woeid'].append(response["woeid"])
            data['City'].append(response["title"])
    return DataFrame(data)


def get_weather_for_previous_5_days_today_and_next_5_days(center_woeids: List[int], today: date, n_threads: int) -> pd.DataFrame:
    """
    Return dataframe of weather parameters of center cities for peroid of 5 days before today, today and 5 days next.
    :param center_woeids: List of centers woeids values.
    :param today: Day today.
    :param n_threads: Amount of threads.
    :return: Pandas dataframe of weather parameters of center cities.
    """
    date_range = pd.date_range((today - timedelta(days=5)), (today + timedelta(days=5))).strftime("%Y/%m/%d")
    base_url = 'https://www.metaweather.com/api/location/'
    urls = [f'{base_url}{woeid}/{date}/' for woeid in center_woeids for date in date_range]
    collected_data = {'Woeid': [id_ for id_ in center_woeids for i in range(11)], 'temp_min, C': [], 'temp_max, C': [],
              'day': []}
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        responses = pool.map(requests.get, urls)
    for res in responses:
        if res.status_code == 200:
            response = res.json()[0]
            collected_data['temp_min, C'].append(response["min_temp"])
            collected_data['temp_max, C'].append(response['max_temp'])
            collected_data['day'].append(response["applicable_date"])
    return DataFrame(collected_data)


def plots_creation(df: pd.DataFrame, output_folder: str) -> None:
    """
    Generate plot of day as x axis and maximum and minimum day temperatures in city center as y axis and save plot in
    output_folder.
    :param df: Pandas dataframe of weather info for all city centers.
    :param output_folder: Path to directory to save plots.
    """
    for i in range(0, len(df) - DAYS_AMOUNT + 1, DAYS_AMOUNT):
        data = df.loc[i:i + DAYS_AMOUNT - 1]
        x_axis = data['day'].values
        y_axis_min = data['temp_min, C'].values
        y_axis_max = data['temp_max, C'].values
        plt.plot(x_axis, y_axis_min, color='red', label='min', linestyle='-.')
        plt.plot(x_axis, y_axis_max, color='blue', label='max', linestyle='-.')
        plt.xticks(fontsize=5)
        plt.yticks(fontsize=8)
        plt.xlabel('Day')
        plt.ylabel('Temperature, C')
        city = data['City'].values[0]
        country = data['Country'].values[0]
        plt.title(f'Minimum and maximum temperature dependence in {city}, {country}')
        plt.legend(loc='upper left', fontsize=8)
        plot_save(plt, country, city, output_folder)


def plot_save(plt, country: str, city: str, output_folder: str) -> None:
    """Save plot in output_folder."""
    plot_path = Path(f"{output_folder}/{country}/{city}")
    plot_path.mkdir(exist_ok=True, parents=True)
    plt.savefig(fname=plot_path / f"min_max_temperature dependence in {city}.png", dpi=150)
    print(f"File 'Plot of min_max_temperature dependence in {city}.png' was created.")
    plt.close()
