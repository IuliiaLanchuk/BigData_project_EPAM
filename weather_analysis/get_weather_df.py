from datetime import timedelta, date
from pathlib import Path

import pandas as pd
import requests
from pandas import DataFrame
import matplotlib.pyplot as plt

DAYS_AMOUNT = 11


def get_weather_forecast(df):
    center_coords = get_city_center_coordinates(df.groupby(["City"]))
    all_centres_ids = get_all_centers_ids(
        center_coords.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1))
    weather_all_cities_11_days = get_weather_for_previous_5_days_today_and_next_5_days(all_centres_ids['Woeid'].values,
                                                                                       date.today())

    df_center_coords_and_ids = pd.concat([center_coords, all_centres_ids], axis=1)
    df_weather_and_coord_ids = df_center_coords_and_ids.merge(weather_all_cities_11_days, on=["Woeid"])
    return df_weather_and_coord_ids


def get_city_center_coordinates(data: pd.DataFrame) -> pd.DataFrame:
    lat_max = data["Latitude"].max().sort_index()
    long_max = data["Longitude"].max().sort_index()
    lat_min = data["Latitude"].min().sort_index()
    long_min = data["Longitude"].min().sort_index()
    data = {'Latitude': ((lat_min + lat_max) / 2).values, 'Longitude': ((long_min + long_max) / 2).values,
            "Country": (i[1].values[0] for i in data['Country'])}
    return DataFrame(data)


def get_all_centers_ids(lat_long) -> pd.DataFrame:
    urls = [f'https://www.metaweather.com//api/location/search/?lattlong={i[0]},{i[1]}' for i in lat_long.values]
    woeids = []
    cities = []
    for url in urls:
        req = requests.get(url)
        if req.status_code == 200:
            responce = req.json()[0]
            woeids.append(responce["woeid"])
            cities.append(responce["title"])
    data = {'City': cities, 'Woeid': woeids}
    return DataFrame(data)


def get_weather_for_previous_5_days_today_and_next_5_days(center_woeids, today) -> pd.DataFrame:
    date_range = pd.date_range((today - timedelta(days=5)), (today + timedelta(days=5))).strftime("%Y/%m/%d")
    base_url = 'https://www.metaweather.com/api/location/'
    urls = [f'{base_url}{woeid}/{date}/' for woeid in center_woeids for date in date_range]
    result = {'Woeid': [id_ for id_ in center_woeids for i in range(11)], 'temp_min, C': [], 'temp_max, C': [],
              'day': []}
    for url in urls:
        res = requests.get(url)
        if res.status_code == 200:
            response = res.json()[0]
            result['temp_min, C'].append(response["min_temp"])
            result['temp_max, C'].append(response['max_temp'])
            result['day'].append(response["applicable_date"])
    return DataFrame(result)


def plots_creation(df: pd.DataFrame, output_folder: str) -> None:
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


def plot_save(plt, country, city, output_folder) -> None:
    plot_path = Path(f"{output_folder}/{country}/{city}")
    plot_path.mkdir(exist_ok=True, parents=True)
    plt.savefig(fname=plot_path / f"min_max_temperature dependence in {city}.png", dpi=150)
    print(f"File 'min_max_temperature dependence in {city}.png' was created.")
    plt.close()
