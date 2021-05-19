import json
import os
import re
import zipfile
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta, date
from pathlib import Path
import matplotlib.pyplot as plt

import click
import pandas as pd
from pandas import DataFrame
from googlegeocoder import GoogleGeocoder
import requests

DAYS_AMOUNT = 11


def regex_filter(value, parameter) -> bool:
    regexp = ''
    if parameter == 'Latitude':
        regexp = r'^(\+|-)?(?:90(?:(?:\.0{1,20})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,20})?))$'
    if parameter == 'Longitude':
        regexp = r'^(\+|-)?(?:180(?:(?:\.0{1,20})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,20})?))$'
    return re.match(regexp, str(value)) is not None


def validate_latitude_longitude(frame):
    long = 'Longitude'
    lat = 'Latitude'
    frame = frame[frame[lat].apply(regex_filter, parameter=lat)]
    filtered_frame = frame[frame[long].apply(regex_filter, parameter=long)]
    return filtered_frame


def data_preparing(filename):
    file_data = pd.read_csv(filename, delimiter=',', verbose=True, encoding="utf-8", index_col='Id')
    frame = file_data.dropna()
    filtered_frame = validate_latitude_longitude(frame)
    return filtered_frame


def multithreading_row_enrichment_with_address(df: DataFrame, threads_amount: int) -> DataFrame:
    geocoder = GoogleGeocoder(os.environ.get("API_KEY"))

    with ThreadPoolExecutor(max_workers=threads_amount) as pool:
        df['Address'] = df.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1) \
            .apply(lambda coordinates: pool.submit(geocoder.get, coordinates)) \
            .apply(lambda future_result: future_result.result()[0])

        # df.to_csv('data_with_address_enrich.csv', index=True, header=True)
        return df


def get_city_center_coordinates(data: pd.DataFrame) -> pd.DataFrame:
    lat_max = data["Latitude"].max().sort_index()
    long_max = data["Longitude"].max().sort_index()
    lat_min = data["Latitude"].min().sort_index()
    long_min = data["Longitude"].min().sort_index()
    data = {'Latitude': ((lat_min + lat_max) / 2).values, 'Longitude': ((long_min + long_max) / 2).values,
            "Country": (i[1].values[0] for i in data['Country'])}
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


def extracting_csv_files() -> pd.DataFrame:
    print('Extracting ZIP.')
    dir_path = Path(os.path.dirname(__file__))
    file_path = dir_path / 'data/hotels.zip'
    archive = zipfile.ZipFile(file_path, 'r')
    archive.extractall('.')
    print('ZIP Extracted.')
    archive.close()
    csv_files = (file_info.filename for file_info in archive.infolist())
    all_data = (data_preparing(csv_file) for csv_file in csv_files)
    return pd.concat(all_data)


def plot_save(plt, country, city) -> None:
    plot_path = Path(f"output_data/{country}/{city}")
    plot_path.mkdir(exist_ok=True, parents=True)
    plt.savefig(fname=plot_path / f"min_max_temperature dependence in {city}.png", dpi=150)
    print(f"File 'min_max_temperature dependence in {city}.png' was created.")
    plt.close()


def plots_creation(df: pd.DataFrame) -> None:
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
        plot_save(plt, country, city)


def get_top_cities_with_max_hotels(data_frame: pd.DataFrame):
    df_grouped = data_frame.groupby(['Country', 'City']).size().to_frame('Size').reset_index().drop_duplicates(
        ['Country', 'Size'])
    top_cities_with_max_hotels_df = df_grouped.sort_values('Size', ascending=False).drop_duplicates(['Country'])
    data_select_cities = data_frame.loc[data_frame['City'].isin(top_cities_with_max_hotels_df["City"].values)]

    data_select_cities['Latitude'] = data_select_cities['Latitude'].apply(lambda lat: float(lat))
    data_select_cities['Longitude'] = data_select_cities['Longitude'].apply(lambda lat: float(lat))
    return data_select_cities


def find_max_or_min_temp_city_and_day(df: pd.DataFrame, sort_key: str, ascending: bool, criteria: str) -> str:
    row = df.sort_values(sort_key, ascending=ascending).reset_index().loc[0]
    return "{criteria} temperature is in {city} on {day}".format(criteria=criteria, city=row['City'], day=row['day'])


def get_city_day_with_max_min_temp_change(base_df: pd.DataFrame) -> str:
    base_df['temp_change'] = base_df['temp_max, C'].values - base_df['temp_min, C'].values
    df = base_df.groupby(['City', 'temp_change', 'day']).size().to_frame('size').reset_index().drop_duplicates(
        subset="City", keep='last')
    row = df.sort_values('temp_change', ascending=False).reset_index().loc[0]
    return "{city} shows maximum difference between the maximum and minimum temperature on {day}".format(
        city=row['City'], day=row['day'])


def get_city_with_max_temp_max_change(base_df: pd.DataFrame) -> str:
    df = base_df.sort_values('temp_max, C').groupby(['City', 'temp_max, C']).size().to_frame('size').reset_index()
    df_min_temp = df.drop_duplicates(subset="City", keep='first')
    df_max_temp = df.drop_duplicates(subset="City", keep='last')
    df_min_temp['temp_change'] = df_max_temp['temp_max, C'].values - df_min_temp['temp_max, C'].values
    city = df_min_temp['City'][df_min_temp['temp_change'].values == df_min_temp['temp_change'].values.max()].values[0]
    return "{city} is the city with maximum change of maximal temperature during 11 days".format(city=city)


@click.command()
def main():
    data_frame = extracting_csv_files()
    data_select_cities = get_top_cities_with_max_hotels(data_frame)

    data_enriched_with_address = multithreading_row_enrichment_with_address(data_select_cities.iloc[:3], 50)
    data_enriched_with_address = pd.read_csv('data_with_address_enrich.csv')

    center_coords = get_city_center_coordinates(data_select_cities.groupby(["City"]))
    all_centres_ids = get_all_centers_ids(
        center_coords.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1))
    weather_all_cities_11_days = get_weather_for_previous_5_days_today_and_next_5_days(all_centres_ids['Woeid'].values,
                                                                                       date.today())

    df_center_coords_and_ids = pd.concat([center_coords, all_centres_ids], axis=1)
    df_weather_and_coord_ids = df_center_coords_and_ids.merge(weather_all_cities_11_days, on=["Woeid"])
    weather_to_csv = df_weather_and_coord_ids.to_csv('weather_11days.csv', index=True, header=True)
    weather = pd.read_csv('weather_11days.csv')
    plots_creation(weather)

    day_city_with_max_temp = find_max_or_min_temp_city_and_day(weather, sort_key='temp_max, C', ascending=False,
                                                               criteria='Maximal')
    day_city_with_min_temp = find_max_or_min_temp_city_and_day(weather, sort_key='temp_min, C', ascending=True,
                                                               criteria='Minimal')
    city_with_max_temp_change = get_city_with_max_temp_max_change(weather)
    city_day_with_max_min_temp_change = get_city_day_with_max_min_temp_change(weather)


if __name__ == "__main__":
    main()
