import os
import re
import zipfile
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import click
import pandas as pd
from pandas import DataFrame
from googlegeocoder import GoogleGeocoder


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


def multithreading_row_enrichment_with_address(data_select_cities: DataFrame, threads_amount: int):
    geocoder = GoogleGeocoder(os.environ.get("API_KEY"))

    with ThreadPoolExecutor(max_workers=threads_amount) as pool:
        data_select_cities['Address'] = data_select_cities.apply(lambda row: (row["Latitude"], row["Longitude"]), axis=1) \
            .apply(lambda coordinates: pool.submit(geocoder.get, coordinates)) \
            .apply(lambda future_result: future_result.result()[0])

        data_select_cities.to_csv('data_with_address_enrich.csv', index=True, header=True)
        return data_select_cities


@click.command()
def main():
    print('Extracting ZIP.')
    dir_path = Path(os.path.dirname(__file__))
    file_path = dir_path / 'data/hotels.zip'
    archive = zipfile.ZipFile(file_path, 'r')
    archive.extractall('.')
    print('ZIP Extracted.')
    archive.close()
    csv_files = (file_info.filename for file_info in archive.infolist())
    all_data = (data_preparing(csv_file) for csv_file in csv_files)

    data_concat = pd.concat(all_data)

    data_concat_size = data_concat.groupby(['Country', 'City']).size().to_frame('Size').reset_index()
    get_rid_duplicates = data_concat_size.drop_duplicates(['Country', 'Size'])
    data_concat_size_max = get_rid_duplicates.sort_values('Size', ascending=False).drop_duplicates(['Country'])

    top_cities_with_max_hotels = data_concat_size_max["City"].values
    data_select_cities = data_concat.loc[data_concat['City'].isin(top_cities_with_max_hotels)]

    data_enriched_with_address = multithreading_row_enrichment_with_address(data_select_cities, 50)
    # data_enriched_with_address = pd.read_csv('data_with_address_enrich.csv')
    print(data_enriched_with_address)


if __name__ == "__main__":
    main()
