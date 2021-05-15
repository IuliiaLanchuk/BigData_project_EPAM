import os
import re
import zipfile

from pathlib import Path

import click

import pandas as pd


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
    print(data_concat_size_max)
    # print(data_concat_size_max["City"])


if __name__ == "__main__":
    main()
