import re
import zipfile
import pandas as pd


def get_extracted_grouped_data(path_to_data: str) -> pd.DataFrame:
    print('Extracting ZIP.')
    archive = zipfile.ZipFile(path_to_data, 'r')
    archive.extractall('.')
    print('ZIP Extracted.')
    archive.close()
    csv_files = (file_info.filename for file_info in archive.infolist())
    all_data = (data_preparing(csv_file) for csv_file in csv_files)
    return get_top_cities_with_max_hotels(pd.concat(all_data))


def data_preparing(filename):
    file_data = pd.read_csv(filename, delimiter=',', verbose=True, encoding="utf-8", index_col='Id')
    frame = file_data.dropna()
    filtered_frame = validate_latitude_longitude(frame)
    return filtered_frame


def validate_latitude_longitude(frame):
    long = 'Longitude'
    lat = 'Latitude'
    frame = frame[frame[lat].apply(regex_filter, parameter=lat)]
    filtered_frame = frame[frame[long].apply(regex_filter, parameter=long)]
    return filtered_frame


def regex_filter(value, parameter) -> bool:
    regexp = ''
    if parameter == 'Latitude':
        regexp = r'^(\+|-)?(?:90(?:(?:\.0{1,20})?)|(?:[0-9]|[1-8][0-9])(?:(?:\.[0-9]{1,20})?))$'
    if parameter == 'Longitude':
        regexp = r'^(\+|-)?(?:180(?:(?:\.0{1,20})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\.[0-9]{1,20})?))$'
    return re.match(regexp, str(value)) is not None


def get_top_cities_with_max_hotels(data_frame: pd.DataFrame):
    df_grouped = data_frame.groupby(['Country', 'City']).size().to_frame('Size').reset_index().drop_duplicates(
        ['Country', 'Size'])
    top_cities_with_max_hotels_df = df_grouped.sort_values('Size', ascending=False).drop_duplicates(['Country'])
    data_select_cities = data_frame.loc[data_frame['City'].isin(top_cities_with_max_hotels_df["City"].values)]

    data_select_cities['Latitude'] = data_select_cities['Latitude'].apply(lambda lat: float(lat))
    data_select_cities['Longitude'] = data_select_cities['Longitude'].apply(lambda lat: float(lat))
    return data_select_cities
