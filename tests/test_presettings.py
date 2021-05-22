import pandas as pd

from presettings import data_cleaning_from_invalid, get_top_cities_with_max_hotels

invalid_data = {
    "Country": ["US", "FR", "US", "RU", "AT", "FR", "AT", 'FR'],
    'Latitude': ['-34.234567', 98.4734682, '57.64787', None, 16.567893, '89.999999', '34.234.567', False],
    'Longitude': [-34.234567, '57.64787', 198.4734682, 45.434355, 0, '34....567', '179.999999', '87.8298294'],
}

data_filtered = {
    "Country": ["US"],
    'Latitude': [-34.234567],
    'Longitude': [-34.234567],
}


def test_data_cleaning_from_invalid():
    df_initial = pd.DataFrame(invalid_data)
    assert data_cleaning_from_invalid(df_initial)['Latitude'].values == pd.DataFrame(data_filtered)['Longitude'].values


def test_get_top_cities_with_max_hotels():
    df = pd.read_csv('hotels_example.csv')
    top_cities_with_max_hotels = [i for i in get_top_cities_with_max_hotels(df).drop_duplicates('City', keep='first')[
        'City'].values]
    assert top_cities_with_max_hotels == ['NewYork', 'Moscow']
