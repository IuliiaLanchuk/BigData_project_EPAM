from post_processing import get_city_day_with_max_or_min_temp, get_city_day_with_max_change_of_day_temp, \
    get_city_with_max_change_of_max_temp
import pandas as pd

df = pd.read_csv('weather_example.csv', sep=',')


def test_get_city_day_with_max_temp():
    day_city_with_max_temp = get_city_day_with_max_or_min_temp(df, sort_column='temp_max, C', ascending=False,
                                                               criteria='Maximal')
    assert day_city_with_max_temp == "Maximal temperature is in Houston on 2021-05-14"


def test_get_city_day_with_min_temp():
    day_city_with_max_temp = get_city_day_with_max_or_min_temp(df, sort_column='temp_min, C', ascending=True,
                                                               criteria='Minimal')
    assert day_city_with_max_temp == "Minimal temperature is in Amsterdam on 2021-05-13"


def test_get_city_day_with_max_change_of_day_temp():
    city_day_with_max_change_of_day_temp = get_city_day_with_max_change_of_day_temp(df)
    assert city_day_with_max_change_of_day_temp == "Barcelona shows maximum difference between max and min temperature on 2021-05-13"


def test_get_city_with_max_change_of_max_temp():
    city_with_max_change_of_max_temp = get_city_with_max_change_of_max_temp(df)
    assert city_with_max_change_of_max_temp == "Barcelona is the city with maximum change of maximal temperature during 2021-05-13 - 2021-05-14"
