import pandas as pd


def get_temperature_analysis(df: pd.DataFrame) -> None:
    """
    Do calculations with weather forecast data of all the cities. Return city and/or day with the maximum or minimum
    temperature or temperature change value during all the period.
    :param df: Pandas dataframe with weather forecast data.
    :return: None.
    """
    day_city_with_max_temp = get_city_day_with_max_or_min_temp(df, sort_column='temp_max, C', ascending=False,
                                                               criteria='Maximal')
    day_city_with_min_temp = get_city_day_with_max_or_min_temp(df, sort_column='temp_min, C', ascending=True,
                                                               criteria='Minimal')
    city_with_max_change_of_max_temp = get_city_with_max_change_of_max_temp(df)
    city_day_with_max_change_of_day_temp = get_city_day_with_max_change_of_day_temp(df)
    print(day_city_with_min_temp, day_city_with_max_temp, city_with_max_change_of_max_temp,
          city_day_with_max_change_of_day_temp,
          sep='\n')


def get_city_day_with_max_or_min_temp(df: pd.DataFrame, sort_column: str, ascending: bool, criteria: str) -> str:
    """
    Sort dataframe by 'sort_column' value and, according to criteria value, return city and day with the maximum or
    minimum temperature of all cities during all the period.
    :param df: Pandas dataframe with weather forecast data.
    :param sort_column: column name which values are to sort.
    :param ascending: True or False indicating ascending or descending order of sort.
    :param criteria: 'Minimal' or 'Maximal'.
    :return: string result about city and day.
    """
    row = df.sort_values(sort_column, ascending=ascending).reset_index().loc[0]
    return "{criteria} temperature is in {city} on {day}".format(criteria=criteria, city=row['City'], day=row['day'])


def get_city_day_with_max_change_of_day_temp(df: pd.DataFrame) -> str:
    """
    Return city and day with the maximum temperature change between minimal day temperature and maximal day temperature
    of all cities during all the period.
    :param df: Pandas dataframe with weather forecast data.
    :return: string result about city and day.
    """
    df['temp_change'] = df['temp_max, C'].values - df['temp_min, C'].values
    row = df[df['temp_change'] == df['temp_change'].max()]
    return "{city} shows maximum difference between max and min temperature on {day}".format(city=row['City'].values[0],
                                                                                             day=row['day'].values[0])


def get_city_with_max_change_of_max_temp(base_df: pd.DataFrame) -> str:
    """
    Return city with the maximum temperature change of maximal day temperature during all the period.
    :param base_df: Pandas dataframe with weather forecast data.
    :return: string result about city.
    """
    df = base_df.sort_values('temp_max, C').groupby(['City', 'temp_max, C']).size().to_frame('size').reset_index()
    df_min_temp = df.drop_duplicates(subset="City", keep='first')
    df_max_temp = df.drop_duplicates(subset="City", keep='last')
    df_min_temp['temp_change'] = df_max_temp.loc[:, 'temp_max, C'].values - df_min_temp.loc[:, 'temp_max, C'].values
    city = df_min_temp['City'][df_min_temp['temp_change'].values == df_min_temp['temp_change'].values.max()].values[0]
    days = base_df['day'].drop_duplicates().values
    return "{city} is the city with maximum change of maximal temperature during {day1} - {day11}".format(city=city,
                                                                                                          day1=days[0],
                                                                                                          day11=days[
                                                                                                              -1])
