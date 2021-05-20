import pandas as pd


def temperature_calculations(df: pd.DataFrame) -> None:
    day_city_with_max_temp = get_city_day_with_max_or_min_temp(df, sort_key='temp_max, C', ascending=False,
                                                               criteria='Maximal')
    day_city_with_min_temp = get_city_day_with_max_or_min_temp(df, sort_key='temp_min, C', ascending=True,
                                                               criteria='Minimal')
    city_with_max_change_of_max_temp = get_city_with_max_change_of_max_temp(df)
    city_day_with_max_change_of_day_temp = get_city_day_with_max_change_of_day_temp(df)
    print(day_city_with_min_temp, day_city_with_max_temp, city_with_max_change_of_max_temp,
          city_day_with_max_change_of_day_temp,
          sep='\n')


def get_city_day_with_max_or_min_temp(df: pd.DataFrame, sort_key: str, ascending: bool, criteria: str) -> str:
    row = df.sort_values(sort_key, ascending=ascending).reset_index().loc[0]
    return "{criteria} temperature is in {city} on {day}".format(criteria=criteria, city=row['City'], day=row['day'])


def get_city_day_with_max_change_of_day_temp(df: pd.DataFrame) -> str:
    df['temp_change'] = df['temp_max, C'].values - df['temp_min, C'].values
    row = df[df['temp_change'] == df['temp_change'].max()]
    return "{city} shows maximum difference between max and min temperature on {day}".format(city=row['City'].values[0],
                                                                                             day=row['day'].values[0])


def get_city_with_max_change_of_max_temp(base_df: pd.DataFrame) -> str:
    df = base_df.sort_values('temp_max, C').groupby(['City', 'temp_max, C']).size().to_frame('size').reset_index()
    df_min_temp = df.drop_duplicates(subset="City", keep='first')
    df_max_temp = df.drop_duplicates(subset="City", keep='last')
    df_min_temp['temp_change'] = df_max_temp.loc[:, 'temp_max, C'].values - df_min_temp.loc[:, 'temp_max, C'].values
    city = df_min_temp['City'][df_min_temp['temp_change'].values == df_min_temp['temp_change'].values.max()].values[0]
    return "{city} is the city with maximum change of maximal temperature during 11 days".format(city=city)
