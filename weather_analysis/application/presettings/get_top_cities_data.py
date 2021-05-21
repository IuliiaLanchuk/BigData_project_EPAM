import pandas as pd


def get_top_cities_with_max_hotels(df: pd.DataFrame) -> pd.DataFrame:
    """
    According data grouped by country, city finds one city in every country with the biggest amount of hotels in it.
    Return dataframe with full info about top cities, hotel parameters.
    :param df: Pandas dataframe with data to group.
    :return: Pandas dataframe with data.
    """
    df_grouped = df.groupby(['Country', 'City']).size().to_frame('Size').reset_index().drop_duplicates(
        ['Country', 'Size'])
    top_cities_with_max_hotels_df = df_grouped.sort_values('Size', ascending=False).drop_duplicates(['Country'])
    return df.loc[df['City'].isin(top_cities_with_max_hotels_df["City"].values)]
