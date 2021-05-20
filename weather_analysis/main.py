import pandas as pd

from get_weather_df import get_weather_forecast, plots_creation
from multithreading_address_enrichment import multithreading_data_enrichment_with_address
from post_processing import temperature_calculations
from presettings import get_extracted_grouped_data


def main(path_to_data: str, output_folder: str, n_threads: int):
    df = get_extracted_grouped_data(path_to_data)

    # data_enriched_with_address = multithreading_data_enrichment_with_address(output_folder, df.iloc[:3], n_threads)
    data_enriched_with_address = pd.read_csv('data_with_address_enrich.csv')

    # weather = get_weather_forecast(df)

    # weather_to_csv = weather.to_csv('weather_11days.csv', index=True, header=True)
    weather = pd.read_csv('weather_11days.csv')
    plots_creation(weather, output_folder)

    temperature_calculation = temperature_calculations(weather)

