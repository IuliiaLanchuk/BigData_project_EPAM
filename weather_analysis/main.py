import asyncio

import pandas as pd

from get_weather_df import get_weather_forecast, plots_creation, save_center_info
from multithreading_address_enrichment import multithreading_data_enrichment_with_address
from post_processing import get_temperature_analysis
from presettings import get_extracted_grouped_data


def main(input_folder: str, output_folder: str, n_threads: int) -> None:
    """
    Do analysis on data from input_folder and save results in output_folder.
    :param input_folder: Path to get data for analysis.
    :param output_folder: Path to save analysis results.
    :param n_threads: Amount of threads to execute task concurrently.
    """
    df = get_extracted_grouped_data(input_folder)

    data_enriched_with_address = multithreading_data_enrichment_with_address(output_folder, df.iloc[:3], n_threads)
    # data_enriched_with_address = pd.read_csv('data_with_address_enrich.csv')

    weather = get_weather_forecast(df, output_folder)

    # weather_to_csv = weather.to_csv('weather_11days.csv', index=True, header=True)
    # weather = pd.read_csv('weather_11days.csv')

    plots_creation(weather, output_folder)
    temperature_analysis = get_temperature_analysis(weather)


# if __name__ == '__main__':
#     main(input_folder='./data/hotels.zip', output_folder='./out', n_threads=30)

