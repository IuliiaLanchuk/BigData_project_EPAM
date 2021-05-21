from weather_analysis.application.enriching_data.get_weather_df import get_weather_forecast, plots_creation
from weather_analysis.application.enriching_data.multithreading_address_enrichment import multithreading_data_enrichment_with_address
from weather_analysis.application.post_processing.post_processing import get_temperature_analysis
from weather_analysis.application.presettings.get_top_cities_data import get_top_cities_with_max_hotels
from weather_analysis.application.presettings.extract_and_validate import get_extracted_grouped_data


def main(input_folder: str, output_folder: str, n_threads: int) -> None:
    """
    Do analysis on data from input_folder and save results in output_folder.
    :param input_folder: Path to get data for analysis.
    :param output_folder: Path to save analysis results.
    :param n_threads: Amount of threads to execute task concurrently.
    """
    collected_valid_data = get_extracted_grouped_data(input_folder)
    df = get_top_cities_with_max_hotels(collected_valid_data)
    multithreading_data_enrichment_with_address(output_folder, df, n_threads)
    weather = get_weather_forecast(df, output_folder)
    plots_creation(weather, output_folder)
    get_temperature_analysis(weather)
