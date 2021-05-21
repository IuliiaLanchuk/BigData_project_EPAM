import click

from weather_analysis.application.main import main


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option("--input_folder", default="./weather_analysis/data/hotels.zip", help="Path to get data for analysis")
@click.option("--output_folder", default="./weather_analysis/output_data", help="Output folder to store the results.")
@click.option("--n_threads", default=50, help="Amount of threads")
def run_weather_analysis(input_folder: str, output_folder: str, n_threads: int):
    """ Analyze files in 'input_folder' and save analysis results in 'output_folder'."""
    main(input_folder, output_folder, n_threads)


if __name__ == "__main__":
    run_weather_analysis()


# python application_run.py --input_folder=./data/hotels.zip --output_folder=./out --n_threads=40