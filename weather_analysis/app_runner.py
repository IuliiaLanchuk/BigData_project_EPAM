from main import main
import click


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option("--path_to_data", default="./data/hotels.zip", help="Path to get data for analysis")
@click.option("--output_folder", default="./output_data", help="Output folder to store the results.")
@click.option("--n_threads", default=50, help="Amount of threads")
def run_weather_analysis(path_to_data: str, output_folder: str, n_threads: int):
    """ Analyze files in the input 'path_to_data' path and stores the results to 'output_folder'."""
    main(path_to_data, output_folder, n_threads)


if __name__ == "__main__":
    run_weather_analysis()


# python app_runner.py --path_to_data=./data/hotels.zip --output_folder=./out --n_threads=50