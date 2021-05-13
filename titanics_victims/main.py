"""Main module"""
import pandas as pd
from src.model.model import TitanicClassificationModel

if __name__ == '__main__':
    '''
    Run the script with arguments (path to the folder with data, threads number) (hint: argparse, multithreading)
    '''

    # Pandas documentation: https://pandas.pydata.org/pandas-docs/version/0.15/index.html
    df = pd.read_csv('path_to_file/path_to_files', header=0)

    '''
    Check quality of passed dataset:
        - number of rows
        - columns (maybe via schema)

    Implement your business logic (filtering and enrich data)
        - delete rows if "Age" = None AND "Cabin" = None
        - add column "lat", "lng" using https://opencagedata.com/ by field "Address". If "Address" is empty or API 
           doesn't return an appropriate result, you should fill these rows by the average value of the column
        - drop column "Address"
    '''

    clf = TitanicClassificationModel(df)

    result_df = clf.predict()

    '''
    Implement you logic of separation data by field "Survived"
    Print statistic (how many people survived/not survived)
    '''

    # Result: 2 folders (Survived, NotSurvived)

    '''
    Format the code 
    Cover the self-developed code by tests (covering the work with API is not necessary, only up to you)
    Write the documentation
    '''
