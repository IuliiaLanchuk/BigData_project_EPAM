**Project info:**

It is a console instrument for multithreaded data processing, resulting in accumulating results via API from Internet, and 
then displaying them on graphs.

Tool extracts csv files from zip archive and analyzes data about hotels, also gets rid from invalid data. Then finds 
one city in each country with the biggest amount of hotels in it. All data are grouped by country and city, added by 
address info for each hotel from the Internet by latitude and longitude and then presented in csv files for each country.
Next step is to in find coordinates of a center between equidistant far hotels in each city and for each center get weather
data for 5 previous days, today and 5 next days. Weather info for each city center is taken from Internet and
presented in csv files and in graphs.

**To run this project:**

1.Copy link to this repo `git clone https://github.com/IuliiaLanchuk/BigData_project_EPAM.git --branch=master`

2.Install python and then in command line add python and pip to PATH variables:
`setx PATH "C:\Users\User_name\AppData\Local\Programs\Python\Python39\;%PATH%"` and
`setx PATH "C:\Users\User_name\AppData\Local\Programs\Python\Python39\Scripts\;%PATH%"`

3.Set up used libs in this project: `pip install -r requirements.txt` 
(probably, with command `python -m pip install -r requirements.txt`)

4.Execute command `python application_run.py` to run application with default values of `input_folder`, 
`output_folder` and `n_threads`, or with your own ones like `python application_run.py --input_folder=./weather_analysis/data/hotels.zip
--output_folder=./output_data --n_threads=30`. Be sure your data to analyse is in `input_folder`. Firstly, you can use 
command `python application_run.py --help` to see available options**.

`input_folder`: Path to get data for analysis (type: str),

`output_folder`: Output folder to store the results (type: str),

`n_threads`: Amount of threads (type: int).


5.Wait for results in `output_folder`.

** To use application you need to have API_KEY to Google Geocoder. To get it for free see here - https://developers.google.com/maps/documentation/geocoding/get-api-key
Then, execute commands 

`export API_KEY`

`API_KEY=your key value`

