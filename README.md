To run this project:

1.Copy link to this repo `git clone https://github.com/IuliiaLanchuk/BigData_project_EPAM.git master`

2.Install python and then in command line add python and pip to PATH variables:
`setx PATH "C:\Users\User_name\AppData\Local\Programs\Python\Python39\;%PATH%"` and
`setx PATH "C:\Users\User_name\AppData\Local\Programs\Python\Python39\Scripts\;%PATH%"`

3.Set up used libs in this project: `pip install -r requirements.txt`

4.Execute command `python application_run.py` to run application with default values of `input_folder`, 
`output_folder` and `n_threads`, or with your own ones like `python application_run.py --input_folder=./weather_analysis/data/hotels.zip
--output_folder=./output_data --n_threads=30`. Be sure your data to analyse is in `input_folder`.

5.Wait for results in `output_folder`.

6.To use application you need to have API_KEY to Google Geocoder. To get it for free see here - 
https://developers.google.com/maps/documentation/geocoding/get-api-key

