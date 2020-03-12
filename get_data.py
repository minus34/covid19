# This script will get the latest data from the Jons Hopkins GitHub repo https://github.com/CSSEGISandData
# code from Nick Evershed, The Guardian Australia

import requests


def get_data():

	files = [
			"time_series_19-covid-Confirmed.csv",
			"time_series_19-covid-Deaths.csv",
			"time_series_19-covid-Recovered.csv"
			]

	headers = {'Accept': 'application/vnd.github.v3.raw'}

	for path in files:
		url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_time_series/{path}".format(path=path)
		print("Getting", path)
		r = requests.get(url, headers=headers)
		with open(path, 'w') as f:
			f.write(r.text)
			
	print("Files saved")


# un-comment to just download the files:
# getData()
