# This script will get the latest data from the Jons Hopkins GitHub repo https://github.com/CSSEGISandData
# code adapted from Nick Evershed's (The Guardian Australia) repo: https://github.com/guardian/coronavirus-live-data

import os
import requests
import zipfile


def get_jhu_data():

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
			
	print("John Hopkins University files saved")


def get_world_bank_data():

	path = "world_bank_population_by_country.zip"

	url = "http://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv"
	print("Getting", path)
	r = requests.get(url, stream=True)
	with open(path, 'wb') as f:
		for chunk in r.iter_content(chunk_size=1024):
			f.write(chunk)

	# extract data file from ZIP
	with zipfile.ZipFile(path, 'r') as zip_ref:
		zip_ref.extract("API_SP.POP.TOTL_DS2_en_csv_v2_821007.csv")

	# delete ZIP file
	os.remove(path)

	print("World Bank file saved")

# un-comment to just download the files:
# get_jhu_data()
# get_world_bank_data()