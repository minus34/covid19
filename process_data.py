
import csv

from datetime import datetime
from get_data import get_data

# get_data()

files = [
    "time_series_19-covid-Confirmed.csv",
    "time_series_19-covid-Deaths.csv",
    "time_series_19-covid-Recovered.csv"
]

dict_list = list()

for filename in files:
    print("parsing {}".format(filename))

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=',')

        status = filename.replace(".csv", "").split("-")[2].lower()

        i = 0
        dates = list()

        # parse data into lists
        for row in reader:
            if i == 0:
                # get date list from column names
                dates = row[4:]
            else:
                # get values for each day
                values = row[4:]

                j = 0

                for date in dates:
                    row_dict = dict()

                    row_dict["status"] = status
                    row_dict["province_state"] = row[0]
                    row_dict["country_region"] = row[1]
                    row_dict["latitude"] = float(row[2])
                    row_dict["longitude"] = float(row[3])

                    # convert date to standard format
                    the_date = datetime.strptime(date, '%m/%d/%y')

                    row_dict["the_date"] = the_date
                    row_dict["persons"] = int(values[j])

                    dict_list.append(row_dict)

                    j += 1
            i += 1
