
import csv

from get_data import get_data

# get_data()

files = [
    "time_series_19-covid-Confirmed.csv",
    "time_series_19-covid-Deaths.csv",
    "time_series_19-covid-Recovered.csv"
]

for filename in files:

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=',')

        i = 0
        num_columns = 0

        dates = list()
        countries = list()
        states = list()
        lats = list()
        longs = list()
        values = list()

        # parse data into lists
        for row in reader:
            if i == 0:
                num_columns = len(row)

                # get date list from column names
                dates = row[4:]
            else:
                states.append(row[0])
                countries.append(row[1])
                lats.append(row[2])
                longs.append(row[3])
                values.append(row[4:])

            i += 1

        print(values)

    # f = open(filename, 'r')
    # header_row = f.readline()
    # print(header_row)
