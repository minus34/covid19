
import csv
import get_data

from datetime import datetime


def clean_int(value):
    if value == '':
        return None
    else:
        return int(value)


# download data files
get_data.get_jhu_data()
get_data.get_world_bank_data()

jhu_files = [
    "time_series_19-covid-Confirmed.csv",
    "time_series_19-covid-Deaths.csv",
    "time_series_19-covid-Recovered.csv"
]

wb_file = "API_SP.POP.TOTL_DS2_en_csv_v2_821007.csv"

jhu_dict_list = list()

# parse and reformat John Hopkins University COVID-19 files
for filename in jhu_files:
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
                    row_dict["persons"] = clean_int(values[j])

                    jhu_dict_list.append(row_dict)

                    j += 1
            i += 1

print("JHU files parsed into dictionary list")

# export dict list to CSV
csv_columns = ["status", "province_state", "country_region", "latitude", "longitude", "the_date", "persons"]

with open("time_series_19-covid-reformatted.csv", 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in jhu_dict_list:
        writer.writerow(data)

print("JHU data exported to CSV")


# parse and reformat World Bank population data
print("parsing {}".format(wb_file))

wb_dict_list = list()

with open(wb_file, "r") as f:
    reader = csv.reader(f, delimiter=',')

    # ignore first 4 rows
    for i in range(4):
        next(reader)

    i = 0
    years = list()

    # parse data into lists
    for row in reader:
        if i == 0:
            # get date list from column names
            years = row[4:]
        else:
            # get values for each year
            values = row[4:]

            j = 0

            for year in years:
                if year != "":  # blank year in last column in CSV
                    row_dict = dict()

                    row_dict["country_name"] = row[0]
                    row_dict["country_code"] = row[1]
                    row_dict["year"] = int(year)
                    row_dict["population"] = clean_int(values[j])

                    wb_dict_list.append(row_dict)

                j += 1
        i += 1

print("World Bank pop. file parsed into dictionary list")

# export dict list to CSV
csv_columns = ["country_name", "country_code", "year", "population",]

with open("world-bank-population-reformatted.csv", 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in wb_dict_list:
        writer.writerow(data)
