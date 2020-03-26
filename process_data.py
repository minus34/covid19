
import csv
import get_data
import os

from datetime import datetime


def clean_int(value):
    if value == '':
        return None
    else:
        return int(value)


# download data files
jhu_files = get_data.get_jhu_data()
# wb_file = get_data.get_world_bank_data()

# UN 2020 Population Projections by Country (Copyright © United Nations)
# https://population.un.org/wpp/Download/Standard/Population/
un_file = 'input_files/un_2020_population_estimates_by_country.csv'


jhu_dict_list = list()

# parse and reformat John Hopkins University COVID-19 files
for filename in jhu_files:
    print("parsing {}".format(filename))

    with open(os.path.join("input_files", filename), "r") as f:
        reader = csv.reader(f, delimiter=',')

        # fartarse around with filenames to get the data type (confirmed, recovered or deaths)
        temp_filename = filename.replace("_global", "").replace(".csv", "").replace("_", "-")
        temp_filename_parts = temp_filename.split("-")

        # if len(temp_filename_parts) > 4:
        status = temp_filename_parts[3].lower()
        # else:
        #     status = temp_filename_parts[3].lower()

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

                    # convert date to standard format -- temp bug in resolved data need to be overcome
                    if status == "recovered":
                        the_date = datetime.strptime(date, '%m/%d/%Y')
                    else:
                        the_date = datetime.strptime(date, '%m/%d/%y')

                    row_dict["the_date"] = the_date
                    row_dict["persons"] = clean_int(values[j])

                    jhu_dict_list.append(row_dict)

                    j += 1
            i += 1

print("JHU files parsed into dictionary list")

# export dict list to CSV
csv_columns = ["status", "province_state", "country_region", "latitude", "longitude", "the_date", "persons"]

with open(os.path.join("output_files/", "time_series_19-covid-reformatted.csv"), 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in jhu_dict_list:
        writer.writerow(data)

print("JHU data exported to CSV")

# parse and reformat World Bank population data
print("parsing {}".format(un_file))

un_dict_list = list()

with open(un_file, "r") as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # skip the header row

    # parse data into lists
    for row in reader:
        row_dict = dict()

        row_dict["country_name"] = row[0]
        row_dict["country_code"] = int(row[1])
        row_dict["year"] = 2020

        string_pop = row[2].replace(" ", "")
        row_dict["population"] = int(string_pop) * 1000

        un_dict_list.append(row_dict)

print("UN pop. file parsed into dictionary list")

# export dict list to CSV
csv_columns = ["country_name", "country_code", "year", "population"]

with open(os.path.join("output_files/", "un-population-reformatted.csv"), 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in un_dict_list:
        writer.writerow(data)


# # parse and reformat World Bank population data
# print("parsing {}".format(wb_file))
#
# wb_dict_list = list()
#
# with open(wb_file, "r") as f:
#     reader = csv.reader(f, delimiter=',')
#
#     # ignore first 4 rows
#     for i in range(4):
#         next(reader)
#
#     i = 0
#     years = list()
#
#     # parse data into lists
#     for row in reader:
#         if i == 0:
#             # get date list from column names
#             years = row[4:]
#         else:
#             # get values for each year
#             values = row[4:]
#
#             j = 0
#
#             for year in years:
#                 if year != "":  # blank year in last column in CSV
#                     row_dict = dict()
#
#                     row_dict["country_name"] = row[0]
#                     row_dict["country_code"] = row[1]
#                     row_dict["year"] = int(year)
#                     row_dict["population"] = clean_int(values[j])
#
#                     wb_dict_list.append(row_dict)
#
#                 j += 1
#         i += 1
#
# print("World Bank pop. file parsed into dictionary list")
#
# # export dict list to CSV
# csv_columns = ["country_name", "country_code", "year", "population",]
#
# with open("world-bank-population-reformatted.csv", 'w') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
#     writer.writeheader()
#     for data in wb_dict_list:
#         writer.writerow(data)

# OPTIONAL: import data into Postgres

import psycopg2

pg_connect_string = "dbname='geo' host='localhost' port='5432' user='postgres' password='password'"

pg_conn = psycopg2.connect(pg_connect_string)
pg_conn.autocommit = True
pg_cur = pg_conn.cursor()

# import data
sql = open("import_into_postgres.sql", "r").read()
pg_cur.execute(sql)

print("Data imported into postgres")

# aggregate data by country and add population data
sql = open("aggregate_by_country.sql", "r").read()
pg_cur.execute(sql)

print("Country table created with cases and population data")

# check for countries that have no population data
sql = "select country_region from covid19.countries where population is null and country_region <> 'Diamond Princess'"
pg_cur.execute(sql)
rows = pg_cur.fetchall()

for row in rows:
    print("WARNING - missing pop. data for : {}".format(row[0]))

# close Postgres connection
pg_cur.close()
pg_conn.close()
