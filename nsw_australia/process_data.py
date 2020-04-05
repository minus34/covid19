
import os
import psycopg2
import requests


def get_nsw_data():
    url = "https://data.nsw.gov.au/data/dataset/aefcde60-3b0c-4bc0-9af1-6fe652944ec2/resource/21304414-1ff1-4243-a5d2-f52778048b29/download/covid-19-cases-by-notification-date-and-postcode-local-health-district-and-local-government-area.csv"
    print("Getting NSW COVID19 Data")
    r = requests.get(url)
    with open(os.path.join("input_files/nsw-covid19-data.csv"), 'w') as f:
        f.write(r.text)

    print("NSW COVID19 Data saved")


# get data
get_nsw_data()

# connect to postgres
pg_connect_string = "dbname='geo' host='localhost' port='5432' user='postgres' password='password'"

pg_conn = psycopg2.connect(pg_connect_string)
pg_conn.autocommit = True
pg_cur = pg_conn.cursor()

# import data
sql = open("import_into_postgres.sql", "r").read()
pg_cur.execute(sql)

print("Data imported into postgres")

# close Postgres connection
pg_cur.close()
pg_conn.close()
