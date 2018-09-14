#!/Users/Battrd/anaconda3/bin/python
# #!/usr/local/bin/python3

#%% Notes about file sources

# Not using this b/c it seems too slow to be worth the saved disk space, and a bit annoying to use their query system
# Probably only worthwhile if I wanted to continuously update the data set
#query = ("https://data.cityofnewyork.us/resource/aagd-wyjz.json?"
#         "$limit=10000"
#         )
#raw_data = pd.read_json(query)

# Parking Violations:
#   2019 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2019/pvqr-7yc4
#   2018 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2018/a5td-mswe
#   2017 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2017/2bnn-yakx
#   2016 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2016/kiv2-tbus
#   2015 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2015/c284-tqph
#   2014 https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2014-August-/jt7v-77mi
#   **Note that 2014 is a partial year\n

# Open Parking and Camera Violations:
#   data https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89
#   eda vis https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/i4p3-pe6a

# Precinct shape (precinct_geo.csv):
#   https://data.cityofnewyork.us/Public-Safety/Police-Precincts/78dh-3ptz

# Parking Violation Codes:
#   https://data.cityofnewyork.us/Transportation/DOF-Parking-Violation-Codes/ncbg-6agr

#%% Create or check for postgres database
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

#  create a database (if it doesn't exist)
username = 'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

proj_dir = "/Users/Battrd/Documents/School&Work/Insight/parking"
file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"


#%% Generate table in postgres database
chunksize = 10000  # number of rows to read at a time
i = 0
j = 1
for df in pd.read_csv(file_parkVio2017, chunksize=chunksize, iterator=True):
      df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})  # removes spaces from any column names
      df.index += j
      i+=1
      df.to_sql("parkVio2017", engine, if_exists="append")
      j = df.index[-1] + 1

