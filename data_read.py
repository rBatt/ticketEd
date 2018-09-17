#!/Users/Battrd/anaconda3/bin/python
# #!/usr/local/bin/python3

# info for nyc open data API
# project app id: e1dcefa7
# project api key: 50908d89c2c20a5555c4991a733f19a3


#%% Other libraries
import re

#%% Set up pycharm console display
import pandas as pd
desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',15)


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


#%% Unique elements of categorical columns
# u_county = engine.execute('SELECT DISTINCT "ViolationCounty" FROM "parkVio2017";').fetchall()  # 13
# u_precinct = engine.execute('SELECT DISTINCT "ViolationPrecinct" FROM "parkVio2017";').fetchall()  # 213
# u_street = engine.execute('SELECT DISTINCT "StreetName" FROM "parkVio2017";').fetchall()  # 84216 streets
# u_post = engine.execute('SELECT DISTINCT "ViolationPostCode" FROM "parkVio2017";').fetchall()  # 1159


#%% Overview Query of pv2017
con = None
con = psycopg2.connect(database = dbname, user = username)

headQ = """
    SELECT "StreetCode1", "StreetCode2", "StreetCode3", "ViolationLocation", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision"
    FROM "parkVio2017" 
    LIMIT 5;
"""
d = pd.read_sql_query(headQ,con)


sql_query = """
  SELECT "index", "SummonsNumber", "IssueDate", "ViolationCode", "ViolationLocation", "ViolationPrecinct", "ViolationTime", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision", "ViolationLegalCode", "ViolationPostCode"
  FROM "parkVio2017"
  WHERE "ViolationCounty" = 'NY';
"""
pv17_from_sql = pd.read_sql_query(sql_query,con, parse_dates={"IssueDate": "%m/%d/%Y"})
# pv17_from_sql.info()


#%% Format Columns
pv17_from_sql['IssueDate'] = pd.to_datetime(pv17_from_sql['IssueDate'], format="%m/%d/%Y")

# sts_in_13th = pv17_from_sql[pv17_from_sql.ViolationPrecinct==13].StreetName.unique()
# pv17_from_sql.IssueDate.head()



#%% Function to fix dates
def fix_dt(X):
    # start_date="2016-06-31", end_date="2017-06-30"
    # X is a pandas dataframe with a column for "IssueDate" and one for "ViolationTime"

    # # https://en.wikipedia.org/wiki/Fiscal_year#State_governments
    # # start_date = "2016-06-01"  # pd.to_datetime(["2016-06-01"])
    # # end_date = "2017-06-27"  # pd.to_datetime(["2017-12-31"])
    # mask = (X["IssueDate"] > pd.to_datetime(start_date)) & (X["IssueDate"] < pd.to_datetime(end_date))
    # X = X[mask]

    park_date = pd.Series(X["IssueDate"].dt.date.map(str))  # .reset_index(name="IssueDate")
    park_time = pd.Series(X["ViolationTime"])  # .reset_index(name="IssueDate")

    # import re
    bad_dot_time = park_time.map(lambda x: bool(re.match(".*\\..*", x)))
    park_time[bad_dot_time] = park_time[bad_dot_time].map(lambda x: re.sub("\\.", "0", x))

    double0_time = park_time.map(lambda x: x[0:2] == '00')  # 15008
    park_time[double0_time] = park_time[double0_time].map(lambda x: re.sub('^00', '12', x))

    # these 'short' times are missing am/pm indicator; assume am
    badTime_short = park_time.map(str).map(len) == 4  # 8 instances where this is true
    park_time[badTime_short] = park_time[badTime_short] + "A"

    # chage "A" to "AM", and "P" to "PM" so it conforms to convention
    park_time = park_time.str.replace('[A]+$','AM')  # sometimes there were indicators like "AA" instead of "AM" or "A"
    park_time = park_time.str.replace('[P]+$', 'PM')

    # create datetime object for output
    datetime_col = park_date + " " + park_time

    # Format to actual datetime object
    datetime_col = pd.to_datetime(datetime_col, format="%Y-%m-%d %I%M%p", errors='coerce')

    return datetime_col;


pv17_from_sql["datetime"] = fix_dt(pv17_from_sql)
pv17_from_sql = pv17_from_sql.drop(columns=["IssueDate", "ViolationTime"])


#%% Make day of week column
# TODO need to finish this and add it to the dataframe, will be a useful feature
dow_col = [i.dayofweek for i in datetime_col] # a bit slow (this is called a list comprehension), maybe next time try map()






#%% Make true locations, get coordinates
address_pieces = pv17_from_sql[["HouseNumber", "StreetName"]]
address = address_pieces.iloc[:,0] + " " + address_pieces.iloc[:,1]
pv17_from_sql["address"] = address

#from geopy.geocoders import Nominatim
# geolocator = Nominatim(user_agent="parking")
# # location = geolocator.geocode("175 5th Avenue NYC") # test
# blah = geolocator.geocode(address.head()) # keeps timing out

from pygeocoder import Geocoder
results = Geocoder.geocode("Tian'anmen, Beijing")
print(results[0].coordinates)

import json
import urllib
app_id = "e1dcefa7"
app_key = "50908d89c2c20a5555c4991a733f19a3"
coordBaseURL = "https://api.cityofnewyork.us//geoclient//v1//search.json?" # can replace 'search' with 'address' if that's what you have

# urlAddr = coordBasURL + urllib.urlencode({"houseNumber": housenumber, "street": streetname, "borough": bc, "app_id": app_id, "app_key": app_key})

# test format using insight address
urlAddr = coordBaseURL + urllib.parse.urlencode({"houseNumber": 35, "street": "E 21st St", "borough": 1, "app_id": app_id, "app_key": app_key})
response = urllib.request.urlopen(urlAddr)
data = json.load(response)


# some examples from 2017, location columns
#    StreetCode1  StreetCode2  StreetCode3  ViolationLocation ViolationCounty HouseNumber            StreetName IntersectingStreet SubDivision
# 0            0            0            0                NaN              BX        None  ALLERTON AVE (W/B) @         BARNES AVE           D
# 1            0            0            0                NaN              BX        None  ALLERTON AVE (W/B) @         BARNES AVE           D
# 2            0            0            0                NaN              BX        None  SB WEBSTER AVE @ E 1            94TH ST           C
# 3        10610        34330        34350               14.0              NY         330               7th Ave               None          l2
# 4        10510        34310        34330               13.0              NY         799               6th Ave               None          h1
# can i use any of these to easily query?
# goal would be to enter something like street code, and get a result that looks like the English descriptors
# example of 10-digit BBL from API website: 1000670001
# example of 7-digit BIN from API website: 1079043
# coordBasURL_gen = "https://api.cityofnewyork.us//geoclient//v1//search.json?"
# urlBBL = coordBasURL_gen + urllib.parse.urlencode({"input": "1000670001", "app_id": app_id, "app_key": app_key})
# response = urllib.request.urlopen(urlBBL)
# data = json.load(response)

sc12_test_url = coordBasURL_gen + urllib.parse.urlencode({"input": "Webster and 94th", "app_id": app_id, "app_key": app_key}) # the input is just street code 1 and 2 from row 3, concat'd
response = urllib.request.urlopen(sc12_test_url)
data = json.load(response)
data