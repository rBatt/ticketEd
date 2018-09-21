


# info for nyc open data API
# project app id: e1dcefa7
# project api key: 50908d89c2c20a5555c4991a733f19a3


#%% Data Options
# grp_by_cols = ['ViolationCounty', 'SubDivision', 'datetime_rnd', 'dow', "hour", "month", "minute"] # columns on which to aggregate
grp_by_cols = ['ViolationCounty', 'ViolationPrecinct', 'datetime_rnd', 'dow', "hour", "month", "minute"] # columns on which to aggregate
rnd_freq = "15min"
#todo need to have this script include the creation of "blocks" that exist in the data_get_coord script ... i only created blocks there because i didn't want to query coordinates for every street address/ ticket, but the coordinate resolution should match the resolution of the data set i ultimate curate.


#%% Other libraries
import re
import matplotlib.pyplot as plt
import numpy as np


#%% Set up pycharm console display
import pandas as pd
desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)


#%% Create or check for postgres database
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import os

#  create a database (if it doesn't exist)
username = os.environ['HOME'] #'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

proj_dir = username + "/Documents/School&Work/Insight/parking"
file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"


#%% Unique elements of categorical columns
# u_county = engine.execute('SELECT DISTINCT "ViolationCounty" FROM "parkVio2017";').fetchall()  # 13
# u_precinct = engine.execute('SELECT DISTINCT "ViolationPrecinct" FROM "parkVio2017";').fetchall()  # 213
# u_street = engine.execute('SELECT DISTINCT "StreetName" FROM "parkVio2017";').fetchall()  # 84216 streets
# u_post = engine.execute('SELECT DISTINCT "ViolationPostCode" FROM "parkVio2017";').fetchall()  # 1159


#%% Overview Query of pv2017
con = None
con = psycopg2.connect(database = dbname, user = username)

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


#%%IMPORTANT: screen out dates that are out of range!!
# year_mask = 2016 <= pv17_from_sql["datetime_rnd"].dt.year.astype(float) and pv17_from_sql["datetime_rnd"].dt.year.astype(float) <= 2017
# year_mask = 2016 <= pv17_from_sql["datetime_rnd"].dt.year.astype(float) <= 2017
date_float = pv17_from_sql["datetime"].dt.year.astype(float)
year_mask_pt1 = 2016 <= date_float
year_mask_pt2 = date_float <= 2017
year_mask = year_mask_pt1 & year_mask_pt2
pv17_from_sql = pv17_from_sql[year_mask]


#%% Make round times and add time/ seasonality features
# rounding
pv17_from_sql["datetime_rnd"] = pv17_from_sql["datetime"].dt.ceil(rnd_freq)  # map(lambda x: x.ceil(rnd_freq))

# seasonality features
pv17_from_sql["dow"] = [i.dayofweek for i in pv17_from_sql["datetime"]] # a bit slow (this is called a list comprehension), maybe next time try map()
pv17_from_sql["hour"] = pv17_from_sql["datetime_rnd"].dt.hour
pv17_from_sql["month"] = pv17_from_sql["datetime_rnd"].dt.month
pv17_from_sql["minute"] = pv17_from_sql["datetime_rnd"].dt.minute


#%% Do aggregation for export
pv17_grp = pv17_from_sql.groupby(grp_by_cols).size().reset_index(name="counts")


#%% Make basline predictions via aggregating to weekday-hour
# Get baseline prediction
# Will be based purely on long-term averages of hour-dayofweek combinations
pv17_how_dour_long = pv17_grp.pivot_table(index=['hour','dow'], values='counts', aggfunc=np.mean)
pv17_how_dour_wide = pv17_grp.pivot_table(index=['hour'], columns='dow', values='counts', aggfunc=np.mean)
pv17_how_dour_wide


#%% Make heat map of number of tickets per dow-hour
import seaborn as sns
sns.heatmap(pv17_how_dour_wide, cmap='RdYlGn_r', linewidths=0.5, annot=True)
plt.show()

# plt.hist(np.log(pv17_grp["counts"]))
# plt.show()


#%% Export 15-min intermediate table for time series analysis in R
# function to regularize a time series
def regularize_ts(x, freq="15min"):
    x_ceil = x.dt.ceil(freq)
    # x_ceil = x.map(lambda x: x.ceil(freq))
    df = pd.Series(0, index=x_ceil).sort_index()
    xout = pd.Series(df.asfreq(freq).index)
    return xout


df = pd.Series(0, index=pv17_grp["datetime_rnd"].unique()).sort_index()
xout = pd.DataFrame(df.asfreq(rnd_freq).index)
xout.index.name = "datetime_rnd"

# pv17_grp_reg = pd.merge(xout, pv17_grp, how='inner', on="datetime_rnd")
pv17_grp.asfreq(rnd_freq)



pv17_grp.to_csv(proj_dir + "/data_int/pv17_grp.csv", index=False)

