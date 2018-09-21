import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import psycopg2
import os


proj_dir = "~/Documents/School&Work/Insight/parking"

#%% Load forecasts from R-generated CSV
f_out = pd.read_csv(proj_dir + "/data_int/mvp_f_out.csv")

#%% Load geogrpahic coords from python query of nyc api
coords = pd.read_csv(proj_dir + "/data_int/pv17_sub_key_coords_2018-09-20_13-56.csv") # these coords are a little messed up because they have non-manhattan matches sometimes, working on fixing
coords = coords.drop(columns="index")
coords = coords[coords.lon < -1] # drop rows that are -0.1, 0, or 0.1, as there are hard-coded as exceptions or missing values or failures during the query process

#%% Load Original Data Set for Summons # (to match coords to subivision [in original data set] via SummonsNumber)
file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"

username = os.environ['HOME'] # 'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
con = None
con = psycopg2.connect(database = dbname, user = username)
sql_query = """
  SELECT "index", "SummonsNumber", "IssueDate", "ViolationCode", "ViolationLocation", "ViolationPrecinct", "ViolationTime", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision", "ViolationLegalCode", "ViolationPostCode"
  FROM "parkVio2017"
  WHERE "ViolationCounty" = 'NY';
"""
pv17_from_sql = pd.read_sql_query(sql_query,con, parse_dates={"IssueDate": "%m/%d/%Y"})
# pv17_from_sql = pv17_from_sql.drop(columns='index')
summs_subd = pv17_from_sql[['SummonsNumber','SubDivision']] # trim down to columns relevant to merge
summs_prect = pv17_from_sql[['SummonsNumber','ViolationPrecinct']] # trim down to columns relevant to merge


#%% Compute Average (mode?) Coordinates for Each SubDivision
md = pd.merge(summs_subd, coords, how='right', on="SummonsNumber")
md_pre = pd.merge(summs_prect, coords, how='right', on="SummonsNumber")



# subdivision doesn't appear to be spatially contiguous ...
# u_sd =  md.SubDivision.unique()
# for i in list(u_sd):
#     td = md[md.SubDivision==i] # [0]
#     plt.scatter(td.lon, td.lat, s=2)
# plt.show()


# precinct provides a pretty good spatial division
u_pre = md_pre.ViolationPrecinct.unique()
for i in list(u_pre[0:20]):
    td = md_pre[md_pre.ViolationPrecinct==i] # [0]
    plt.scatter(td.lon, td.lat, s=2)
plt.show()