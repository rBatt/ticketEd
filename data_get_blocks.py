
import pandas as pd
import re
import psycopg2
import os
import numpy as np
import json
import urllib
import time


desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)


#%% Read Data from Postgres
# directories
username = os.environ['USER'] # 'Battrd'  # on computer, user name
proj_dir = "~/Documents/School&Work/Insight/parking"

# database
dbname = 'parkVio'  # name of database (not table)
con = None
con = psycopg2.connect(database = dbname, user = username)
sql_query = """
  SELECT "index", "SummonsNumber", "IssueDate", "ViolationCode", "ViolationLocation", "ViolationPrecinct", "ViolationTime", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision", "ViolationLegalCode", "ViolationPostCode"
  FROM "parkVio2017"
  WHERE "ViolationCounty" = 'NY';
"""

# query database
pv17_from_sql = pd.read_sql_query(sql_query,con, parse_dates={"IssueDate": "%m/%d/%Y"})


#%% Get street names and house names
int_logic = ~pd.isna(pv17_from_sql["IntersectingStreet"]) & pv17_from_sql["StreetName"].map(lambda x: not x is None)
all_noneInd = pv17_from_sql["StreetName"].map(lambda x: not x is None)
all_stNames = pv17_from_sql[all_noneInd & ~int_logic]["StreetName"] # all street names that aren't none or part of intersections
# all_stNames = reduce_stNames(all_stNames) # use this if i want to try to standardize street names .. just tested, took a while, and seemed to return nothing ... be careful
# len(all_stNames.unique()) # 10283
all_county = pv17_from_sql[all_noneInd & ~int_logic]["ViolationCounty"] # used to specify the borough of the violation in the query

# Get house names
hn = pd.Series(pv17_from_sql[all_noneInd & ~int_logic]["HouseNumber"]) # get house names that are not part of intersections, and not associated with 'none' for street
hn_miss_skip = hn.map(lambda x: x is None) | pd.isnull(hn) # sum(hn_miss_skip) == 5710
hn[hn.map(lambda x: x is None)] = "" # if the house name is 'none', just set it to an empty string
hn[pd.isnull(hn)] = "" # also set other null forms to empty string
# len(hn.unique()) # 10406

# cleaning stuff up so that it can be converted to integer
# just kept going on the regex's until the integer conversion worked
hn_block = hn.map(lambda x: re.sub("[a-zA-z]*", "", x))
hn_block = hn_block.map(lambda x: re.sub("[0-9]\\/[0-9]$", "", x))
hn_block = hn_block.map(lambda x: re.sub("\\+[0-9]*$", "", x))
hn_block = hn_block.map(lambda x: re.sub("\\=[0-9]*$", "", x))
hn_block = hn_block.map(lambda x: re.sub("\\-.*", "", x))
hn_block = hn_block.map(lambda x: re.sub(" [0-9]*$", "", x))
hn_block = hn_block.map(lambda x: re.sub(" [0-9]*$", "", x)) #idky, have to run twice
hn_block = hn_block.map(lambda x: re.sub("\\/", "0", x))
hn_block = hn_block.map(lambda x: re.sub("\\*", "0", x))
hn_block = hn_block.map(lambda x: re.sub("\\.[0-9]*", "0", x))
hn_block = hn_block.map(lambda x: re.sub("[{}]", "0", x))
hn_block = hn_block.map(lambda x: re.sub("^$", "0", x))

# convert cleaned up house numbers to a "block" by putting the number in 'hundreds'
hn_block = (np.multiply(np.floor(hn_block.astype(float)/100), 100)).astype(int) + 50


# str_length = hn_block.map(lambda x: min(3, len(x))) - 1
# denom = str_length.map(lambda x: pow(10, x))
# (np.multiply(np.floor(np.divide(hn_block.astype(float), denom)), denom)).astype(int)

#%% format output, and merge with other example or identifying columns
# by 'example', i mean that 'house number' and 'street name' might be
# one of several of such elements in those original columns that converged to a unique block_st
block_st = hn_block.astype(str) + " " + all_stNames + ", " + all_county + ", NY"

block_st_dups_noHN = pd.Series(0, index=block_st).index.duplicated() | hn_miss_skip
# block_st_uni = block_st[~block_st_dups] # block_st.unique() # gives same; 31857 elements
block_st_uni = block_st[~block_st_dups_noHN] # both unduplicated AND not a missing house number; 30595 elements
block_st_uni = block_st_uni.values

pv17_block_st_uni = pv17_from_sql[all_noneInd & ~int_logic][["SummonsNumber","HouseNumber","StreetName","ViolationCounty"]][~block_st_dups_noHN] # create this so easy to merge results back into original
pv17_block_st_uni["block_st_uni"] = block_st_uni

#%% write to csv
pv17_block_st_uni.to_csv(proj_dir+"/data_int/pv17_block_st_uni.csv", index=False) # passed to data_get_coord.py

pv17_block_st = pv17_from_sql[all_noneInd & ~int_logic][["SummonsNumber","HouseNumber","StreetName","ViolationCounty"]]
pv17_block_st['block_st'] = block_st
pv17_block_st.to_csv(proj_dir+"/data_int/pv17_block_st.csv", index=False) # passed to data_read.py so that each stored address can be associated with a block and coordinates