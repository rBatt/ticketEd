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
proj_dir = username + "/Documents/School&Work/Insight/parking"
file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"

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


# TODO working on stuff below ... trying to clean up streetName and IntersectingStreet so I can make a clean match to the LionIntersections.csv
# test = pd.read_csv(file_parkVio2017, nrows=10)
# test = test.rename(columns={c: c.replace(' ', '') for c in test.columns})
# ref_int = pd.read_csv(proj_dir+"/NYCParkingGeocode/LionIntersections.csv")

# test[~pd.isna(test["IntersectingStreet"])]
# int_logic = ~pd.isna(pv17_from_sql["IntersectingStreet"]) & pv17_from_sql["StreetName"].map(lambda x: not x is None)
#
# dat_ints = pv17_from_sql[int_logic].reset_index()
# dat_ints["StreetName"] = dat_ints["StreetName"].map(lambda x: x.lower())
# dat_ints["IntersectingStreet"] = dat_ints["IntersectingStreet"].map(lambda x: x.lower())
# remove_slash = "[wesnic]\\/[seoofb]{1,2}"
# dat_ints["StreetName"] = dat_ints["StreetName"].map(lambda x: re.sub(remove_slash, "",x))
# dat_ints["IntersectingStreet"] = dat_ints["IntersectingStreet"].map(lambda x: re.sub(remove_slash, "",x))
#
# remove_ftFrom = "[0-9]{1,3}ft"
# dat_ints["IntersectingStreet"] = dat_ints["IntersectingStreet"].map(lambda x: re.sub(remove_ftFrom, "",x))
#
# remove_suff = "[0-9]{1,3}[(th)|(st)|(rd)|(nd)]"
# dat_ints["StreetName"] = dat_ints["StreetName"].map(lambda x: re.sub(remove_suff, "",x))
# dat_ints["IntersectingStreet"] = dat_ints["IntersectingStreet"].map(lambda x: re.sub(remove_suff, "",x))



#%% Function to clean up/ standardize street names ... WiP
def reduce_stNames(vec):
    remove_slash = "[wesnic]\\/[seoofb]{1,2}"
    remove_ftFrom = "[0-9]{1,3}ft"
    remove_suff = "(?<=[0-9])(th|st|rd|nd)"
    # remove_stType = "[(st)|(av)|(ave)|(aly)|(street)|(ave)|(avenue)|(alley)]"

    vec = vec.map(lambda x: x.lower())
    vec = vec.map(lambda x: re.sub(remove_slash, "", x))
    vec = vec.map(lambda x: re.sub(remove_ftFrom, "", x))
    vec = vec.map(lambda x: re.sub(remove_suff, "", x))
    # vec = vec.map(lambda x: re.sub(remove_stType, "", x))

    vec = vec.map(lambda x: re.sub("^\\s*", "", x)) # leading whitespace
    vec = vec.map(lambda x: re.sub("\\s*$", "", x)) # trailing whitespace

    vec = vec.map(lambda x: re.sub("\\bave\\b", "avenue", x))
    vec = vec.map(lambda x: re.sub("\\bav\\b", "avenue", x))
    vec = vec.map(lambda x: re.sub("\\bst\\b", "street", x))
    vec = vec.map(lambda x: re.sub("\\baly\\b", "alley", x))
    vec = vec.map(lambda x: re.sub("\\brd\\b", "road", x))
    vec = vec.map(lambda x: re.sub("\\bpl\\b", "place", x))
    vec = vec.map(lambda x: re.sub("\\bln\\b", "lane", x))
    vec = vec.map(lambda x: re.sub("\\bdr\\b", "drive", x))

    vec = vec.map(lambda x: re.sub("\\bw\\b", "west", x))
    vec = vec.map(lambda x: re.sub("\\bs\\b", "south", x))
    vec = vec.map(lambda x: re.sub("\\be\\b", "east", x))
    vec = vec.map(lambda x: re.sub("\\bn\\b", "north", x))

    vec = vec.map(lambda x: re.sub("\\bbway\\b", "broadway", x))
    vec = vec.map(lambda x: re.sub("\\bpak\\b", "park", x))
    vec = vec.map(lambda x: re.sub("\\bpk\\b", "park", x))

    vec = vec.map(lambda x: re.sub("\\bfdr\\b", "fdr drive", x))

    return vec


#%% Get Intersections -- NOTE, logic used for other addresses, too
# logic to get intersections, and clean up intersection columns for those rows
int_logic = ~pd.isna(pv17_from_sql["IntersectingStreet"]) & pv17_from_sql["StreetName"].map(lambda x: not x is None)
dat_ints = pv17_from_sql[int_logic].reset_index()
dat_ints["StreetName"] = reduce_stNames(dat_ints["StreetName"])
dat_ints["IntersectingStreet"] = reduce_stNames(dat_ints["IntersectingStreet"])