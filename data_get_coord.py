

import pandas as pd
import re
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2


desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)



#%% Begin playing with ways to add coordinates to the database
proj_dir = "/Users/Battrd/Documents/School&Work/Insight/parking"
file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"

username = 'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
con = None
con = psycopg2.connect(database = dbname, user = username)
sql_query = """
  SELECT "index", "SummonsNumber", "IssueDate", "ViolationCode", "ViolationLocation", "ViolationPrecinct", "ViolationTime", "ViolationCounty", "HouseNumber", "StreetName", "IntersectingStreet", "SubDivision", "ViolationLegalCode", "ViolationPostCode"
  FROM "parkVio2017"
  WHERE "ViolationCounty" = 'NY';
"""
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


#%% Get street names and house names
all_noneInd = pv17_from_sql["StreetName"].map(lambda x: not x is None)
all_stNames = pv17_from_sql[all_noneInd & ~int_logic]["StreetName"] # all street names that aren't none or part of intersections
# all_stNames = reduce_stNames(all_stNames) # use this if i want to try to standardize street names .. just tested, took a while, and seemed to return nothing ... be careful
# len(all_stNames.unique()) # 10283
all_county = pv17_from_sql[all_noneInd & ~int_logic]["ViolationCounty"] # used to specify the borough of the violation in the query

# Get house names
hn = pd.Series(pv17_from_sql[all_noneInd & ~int_logic]["HouseNumber"]) # get house names that are not part of intersections, and not associated with 'none' for street
hn_miss_skip = hn.map(lambda x: x is None) | pd.isnull(hn) # sum(hn_miss_skip) == 5710
hn[hn.map(lambda x: x is None)] = "" # if the house name is 'none', just set it to an empty string (see todo below ... need to skip over these when querying)
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
import numpy as np
hn_block = (np.multiply(np.floor(hn_block.astype(float)/100), 100)).astype(int) + 50


# str_length = hn_block.map(lambda x: min(3, len(x))) - 1
# denom = str_length.map(lambda x: pow(10, x))
# (np.multiply(np.floor(np.divide(hn_block.astype(float), denom)), denom)).astype(int)

block_st = hn_block.astype(str) + " " + all_stNames + ", " + all_county + ", NY"
block_st_dups_noHN = pd.Series(0, index=block_st).index.duplicated() | hn_miss_skip
# block_st_uni = block_st[~block_st_dups] # block_st.unique() # gives same; 31857 elements
block_st_uni = block_st[~block_st_dups_noHN] # both unduplicated AND not a missing house number; 30595 elements
block_st_uni = block_st_uni.values


#%% Build a key back to original dataframe, and also store lon lat in it
# key back to original
pv17_sub_key = pv17_from_sql[all_noneInd & ~int_logic][["SummonsNumber","HouseNumber","StreetName","ViolationCounty"]][~block_st_dups_noHN] # create this so easy to merge results back into original
pv17_sub_key["block_st_uni"] = block_st_uni

# store lon lat
pv17_sub_key['lon'] = 0.0
pv17_sub_key['lat'] = 0.0

pv17_sub_key = pv17_sub_key.reset_index()

# write to csv, to start
# pv17_sub_key.to_csv(proj_dir+"/data_int/pv17_sub_key_coords.csv", index=False)


#%% Loop through cleaned up block names and query NYC API for Lon/Lat
import json
import urllib
import time
app_id = "e1dcefa7"
app_key = "50908d89c2c20a5555c4991a733f19a3"
coordBaseURL = "https://api.cityofnewyork.us//geoclient//v1//search.json?" # can replace 'search' with 'address' if that's what you have

lon_vec = [None] * len(block_st_uni)
lat_vec = [None] * len(block_st_uni)

# for i in iter([0, 1, 2, 3]):
for i in range(len(block_st_uni)):
# for i in iter([0, 1, 2, 3]):
    t_add = block_st_uni[i]

    try:
        urlAddr = coordBaseURL + urllib.parse.urlencode({"input": t_add, "app_id": app_id, "app_key": app_key})  # the input is just street code 1 and 2 from row 3, concat'd
        response = urllib.request.urlopen(urlAddr)
        data = json.load(response)
        retCode = data['results'][0]["response"]["geosupportReturnCode"]

        if retCode == "00" or retCode == "01":
            lon = data['results'][0]["response"]["longitude"]
            lat = data['results'][0]["response"]["latitude"]
        else:
            lon = 0.1
            lat = 0.1

    except Exception as e:
        lon = -0.1
        lat = -0.1

    lon_vec[i] = lon
    lat_vec[i] = lat

    # pv17_sub_key['lon'][i] = lon
    # pv17_sub_key['lat'][i] = lat
    pv17_sub_key.loc[i, 'lon'] = lon
    pv17_sub_key.loc[i, 'lat'] = lat

    rand_pause = np.random.uniform(0.2, 0.9)
    time.sleep(rand_pause)
    if ((i+1) % 50) == 0:
        pv17_sub_key.to_csv(proj_dir+"/data_int/pv17_sub_key_coords_2018-09-20_13-56.csv", index=False)

# plt.plot(lon_vec, lat_vec)

#%% Below is old test code
# # urlAddr = coordBaseURL + urllib.parse.urlencode({"houseNumber": 35, "street": "E 21st St", "borough": 1, "app_id": app_id, "app_key": app_key})
# urlAddr = coordBaseURL + urllib.parse.urlencode({"input": st_hn_combos[0], "app_id": app_id, "app_key": app_key}) # the input is just street code 1 and 2 from row 3, concat'd
# response = urllib.request.urlopen(urlAddr)
# data = json.load(response)

# #%% Make true locations, get coordinates
# address_pieces = pv17_from_sql[["HouseNumber", "StreetName"]]
# address = address_pieces.iloc[:,0] + " " + address_pieces.iloc[:,1]
# pv17_from_sql["address"] = address
#
# #from geopy.geocoders import Nominatim
# # geolocator = Nominatim(user_agent="parking")
# # # location = geolocator.geocode("175 5th Avenue NYC") # test
# # blah = geolocator.geocode(address.head()) # keeps timing out
#
# from pygeocoder import Geocoder
# results = Geocoder.geocode("Tian'anmen, Beijing")
# print(results[0].coordinates)
#
# import json
# import urllib
# app_id = "e1dcefa7"
# app_key = "50908d89c2c20a5555c4991a733f19a3"
# coordBaseURL = "https://api.cityofnewyork.us//geoclient//v1//search.json?" # can replace 'search' with 'address' if that's what you have
#
# # urlAddr = coordBasURL + urllib.urlencode({"houseNumber": housenumber, "street": streetname, "borough": bc, "app_id": app_id, "app_key": app_key})
#
# # test format using insight address
# urlAddr = coordBaseURL + urllib.parse.urlencode({"houseNumber": 35, "street": "E 21st St", "borough": 1, "app_id": app_id, "app_key": app_key})
# response = urllib.request.urlopen(urlAddr)
# data = json.load(response)
#
#
# # some examples from 2017, location columns
# #    StreetCode1  StreetCode2  StreetCode3  ViolationLocation ViolationCounty HouseNumber            StreetName IntersectingStreet SubDivision
# # 0            0            0            0                NaN              BX        None  ALLERTON AVE (W/B) @         BARNES AVE           D
# # 1            0            0            0                NaN              BX        None  ALLERTON AVE (W/B) @         BARNES AVE           D
# # 2            0            0            0                NaN              BX        None  SB WEBSTER AVE @ E 1            94TH ST           C
# # 3        10610        34330        34350               14.0              NY         330               7th Ave               None          l2
# # 4        10510        34310        34330               13.0              NY         799               6th Ave               None          h1
# # can i use any of these to easily query?
# # goal would be to enter something like street code, and get a result that looks like the English descriptors
# # example of 10-digit BBL from API website: 1000670001
# # example of 7-digit BIN from API website: 1079043
# # coordBasURL_gen = "https://api.cityofnewyork.us//geoclient//v1//search.json?"
# # urlBBL = coordBasURL_gen + urllib.parse.urlencode({"input": "1000670001", "app_id": app_id, "app_key": app_key})
# # response = urllib.request.urlopen(urlBBL)
# # data = json.load(response)
#
# sc12_test_url = coordBasURL_gen + urllib.parse.urlencode({"input": "Webster and 94th", "app_id": app_id, "app_key": app_key}) # the input is just street code 1 and 2 from row 3, concat'd
# response = urllib.request.urlopen(sc12_test_url)
# data = json.load(response)
# data
