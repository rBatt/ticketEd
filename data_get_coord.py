

import pandas as pd
import os
import numpy as np
import json
import urllib
import time


desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)


#%% Begin playing with ways to add coordinates to the database
username = os.environ['USER'] # 'Battrd'  # on computer, user name
proj_dir = "~/Documents/School&Work/Insight/parking"

pv17_block_st_uni = pd.read_csv(proj_dir+"/data_int/pv17_block_st_uni.csv")


#%% Build a key back to original dataframe, and also store lon lat in it
# key back to original
# pv17_sub_key = pv17_from_sql[all_noneInd & ~int_logic][["SummonsNumber","HouseNumber","StreetName","ViolationCounty"]][~block_st_dups_noHN] # create this so easy to merge results back into original
# pv17_sub_key["block_st_uni"] = block_st_uni

# store lon lat
pv17_block_st_uni['lon'] = 0.0
pv17_block_st_uni['lat'] = 0.0

pv17_block_st_uni = pv17_block_st_uni.reset_index()
block_st_uni = pv17_block_st_uni['block_st_uni']

# write to csv, to start
# pv17_sub_key.to_csv(proj_dir+"/data_int/pv17_sub_key_coords.csv", index=False)


#%% Loop through cleaned up block names and query NYC API for Lon/Lat
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
    pv17_block_st_uni.loc[i, 'lon'] = lon
    pv17_block_st_uni.loc[i, 'lat'] = lat

    rand_pause = np.random.uniform(0.2, 0.9)
    time.sleep(rand_pause)
    if ((i+1) % 50) == 0:
        pv17_block_st_uni.to_csv(proj_dir+"/data_int/pv17_sub_key_coords_INSERTDATE.csv", index=False)


# plt.scatter(np.array(lon_vec)[np.array(lon_vec)< -0.1], np.array(lat_vec)[np.array(lon_vec)<-0.1], s=2, c='black')
# plt.ylabel("Latitude")
# plt.xlabel("Longitude")
# # plt.show()
# plt.savefig(proj_dir + '/lon_lat_block_tickets.png', bbox_inches='tight', dpi=250)

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
