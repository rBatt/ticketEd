


# info for nyc open data API
# project app id: e1dcefa7
# project api key: 50908d89c2c20a5555c4991a733f19a3


#%% Data Options
# grp_by_cols = ['ViolationCounty', 'SubDivision', 'datetime_rnd', 'dow', "hour", "month", "minute"] # columns on which to aggregate
# grp_by_cols = ['ViolationCounty', 'ViolationPrecinct', 'datetime_rnd', 'dow', "hour", "month", "minute"] # columns on which to aggregate
# grp_by_cols = ['ViolationCounty', 'ViolationPrecinct', 'datetime_rnd'] # columns on which to aggregate (excluding seasonality, b/c will add those prior to aggregation)
# grp_by_cols = ['ViolationCounty', 'ViolationPrecinct', 'block_st', 'datetime_rnd']
grp_by_cols = ['ViolationCounty', 'ViolationPrecinct', 'datetime_rnd']
rnd_freq = "15min"


#  todo need to have this script include the creation of "blocks" that exist in the data_get_coord script ... i only created blocks there because i didn't want to query coordinates for every street address/ ticket, but the coordinate resolution should match the resolution of the data set i ultimate curate.


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
username = os.environ['USER'] #'Battrd'  # on computer, user name
dbname = 'parkVio'  # name of database (not table)
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

proj_dir = "~/Documents/School&Work/Insight/parking"
# file_parkVio2017 = proj_dir + "/data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"


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


#%% Add precinct to block-st-coords data
# NOTE: It is important to do this merge early
# Due to all the subsequent subsetting of pv17_from_sql, and the nature of lookups of coords being for unique blocks
# Any subsetting of early rows in pv17_from_sql will tend to generate no matches in the summons numbers of pv17_block_st_uni_coords
# But the subsetting of pv17_from_sql was not because its ViolationPrecinct data were bad, so feel free to use for that here
summs_precinct = pv17_from_sql[['SummonsNumber',"ViolationPrecinct"]]
pv17_block_st = pd.read_csv(proj_dir+"/data_int/pv17_block_st.csv")[['SummonsNumber',"block_st"]]
summs_precinct_block = pd.merge(summs_precinct, pv17_block_st, on='SummonsNumber', how='outer')
# note that there are 569,988 NA's in the block_st column after the merge sum(pd.isna(summs_precinct_block['block_st']))
# and in data_get_block.py, the removal of intersection and no house numbers is about responsible for 533,934 of these sum(~(all_noneInd & ~int_logic))
# there are also 5710 rows for which the house name was missing sum(hn_miss_skip)
# pv17_from_sql[pv17_from_sql.SummonsNumber.isin(summs_precinct_block[all_noneInd & ~int_logic].SummonsNumber[pd.isna(summs_precinct_block[all_noneInd & ~int_logic].block_st)])]
# the above line shows the rows from pv17_from_sql where there is a match in SummonsNumber for summs_precinct_block (all of them, after left merge, oops), but has NA's there
# in other words, the original rows for the summons w/ NA's for block_st
# Visual inspection indicates that almost all of these rows have something odd about their time or date formatting. Dates w/ years far into future, or times that deviate from convention
# problem is, i don't see where i'm doing any subsetting to rule these rows out when i create the block_st object in data_get_blocks.py
# So I have no idea why there are so many NaN's, or why they are in useful places ..... ha
# BUT, half the point of bringing these block_st values in was so that we could have blocks and precincts and coords all matched up as best as possible
# AND, it looks like after the merge below (into locations_uni), there are no missing block_st_uni or ViolationPrecinct values, which means that
# any NaN's that were there before weren't looked up when finding coordinates anyway

# summs_precinct_block.drop(columns="SummonsNumber", inplace=True)
pv17_block_st_uni_coords = pd.read_csv(proj_dir + "/data_int/pv17_sub_key_coords_2018-09-20_13-56.csv")
# locations_uni = pd.merge(pv17_block_st_uni_coords, summs_precinct_block, on="SummonsNumber", how='left')[["ViolationCounty", "ViolationPrecinct", "block_st_uni","lon","lat"]]
locations = pd.merge(summs_precinct_block, pv17_block_st_uni_coords[['block_st_uni','lon','lat']], left_on="block_st", right_on="block_st_uni", how='left')[["SummonsNumber","ViolationPrecinct", "block_st","lon","lat"]]
locations = locations[~pd.isna(locations.lon) & ~pd.isna(locations.block_st)]
locations = locations[locations.lon < -1]
# if i had put more co.umns into the objects that merged into locations, i could probably have just used it as the primary dataframe to work with in this script
# but i'll leave it as-is for now, just to make less changes in this already massive set of changes i'll be making for a commit :p
if not grp_by_cols == 'block_st':
    from scipy.spatial import ConvexHull
    def hull_centroid(x):
        # x = locations[locations.ViolationPrecinct == 114]
        # x = locations[locations.ViolationPrecinct == 161]
        # xo = np.mean(x.unique())
        # return(xo)

        d = {}
        # d['a_sum'] = x['a'].sum()
        # d['a_max'] = x['a'].max()
        # d['b_mean'] = x['b'].mean()
        # d['c_d_prodsum'] = (x['c'] * x['d']).sum()
        # return pd.Series(d, index=['a_sum', 'a_max', 'b_mean', 'c_d_prodsum'])
        nx = len(x['lon'].unique())
        ny = len(x['lat'].unique())
        if (nx < 3) & (ny < 3):
            cx = np.mean(x['lon'])
            cy = np.mean(x['lat'])
        else:
            dat = np.array(x[['lon', 'lat']])
            hull = ConvexHull(dat)
            cx = np.mean(hull.points[hull.vertices, 0])
            cy = np.mean(hull.points[hull.vertices, 1])
        # plt.plot(hull.points[hull.vertices, 0], hull.points[hull.vertices, 1]);
        # plt.scatter(locations.lon, locations.lat, s=1);
        # plt.show()

        d['lon'] = cx
        d['lat'] = cy
        return pd.Series(d, index=['lon','lat'])


    # agg_dict = {'SummonsNumber':{'SummonsNumber':lambda x: x},'lon':{'lon':'af'}, 'lat':{'lat':'af'}}
    # test = locations.groupby(['ViolationPrecinct']).agg(agg_dict)
    locations2 = locations.groupby(['ViolationPrecinct']).apply(hull_centroid).reset_index() #.drop(columns='SummonsNumber')
else:
    locations2 = locations

# super slow to do this, i run out of memory ... it took about 9GB+
# plus this shouldn't really be the best way to do this ... the 2D KDE isn't necessary b/c the points are actually on a grid
# and i have and will have weights to use for each unique set of coordinates
# x = locations.lon
# y = locations.lat
# from scipy.stats import kde
# nbins=300
# k = kde.gaussian_kde([x,y])
# xi, yi = np.mgrid[x.min():x.max():nbins*1j, y.min():y.max():nbins*1j]
# zi = k(np.vstack([xi.flatten(), yi.flatten()]))
# plt.pcolormesh(xi, yi, zi.reshape(xi.shape))
# plt.show()




#%% Format Columns
pv17_from_sql['IssueDate'] = pd.to_datetime(pv17_from_sql['IssueDate'], format="%m/%d/%Y")


#%% Function to fix dates
def fix_dt(X):
    # start_date="2016-06-31", end_date="2017-06-30"
    # X is a pandas dataframe with a column for "IssueDate" and one for "ViolationTime"

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
pv17_from_sql.drop(columns=["IssueDate", "ViolationTime"], inplace=True)


#%%IMPORTANT: screen out dates that are out of range!!
# https://en.wikipedia.org/wiki/Fiscal_year#State_governments
start_date = pd.to_datetime("2016-06-30")
end_date = pd.to_datetime("2017-06-30")
pv17_from_sql_mask = (pv17_from_sql.datetime >= start_date) & (pv17_from_sql.datetime < end_date)
pv17_from_sql = pv17_from_sql[pv17_from_sql_mask].copy()


#%% Make round times, and aggregate
pv17_from_sql["datetime_rnd"] = pv17_from_sql["datetime"].dt.ceil(rnd_freq)  # map(lambda x: x.ceil(rnd_freq))
# pv17_grp = pv17_from_sql.groupby(grp_by_cols).size().reset_index(name="counts")
subColumns = ['SummonsNumber', "ViolationPrecinct", "ViolationCounty", "HouseNumber","StreetName","SubDivision","datetime_rnd"]
pv17_from_sql_locations = pd.merge(pv17_from_sql[subColumns], locations2, on=["ViolationPrecinct"], how='inner') #todo i shouldn't hardcode 'on' ... need to use groupby columns


grp_funcs = {'lon':{'lon':'mean'}, 'lat':{'lat':'mean', 'counts':'count'}}
pv17_grp = pv17_from_sql_locations.groupby(grp_by_cols).agg(grp_funcs)
# pv17_from_sql_locations.groupby(grp_by_cols).size().reset_index(name="counts")
pv17_grp.columns = pv17_grp.columns.get_level_values(1)
pv17_grp = pv17_grp.reset_index()


#%% Make basline predictions via aggregating to weekday-hour
# Get baseline prediction
# Will be based purely on long-term averages of hour-dayofweek combinations
# pv17_how_dour_long = pv17_grp.pivot_table(index=['hour','dow'], values='counts', aggfunc=np.mean)
# pv17_how_dour_wide = pv17_grp.pivot_table(index=['hour'], columns='dow', values='counts', aggfunc=np.mean)

# Make heat map of number of tickets per dow-hour
# import seaborn as sns
# sns.heatmap(pv17_how_dour_wide, cmap='RdYlGn_r', linewidths=0.5, annot=True)
# plt.show()


#%% Export 15-min intermediate table for time series analysis in R
# Subset to rows that correspond to precincts
df = pv17_grp[grp_by_cols + ['lon', 'lat', 'counts']] # going by precinct will take up much less when we go to regularize time seies

# if aggregating by something other than block, remember that lon and lat are defined per block
# therefore, you don't want to take a straight average of lon-lat per precinct, e.g.
# because it'll be weighted by where the most tickets are, not representing the average location of blocks in that precinct

# code below is to limit to precincts (e.g.) with many tickets (counts)
precinct_counts = df.ViolationPrecinct.value_counts()
use_precincts = precinct_counts[precinct_counts > 1000]
precinct_index = df.isin({'ViolationPrecinct': list(use_precincts.index)}).ViolationPrecinct
df = df[precinct_index]
# df['key'] = 0


# make `xout`, which will serve as a 'skeleton' df
# i.e., it will contain all the combinations of categories
# within which, each combination of these categories we will have a regular time series
# at this time, i am just planning on having a full regular time series for each precinct
# but in the future might want to include ViolationCounty if expanding beyond Manhattan
# round to 15 minutes
xout = pd.DataFrame({'val': 0}, index=pv17_grp["datetime_rnd"].unique()).sort_index()
xout = pd.DataFrame(xout.asfreq(rnd_freq))
# xout.index.name = "datetime_rnd"
# xout = xout.reset_index("datetime_rnd")
xout = xout.drop(columns='val')
xout.reset_index(inplace=True)
xout.rename(columns={'index':'datetime_rnd'}, inplace=True)
xout['key'] = 0
# example/ test merge, proof of principle for how i can extend this to the other spatial category columns
# say that 'pre' is a precinct, within which there could be several blocks, and 'dt' is a date-time combination
# you want each date-time to combine with all *existing* combinations of 'pre' and 'block':
# pd.merge(pd.DataFrame({'key':0, 'pre':[0,0,0,1,1,2], 'block':['a','b','b','c','c','d']}), pd.DataFrame({'key':0, 'dt':[12,13,14]}), on='key')
location_df = df.drop(columns=['datetime_rnd','counts']).drop_duplicates(subset=['ViolationCounty','ViolationPrecinct']).reset_index(drop=True) #pd.DataFrame({'ViolationPrecinct': list(use_precincts.index), 'key': 0}) # todo shouldn't ahrd code these columns
location_df['key'] = 0
xout = pd.merge(xout, location_df, on='key') # all combinations of dates and precincts
xout = xout.drop(columns='key')

# trim dates beyond just year
# start_date = pd.to_datetime("2016-06-30")
# end_date = pd.to_datetime("2017-6-30")
# df_mask = (df.datetime_rnd >= start_date) & (df.datetime_rnd < end_date)
# xout_mask = (xout.datetime_rnd >= start_date) & (xout.datetime_rnd < end_date)
# df = df[df_mask]
# xout = xout[xout_mask]

# df['value'] = df.groupby(['category', 'name'])['value'].transform(lambda x: x.fillna(x.mean())) # example of how to fill NA values with mean

# combine rounded ts series with the precinct-trimmed data set
# precinct_df = pd.DataFrame({'key':0, 'ViolationPrecinct': use_precincts.index})

pv = pd.merge(xout, df, on=['ViolationCounty', 'ViolationPrecinct', 'datetime_rnd', 'lon', 'lat'], how='left')# todo should not hardcode these columns, need to use group by columns
pv.loc[np.isnan(pv['counts']), 'counts'] = 0

# seasonality features
pv["dow"] = pv["datetime_rnd"].dt.dayofweek
pv["hour"] = pv["datetime_rnd"].dt.hour
pv["month"] = pv["datetime_rnd"].dt.month
pv["minute"] = pv["datetime_rnd"].dt.minute

# xout.ViolationPrecinct.value_counts()
# all(xout.datetime_rnd.value_counts()==22)
# min(xout.datetime_rnd.unique())
# max(xout.datetime_rnd.unique())
#
# min(pv17_grp.datetime_rnd.unique())
# max(pv17_grp.datetime_rnd.unique())
#
# min(df.datetime_rnd.unique())
# max(df.datetime_rnd.unique())
#
# pv.ViolationPrecinct.value_counts()
# all(pv.datetime_rnd.value_counts()==22)


pv.to_csv(proj_dir + "/data_int/pv.csv", index=False)

# sum(pv.counts>0)/len(pv.counts) # ~35% of the observations are greater than 0