#%% load libraries
import geopandas as gpd
import pandas as pd


# show more columns in printout
desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)

# import os; os.chdir("/Users/Battrd/Documents/School&Work/Insight/parking")


#%% read street length shapefile
cscl = gpd.read_file("./NYC Street Centerline (CSCL)")
# cscl_Series = gpd.GeoSeries(cscl.geometry)


#%%read in precinct shapefile
prec = gpd.read_file("./app_mvp/PolicePrecincts")
# prec_Series = gpd.GeoSeries(prec.geometry)


#%% associate each street with a precinct
cscl_prec = gpd.sjoin(cscl, prec, how='inner', op='within')
cscl_prec.groupby("precinct").shape_leng_left.agg('sum')
street_leng_per_prec = cscl_prec.groupby("precinct").shape_leng_left.agg('sum').reset_index()
street_leng_per_prec.rename(columns={'shape_leng_left':'street_leng'}, inplace=True)


#%% export data set
street_leng_per_prec.to_csv("./app_mvp/street_leng_per_prec.csv", index=False)