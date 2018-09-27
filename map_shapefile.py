import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# import data used in analysis
# get precincts analyzed
proj_dir = "~/Documents/School&Work/Insight/parking"
data = pd.read_csv(proj_dir + "/data_int/pv.csv")
precs = data.ViolationPrecinct.unique()
tickets_per_precs = data.groupby("ViolationPrecinct").counts.agg('mean').reset_index()

# read in shape files
# subset to analyzed preincts
shp_path = "/Users/battrd/Documents/School&Work/Insight/parking/data/PolicePrecincts/geo_export_210823e9-ae0b-4030-807e-35d84173eb87.shp"
map_df = gpd.read_file(shp_path)
map_df_sub = map_df[map_df.precinct.isin(precs)]
map_df_sub.plot()  # ; plt.show()


# combine average tickets per precinct with shape files

map_df_merge = pd.merge(map_df_sub, tickets_per_precs, how='left', left_on='precinct', right_on='ViolationPrecinct')


fig, ax = plt.subplots(1, figsize=(3, 6))
map_df_merge.plot(column='counts', cmap='jet', legend=True)

# below changes whitespace within the plot
# plot_margin = 0.25
# x0, x1, y0, y1 = plt.axis()
# plt.axis((x0 - plot_margin,
#           x1 + plot_margin,
#           y0 - plot_margin,
#           y1 + plot_margin))

fig.tight_layout()


# plt.subplots_adjust(hspace=0)
# plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
# plt.bbox_inches('tight')
# plt.savefig()

plt.savefig('/Users/battrd/Desktop/test.png', bbox_inches='tight')

# plt.show()