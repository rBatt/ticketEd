

#%% load libraries
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# show more columns in printout
desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)

# import os; os.chdir("/Users/Battrd/Documents/School&Work/Insight/parking")

#%% Options
test_size_per = 24*7*4  # number of observations per precinct; (hours/day)*(days)*(obs/hour)
date_end = pd.to_datetime("2017-06-27")  # used to truncate data set


#%% load data
proj_dir = "~/Documents/School&Work/Insight/parking"
data = pd.read_csv(proj_dir + "/data_int/pv.csv")
n_ts = len(data.ViolationPrecinct.unique())
data["datetime_rnd"] = pd.to_datetime(data.datetime_rnd)
data = data[data.datetime_rnd <= date_end].reset_index(drop=True)


#%% split into train and test
n_precincts = len(data.ViolationPrecinct.unique())
test_size = test_size_per*n_precincts
nrow = data.shape[0]
nrow_per = nrow/n_precincts
train_size_per = nrow_per - test_size_per  # number of observations per precinct in training set
train_size = nrow - test_size

train = data.groupby('ViolationPrecinct').apply(lambda x: x.iloc[range(int(train_size_per)+1)])
train.reset_index(inplace=True, drop=True)
train = train[['ViolationPrecinct','counts','dow','hour']]
test = data.groupby('ViolationPrecinct').apply(lambda x: x.iloc[-test_size_per:])


#%%Forecast from Baseline Model (avg DoW-HoD combos)
# average parking tickets for each hour-of-day x day-of-week combination
# baseline_df useful for statistics
baseline_df = train.drop(columns='ViolationPrecinct').groupby(['dow','hour']).agg('mean').reset_index()
baseline_df.rename({'counts':'counts_hat'}, axis='columns', inplace=True)
base_test_df = pd.merge(test, baseline_df, how='left')
# base_test_df['datetime_rnd'] = pd.to_datetime(base_test_df.datetime_rnd)

# correction between baseline and and observed in test
baseline_corr = np.corrcoef(np.array(base_test_df['counts']), np.array(base_test_df['counts_hat']))[1,0]

#%% Save baseline model to csv
base_test_df.to_csv("./data_int/model_00_baseline.csv", index=False)


#%% Plot time series of observed test and baseline predictions for test dates
# plot 2 time series:
# 1 of true time series (test set)
# 1 of baseline forecast
eg1 = base_test_df[base_test_df.ViolationPrecinct == 1].reset_index(drop=True)
fig, ax = plt.subplots(figsize=[6,4])
ax.plot(eg1.datetime_rnd, eg1.counts, c='black', linewidth=0.5)
ax.plot(eg1.datetime_rnd, eg1.counts_hat, c='royalblue')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))  # doesn't seem to do anything
plt.ylabel('Tickets per 15 min')
plt.title("Tickets in Precinct 1, FY 2017")
plt.show()


# #%% Average DoW-HoD combinations, in table for plotting
# # baseline averages, but in table for plotting convenience
# baseline_table = train.drop(columns='ViolationPrecinct').pivot_table(index='hour', columns='dow', values='counts', aggfunc='mean')
#
# # plot heat map day-time averages
# xticklab = ('Mon',"Tue",'Wed','Thu','Fri','Sat','Sun')
# sns.heatmap(baseline_table, annot=True, cmap='RdYlGn_r', linewidths=0.5, yticklabels=2, xticklabels=xticklab, fmt=".1f")
# plt.ylabel("Hour of Day")
# plt.xlabel("Day of Week")
# plt.title("Tickets per 15 min")
# plt.savefig("./figures/ticketHeatmap.png", bbox_inches='tight')
# # plt.show()