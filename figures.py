
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# import os; os.chdir("/Users/Battrd/Documents/School&Work/Insight/parking")

# show more columns in printout
desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',65)


#%% data options
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


#%% Create data frame of time series: observed, baseline, glm
# baseline forecasts
base_test_df = pd.read_csv("./data_int/model_00_baseline.csv")
base_test_df.rename(columns={'counts_hat':'counts_hat_base'}, inplace=True)
base_test_df.datetime_rnd = pd.to_datetime(base_test_df.datetime_rnd)

# glm forecasts
glm_test_df = pd.read_csv("./data_int/mvp_f_out2.csv")
glm_test_df.rename(columns={'counts_hat':'counts_hat_glm'}, inplace=True)
glm_test_df.datetime_rnd = pd.to_datetime(glm_test_df.datetime_rnd)

# both forecasts and obs
fore_obs_df = pd.merge(base_test_df, glm_test_df, on=['datetime_rnd','ViolationPrecinct','counts'], how='outer')


#%% Plot Fits and Residuals
# set up plots
fig, ax = plt.subplots(2,2) # 2x2 grid
sval = 1 # point size
alpha_val = 0.5 # transparency of points

# plot baseline vs observed
plt.subplot(2,2,1)
plt.scatter(fore_obs_df.counts, fore_obs_df.counts_hat_base, s=sval, alpha=alpha_val)

# plot glm vs observed
plt.subplot(2,2,2)
plt.scatter(fore_obs_df.counts, fore_obs_df.counts_hat_glm, s=sval, alpha=alpha_val)

# plot glm vs baseline
plt.subplot(2,2,3)
plt.scatter(fore_obs_df.counts_hat_base, fore_obs_df.counts_hat_glm, s=sval, alpha=alpha_val)

# plot residual time series on top of each other
ax = plt.subplot(2, 2, 4)
for i in fore_obs_df['ViolationPrecinct'].unique():
    td = fore_obs_df[fore_obs_df.ViolationPrecinct == i]
    t_glm_error = td.counts_hat_glm - td.counts
    t_dtrnd = td['datetime_rnd'].values
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))
    plt.plot(td.datetime_rnd, t_glm_error)
# plt.plot(fore_obs_df.datetime_rnd, glm_error)

plt.show()


#%% Time Series full: Manhattan daily
daily_tickets = data.groupby(data.datetime_rnd.dt.date)['counts'].sum()
# daily_tickets.index = pd.DatetimeIndex(daily_tickets.index)
# day_dict = {'weekday':[0,1,2,3,4,5],'sat':[6], 'sun':[7]}
day_dict = {0:'weekday', 1:'weekday', 2:'weekday', 3:'weekday', 4:'weekday', 5:'sat', 6:'sun'}
daily_tickets_dayType = pd.Series(pd.to_datetime(daily_tickets.index).dayofweek).replace(day_dict)

fig = plt.figure(facecolor='white', figsize=[6,4]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d\n%Y'))
plt.plot(daily_tickets.index, daily_tickets, c='black', linewidth=0.5)
for i in daily_tickets_dayType.unique():
    t_index = list(daily_tickets_dayType == i)
    t_date = daily_tickets.index[t_index]
    t_tick = daily_tickets[list(t_index)]
    plt.scatter(t_date, t_tick)
plt.title("Manhattan FY 2017")
plt.ylabel('Tickets per Day')
plt.savefig("./figures/dailyTimeSeries_Manhattan.png", bbox_inches='tight', dpi=200)
# plt.show()


#%%Options for example Time Series
vio_prec_eg = 13
fore_obs_df_eg = fore_obs_df[fore_obs_df.ViolationPrecinct==vio_prec_eg]


#%% Time series test: observed
fig = plt.figure(facecolor='white', figsize=[6,4]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts, c='black', linewidth=0.5)
plt.title('End of FY 2017, Precinct ' + str(vio_prec_eg))
plt.ylabel('Tickets per 15 min')
plt.savefig("./figures/forecast_obs.png", bbox_inches='tight', dpi=200)
# plt.show()


#%% Time series test: observed, baseline model
fig = plt.figure(facecolor='white', figsize=[6,4]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts, c='black', linewidth=0.5)
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts_hat_base, c='blue', linewidth=1.5)
plt.title('End of FY 2017, Precinct ' + str(vio_prec_eg))
plt.ylabel('Tickets per 15 min')
# plt.legend(labels=['Observed','Baseline'])
plt.savefig("./figures/forecast_obs_baseline.png", bbox_inches='tight', dpi=200)
# plt.show()


#%% Time series test: observed, glm
fig = plt.figure(facecolor='white', figsize=[6,4]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts, c='black', linewidth=0.5)
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts_hat_glm, c='red', linewidth=1.5)
plt.title('End of FY 2017, Precinct ' + str(vio_prec_eg))
plt.ylabel('Tickets per 15 min')
# plt.legend(labels=['Observed','GLM'])
plt.savefig("./figures/forecast_obs_glm.png", bbox_inches='tight', dpi=200)
# plt.show()


#%% Time series test: observed, baseline, glm
fig = plt.figure(facecolor='white', figsize=[6,4]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%a\n%b %d'))
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts, c='black', linewidth=0.5)
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts_hat_base, c='blue', linewidth=1.5)
plt.plot(fore_obs_df_eg.datetime_rnd, fore_obs_df_eg.counts_hat_glm, c='red', linewidth=1.5)
plt.title('End of FY 2017, Precinct ' + str(vio_prec_eg))
plt.ylabel('Tickets per 15 min')
# plt.legend(labels=['Observed','Baseline','GLM'])
plt.savefig("./figures/forecast_obs_baseline_glm.png", bbox_inches='tight', dpi=200)
# plt.show()


#%% Average DoW-HoD combinations, in table for plotting
# baseline averages, but in table for plotting convenience
baseline_table = train.drop(columns='ViolationPrecinct').pivot_table(index='hour', columns='dow', values='counts', aggfunc='mean')

# plot heat map day-time averages
fig = plt.figure(facecolor='white', figsize=[3,5]) # uses matplotlib.pyplot imported as plt
ax = fig.add_subplot(111)
xticklab = ('Mon',"Tue",'Wed','Thu','Fri','Sat','Sun')
sns.heatmap(baseline_table, annot=False, cmap='RdYlGn_r', linewidths=0.5, yticklabels=2, xticklabels=xticklab, fmt=".1f")
plt.ylabel("Hour of Day")
plt.xlabel("Day of Week")
plt.title("Tickets per 15 min")
plt.savefig("./figures/ticketHeatmap.png", bbox_inches='tight', dpi=250)
# plt.show()


#%% How forecast error as a function of horizon
u_prec = fore_obs_df.ViolationPrecinct.unique()
eg_datetime = fore_obs_df[fore_obs_df.ViolationPrecinct == i].datetime_rnd
horizon = (eg_datetime - pd.to_datetime("2017-06-20 00:00:00")).dt.total_seconds()/60/60/24
fore_errors_glm = np.zeros((len(horizon), len(u_prec)))
for i in range(len(u_prec)):
    td = fore_obs_df[fore_obs_df.ViolationPrecinct == u_prec[i]]
    fore_errors_glm[:, i] = np.array(td.counts - td.counts_hat_glm)


def mae(x):
    return np.mean(np.abs(x))


blah = np.apply_along_axis(mae, 1, fore_errors_glm)
plt.plot(horizon, blah)
# plt.scatter(horizon, blah)
plt.show()


# import seaborn as sns
# sns.heatmap(fore_errors_glm)
# plt.show()