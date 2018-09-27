#%% load libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import sklearn

# import keras
# from keras.models import Sequential
# from keras.layers.recurrent import LSTM
# from keras.layers.core import Dense, Activation, Dropout
from sklearn.preprocessing import MinMaxScaler
# from sklearn.metrics import mean_squared_error
# from sklearn.utils import shuffle


#%% Options
n_lag = 4
test_size_per = 24*14*4


#%% load data
proj_dir = "~/Documents/School&Work/Insight/parking"
data = pd.read_csv(proj_dir + "/data_int/pv.csv")
n_ts = len(data.ViolationPrecinct.unique())

#%% split into train and test
n_precincts = len(data.ViolationPrecinct.unique())
test_size = test_size_per*n_precincts
nrow = data.shape[0]
nrow_per = nrow/n_precincts
train_size_per = nrow_per - test_size_per
train_size = nrow - test_size
# train, test = data[0:train_size], data[train_size:nrow]

train = data.groupby('ViolationPrecinct').apply(lambda x: x.iloc[range(int(train_size_per)+1)])
train.reset_index(inplace=True, drop=True)
train = train[['ViolationPrecinct','counts','dow','hour']]
test = data.groupby('ViolationPrecinct').apply(lambda x: x.iloc[-test_size_per:])


#
baseline_table = train.drop(columns='ViolationPrecinct').pivot_table(index='hour', columns='dow', values='counts', aggfunc='mean')
baseline_df = train.drop(columns='ViolationPrecinct').groupby(['dow','hour']).agg('mean').reset_index()