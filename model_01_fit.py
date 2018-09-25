#%% load libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sklearn

import keras
from keras.models import Sequential
from keras.layers.recurrent import LSTM
from keras.layers.core import Dense, Activation, Dropout
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from sklearn.utils import shuffle


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




# %% add lagged counts as features
# define function for creatign embeddings for historical regression and forecasting
def embed(x, k=1, update=False):
    # if (update){
    #   tsVec_lag < - matrix(embed(tsVec0, lag_max+1)[, -1, drop=FALSE], nrow=1)  # [,-(lag_max+1)]
    #   dimnames(tsVec_lag) < - list(NULL, paste0('lag', 1:lag_max))
    # } else {
    #   tsVec_lag <- embed(tsVec0, lag_max+1)[,-1]
	#	dimnames(tsVec_lag) <- list(NULL, paste0('lag',1:lag_max))
    # }
    # > lag_design(1:4, 1:3, update=TRUE)
    #      y lag1 lag2 lag3
    # [1,] 4    3    2    1

    if(update):
        k = -k

    x = pd.Series(x)
    s = np.sign(k)
    kvec = list(range(0, np.abs(k) + 1))
    kvec.pop(0)
    kvec = list(np.multiply(kvec, s))

    if(update):
        x2 = x[:(np.abs(k)+1)]
        xo = pd.DataFrame({'y': x2})
        for i in kvec:
            xo['lag' + str(np.abs(i))] = pd.Series(x2).shift(-i)
    else:
        xo = pd.DataFrame({'y': x})
        for i in kvec:
            xo['lag' + str(np.abs(i))] = pd.Series(x).shift(i)

    xo = xo[xo.columns[::-1]].drop(columns='y')
    return xo#dropna().reset_index(drop=True)

# embed(list(range(4)), k=3, update=True) # for updating/ forecasting
# embed(list(range(10)), k=3, update=False)


# add lagged counts to training data
train_lag_preds = train.groupby("ViolationPrecinct").apply(lambda x: embed(x.counts, k=n_lag))
train = pd.concat([train, train_lag_preds], axis=1)
train.dropna(inplace=True)

# split training into target and features
def to_wide(x, col='ViolationPrecinct', values='counts'):
    n_ts = x[col].nunique()
    fake_vec = list(range(x.groupby(col).size()[0])) * n_ts
    x = x[[col, values]].reset_index(drop=True)
    x['fake'] = fake_vec
    xo = x.pivot(index='fake', columns=col, values=values)
    xo.reset_index(drop=True)
    return xo

train_y = np.array(to_wide(train))



#%% convert into formats suitable for model fitting
# train_in = train[train.ViolationPrecinct==0]
train_in = train.values.astype('float32')
scaler = MinMaxScaler(feature_range = (0, 1))
dataset = scaler.fit_transform(train_in)


#%% set up model architecture
# model = Sequential()
# model.add(LSTM(input_dim=1, output_dim=16,  return_sequences=True))

#%% vector autoregression
from statsmodels.tsa.api import VAR, DynamicVAR
model = VAR(train_y)
results = model.fit(maxlags=24*4*1, ic='aic')
results.summary()
# mod_sel = model.select_order(15)
lag_order = results.k_ar
mod_fore = results.forecast(pd.DataFrame(train_y).values[-lag_order:], test_size)

#%% vector autoregression with exogenous input
# http://www.statsmodels.org/devel/generated/statsmodels.tsa.statespace.varmax.VARMAX.html#statsmodels.tsa.statespace.varmax.VARMAX
# http://www.statsmodels.org/devel/examples/notebooks/generated/statespace_varmax.html
# Their example, from the second link, below:
# dta = sm.datasets.webuse('lutkepohl2', 'https://www.stata-press.com/data/r12/')
# dta.index = dta.qtr
# endog = dta.loc['1960-04-01':'1978-10-01', ['dln_inv', 'dln_inc', 'dln_consump']]
# exog = endog['dln_consump']
# mod = sm.tsa.VARMAX(endog[['dln_inv', 'dln_inc']], order=(2,0), trend='nc', exog=exog)
# res = mod.fit(maxiter=1000, disp=False)
# print(res.summary())

