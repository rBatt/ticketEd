import pickle
import pandas as pd


# load original data and its test/train
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





# load VAR forecast
mod_fore_file_name = ('/Users/Battrd/Documents/School&Work/Insight/parking' + '/data_int/model_01_VAR_mod_fore.pkl')
mod01_fore_pkl = open(mod_fore_file_name, 'rb')
mod01_fore = pickle.load(mod01_fore_pkl)
mod01_fore_pkl.close()

mod01_results = pd.DataFrame(mod01_fore)



# load VAR full results
results_file_name = ('/Users/battrd/Documents/School&Work/Insight/parking' + '/data_int/model_01_VAR_results.pkl')
mod01_results_pkl = open(results_file_name, 'rb')
mod01_results = pickle.load(mod01_results_pkl)
mod01_results_pkl.close()

results_coefs_file_name = ('/Users/battrd/Documents/School&Work/Insight/parking' + '/data_int/model_01_VAR_results_coefs.pkl')



# combine forecast with test set
