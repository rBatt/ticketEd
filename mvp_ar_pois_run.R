

library(data.table)
library(fasttime)


proj_dir <- "~/Documents/School&Work/Insight/parking"
# dat2 <- fread(file.path(proj_dir, "data_int", "mvp_ts_prep.csv")) # don't do this b/c converting datetime back to date class is pretty close
# load(file.path(proj_dir, "data_int", "mvp_ts_prep.RData")) # loads object "dat2"
cc <- c('datetime_rnd'='character', 'ViolationCounty'='character', 'ViolationPrecinct'='character', 'lon'='numeric', 'lat'='numeric', 'counts'='integer', 'dow'='integer', 'hour'='integer', 'month'='integer', 'minute'='integer')
dat2 <- fread(file.path(proj_dir, "data_int", "pv.csv"), colClasses=cc)
dat2[,datetime_rnd:=fasttime::fastPOSIXct(datetime_rnd)]


# ==================================================
# = Load Functions for GLM Fitting and Forecasting =
# ==================================================
source(file.path(proj_dir, "mvp_ar_pois.R"))


# ====================
# = Forecast Options =
# ====================
nMin <- 15 # how many minutes between time steps?
forecastHorizon <- 24*7*(60/nMin) # how many time steps ahead of most recent observation should the forecast go?
n_test <- forecastHorizon # how many time steps should be reserved for testing at end of time series? if you want to match forecast for the test set, forecastHorizon and n_test should be the same
time_preds <- c("hour", "dow") #, "minute", "month")

use_lags <- c(
	1, 2, 3, 4, # lags in the past hour
	4*24-1, 4*24, 4*24+1, # lags for this time of day yesterday
	4*24*7-1, 4*24*7, 4*24*7+1 # lags for this time of day (on this day of week) last week
)
# use_lags <- c(
# 	1, 2, 3, 4, # lags in the past hour
# 	4*24, # lags for this time of day yesterday
# 	4*24*7 # lags for this time of day (on this day of week) last week
# )
# use_lags <- 1:4


# ================
# = Example Data =
# ================
load(file.path(proj_dir, "/data_int/egDat.RData"))
# f_out_eg <- egDat[datetime_rnd<="2017-06-27",j={
# 	out <- ar_pois(.SD, nHold=n_test, nAhead=forecastHorizon, nMin=nMin, time_preds=time_preds)
# 	out
# }, by=c("ViolationCounty", "ViolationPrecinct")]


# =================
# = example/ test =
# =================
# test_dat <- egDat[datetime_rnd<"2017-06-27"] #dat2[ViolationPrecinct=="1" & datetime_rnd<="2017-06-27"]
# test <- ar_pois(dat=test_dat, lags=1:4, time_preds=c("dow","hour"))
# # png("~/Desktop/mvp_test_forecast.png", res=150, units='in', width=6, height=6)
# test[,plot(datetime_rnd, counts, col='black', type='l', ylim=range(c(counts, counts_hat)), ylab="Tickets per 15 min", xlab="Date")]
# test[,lines(datetime_rnd, counts_hat, col='red')]
# legend("topleft", col=c("black", "red"), lty=1, legend=c("Observed Tickets", "Forecasted Tickets"))
# # dev.off()
# dev.new(); test[,plot(counts, counts_hat)]
# test[,cor(counts, counts_hat)]


# =============
# = Trim Down =
# =============
dat3 <- dat2[,list(datetime_rnd, ViolationPrecinct, counts, dow, hour)]


# =======
# = wtf =
# =======
# ar_pois(dat3[datetime_rnd<="2017-06-27" & ViolationPrecinct==1], nHold=n_test, nAhead=forecastHorizon, nMin=nMin, time_preds=time_preds, lags=1:4)
# ar_pois(dat3[datetime_rnd<="2017-06-27" & ViolationPrecinct==1], nHold=n_test, nAhead=forecastHorizon, nMin=nMin, time_preds=time_preds, lags=1:4)
# test <- ar_pois(dat=test_dat, lags=1:4, time_preds=c("dow","hour")) # pass
# test <- ar_pois(dat=dat3[datetime_rnd<="2017-06-27" & ViolationPrecinct==1], lags=1:4, time_preds=c("dow","hour")) # fail

# test2 <- dat3[datetime_rnd<"2017-06-27" & ViolationPrecinct==1]
# test <- ar_pois(dat=test2, lags=1:4, time_preds=c("dow","hour")) # the problem was a datetime format ... this was blowing up (presumably on a merge) when the datetime_rnd was a character instead of POSIX


# ===============
# = Do Forecast =
# ===============
f_out <- dat3[datetime_rnd<="2017-06-27",j={
	out <- ar_pois(.SD, nHold=n_test, nAhead=forecastHorizon, nMin=nMin, time_preds=time_preds, lags=use_lags)
	out
}, by=c("ViolationPrecinct")]



# save(f_out, file="~/Desktop/f_out.RData")

f_out[!is.finite(counts_hat), counts_hat:=NaN]
f_out[,datetime_rnd:=as.character(datetime_rnd)]
fwrite(f_out, file=file.path(proj_dir, "data_int", "mvp_f_out2.csv"))

# f_out[,cor(counts, counts_hat), by='ViolationPrecinct'][,hist(V1)]
