

library(data.table)


proj_dir <- "~/Documents/School&Work/Insight/parking"
# dat2 <- fread(file.path(proj_dir, "data_int", "mvp_ts_prep.csv")) # don't do this b/c converting datetime back to date class is pretty close
load(file.path(proj_dir, "data_int", "mvp_ts_prep.RData")) # loads object "dat2"


# ==================================================
# = Load Functions for GLM Fitting and Forecasting =
# ==================================================
source(file.path(proj_dir, "mvp_ar_pois.R"))


# =================
# = example/ test =
# =================
# test_dat <- dat2[SubDivision=="C8" & datetime_rnd<="2017-06-27"]
# test <- ar_pois(test_dat)
# png("~/Desktop/mvp_test_forecast.png", res=150, units='in', width=6, height=6)
# test[,plot(datetime_rnd, counts, col='black', type='l', ylim=range(c(counts, counts_hat)), ylab="Tickets per 15 min", xlab="Date")]
# test[,lines(datetime_rnd, counts_hat, col='red')]
# legend("topleft", col=c("black", "red"), lty=1, legend=c("Observed Tickets", "Forecasted Tickets"))
# dev.off()
# dev.new(); test[,plot(counts, counts_hat)]
# test[,cor(counts, counts_hat)]


# ====================
# = Forecast Options =
# ====================
nMin <- 15 # how many minutes between time steps?
forecastHorizon <- 24*7*nMin # how many time steps ahead of most recent observation should the forecast go?
n_test <- forecastHorizon # how many time steps should be reserved for testing at end of time series? if you want to match forecast for the test set, forecastHorizon and n_test should be the same
time_preds <- c("minute", "hour", "dow") #, "month")

# ===============
# = Do Forecast =
# ===============
f_out <- dat2[datetime_rnd<="2017-06-27",j={
	out <- ar_pois(.SD, nHold=n_test, nAhead=forecastHorizon, nMin=nMin, time_preds=time_preds)
	out
}, by=c("ViolationCounty", "SubDivision")]



# save(f_out, file="~/Desktop/f_out.RData")

f_out[!is.finite(counts_hat), counts_hat:=NaN]
f_out[,datetime_rnd:=as.character(datetime_rnd)]
fwrite(f_out, file=file.path(proj_dir, "data_int", "mvp_f_out.csv"))

