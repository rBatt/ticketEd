library(data.table)
library(forecast)

proj_dir <- "~/Documents/School&Work/Insight/parking"
file_dir <- "/data_int/pv17_grp.csv"

file <- paste0(proj_dir, file_dir)

dat0 <- fread(file)


# ==========
# = Subset =
# ==========
dat0[,datetime_rnd:=as.POSIXct(datetime_rnd)]
dat <- dat0[datetime_rnd>=as.POSIXct("2016-06-30") & datetime_rnd<=as.POSIXct("2017-6-30")]
dat <- dat[order(datetime_rnd)]
dat[,dow:=as.integer(dow)]
dat[,hour:=as.integer(hour)]
dat[,month:=as.integer(month)]
dat[,minute:=as.integer(minute)]


# ==========================
# = Regularize Time Series =
# ==========================
reg_datetime_seq <- seq(from=dat[,min(datetime_rnd)], to=dat[,max(datetime_rnd)], by="15 min")
reg_dt <- data.table(CJ(datetime_rnd=reg_datetime_seq, ViolationCounty=dat[,unique(ViolationCounty)], SubDivision=dat[,unique(SubDivision)]))
reg_dt[,c("dow","hour","month","minute"):=list(dow=as.integer(format.Date(datetime_rnd, format="%u"))-1, hour=hour(datetime_rnd), month=month(datetime_rnd), minute=minute(datetime_rnd))]

on_cols <- c("datetime_rnd", "ViolationCounty", "SubDivision", "dow", 'hour', "month", 'minute')
dat2 <- merge(dat, reg_dt, by=on_cols, all=TRUE)
dat2[is.na(counts), counts:=0]


# =====================================================
# = Select a Random SubDivision, Plot the Time Series =
# =====================================================
arbSD <- "C8" # there are 138 subdivisions
egDat <- dat2[SubDivision==arbSD]
egDat[,plot(datetime_rnd, counts, type='l')]
# egDat[,hist(counts)]
# egDat[,table(pmin(1, counts))] # ~20% of observations are > 0


# ============================
# = Try tscount to Fit Model =
# ============================
library(tscount)
nLag <- 2
tsVec <- egDat[,counts]
mod1 <- tscount::tsglm(tsVec, model=list(past_obs=nLag, past_mean=1)) # kinda slow, but not super bad.
# AIC = 46446.16
# cor(egDat[,counts], fitted(mod1)) # 0.4288084
# doesn't appear to be reporting all of the coefficients ... reports an intercept, beta_2, and alpha_1.
# the beta are the autogressive, and the alpha are the moving average
# it could be that it skips beta_1 and considers beta_1 to be fixed at 1 for lag0 ... but that'd be kinda silly
# the equations in the paper and help page seem to indicate that beta_1 should be the AR(1) coef, so yeah, it appears to be missing


# ==================
# = Try just a GLM =
# ==================
nLag <- 2
tsVec0 <- egDat[,counts]
tsVec <- tail(tsVec0, -nLag)
tsVec_lag <- embed(tsVec0, nLag+1)[,-(nLag+1)]
dimnames(tsVec_lag) <- list(NULL, paste0('lag',nLag:1))
mod2_X <- cbind(y=tsVec, tsVec_lag)
mod2 <- glm(y~lag1+lag2, family=poisson, data=as.data.frame(mod2_X))
# AIC = 37343
# cor(tsVec, fitted(mod2)) # 0.2753909


nLag <- 12
tsVec0 <- egDat[,counts]
tsVec <- tail(tsVec0, -nLag)
tsVec_lag <- embed(tsVec0, nLag+1)[,-(nLag+1)]
dimnames(tsVec_lag) <- list(NULL, paste0('lag',nLag:1))
mod3_X <- cbind(y=tsVec, tsVec_lag)
mod3 <- glm(y~., family=poisson, data=as.data.frame(mod3_X))
# AIC = 35461
# cor(tsVec, fitted(mod3)) # 0.3381322


nLag <- 672 # 672 is number of 15-min observations in 1 week
tsVec0 <- egDat[,counts]
tsVec <- tail(tsVec0, -nLag)
tsVec_lag <- embed(tsVec0, nLag+1)[,-(nLag+1)]
dimnames(tsVec_lag) <- list(NULL, paste0('lag',nLag:1))
mod4_X <- cbind(y=tsVec, tsVec_lag)
mod4 <- glm(y~., family=poisson, data=as.data.frame(mod4_X)) # pretty slow; took about 5-10 min
# AIC = 29166
# cor(tsVec, fitted(mod4)) # 0.7437571


nLag <- 673 # 672 is number of 15-min observations in 1 week
tsVec0 <- egDat[,counts]
tsVec <- tail(tsVec0, -nLag)
tsVec_lag <- embed(tsVec0, nLag+1)[,-(nLag+1)]
dimnames(tsVec_lag) <- list(NULL, paste0('lag',nLag:1))
mod5_X0 <- cbind(y=tsVec, tsVec_lag)
use_lags <- c(
	1, 2, 3, 4, # lags in the past hour
	4*24-1, 4*24, 4*24+1, # lags for this time of day yesterday
	4*24*7-1, 4*24*7, 4*24*7+1 # lags for this time of day (on this day of week) last week
)
use_lags_names <- paste0("lag",use_lags)
mod5_X <- mod5_X0[, c("y",use_lags_names)]
mod5 <- glm(y~., family=poisson, data=as.data.frame(mod5_X)) # fast, just a couple seconds
# AIC = 34903
# cor(tsVec, fitted(mod5)) # 0.3492863


nLag <- 673 # 672 is number of 15-min observations in 1 week
tsVec0 <- egDat[,counts]
tsVec <- tail(tsVec0, -nLag)
tsVec_lag <- embed(tsVec0, nLag+1)[,-(nLag+1)]
dimnames(tsVec_lag) <- list(NULL, paste0('lag',nLag:1))
mod6_X0 <- cbind(y=tsVec, tsVec_lag)
use_lags <- c(
	1, 2, 3, 4, # lags in the past hour
	4*24-1, 4*24, 4*24+1, # lags for this time of day yesterday
	4*24*7-1, 4*24*7, 4*24*7+1 # lags for this time of day (on this day of week) last week
)
use_lags_names <- paste0("lag",use_lags)
mod6_X <- mod6_X0[, c("y",use_lags_names)]
time_indices <- tail(egDat[,list(minute, hour, dow, month)],-nLag)
time_indices <- sapply(time_indices, function(x)as.factor(x))
mod6_X <- data.frame(mod6_X, time_indices)
mod6 <- glm(y~., family=poisson, data=mod6_X) # pretty fast, maybe 5 seconds
# AIC = 28959
# cor(tsVec, fitted(mod6)) # 0.4591734

# ===========
# = Summary =
# ===========
#' There is definitely some autocorrelation. It shows up at short time scales, as well as longer seasonal scales. For a full data set, lots of arbitrary time lags pop as significant predictors. The relevance of these lags is hard to interpret mechanistically, but they can massively improve the correlation between observations and prediction, and produce competitive AIC. Because they don't result in a proportionally increased AIC, I'm assuming that the improvement in cor(predicted, observed) can be attributed, mainly, to the large increase in feature number (using 672+ features, instead of ~15).  
#' 
#' For example, the model with all lags for the past week (15 min data) had an AIC of 30693 and an r=0.7408245. The last model I tried had 15 features -- including time lags for the past hour, yesterday at same time, and last week at same time -- and had an AIC of 29112 and an r = 0.4520971. The latter model also had features for day of week, hour of day, (quarter-)minute of hour, and day of week. These factors were fairly predictive, with the exception that the weekdays weren't as relevant as the weekend factors (weekdays are very similar to each other, whereas saturdays are a bit lower [coeff = -0.146256] and sundays are much lower [coeff = -0.412691])
#' 
#' 


