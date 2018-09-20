
# Need some functions to fit a Poisson-family GLM.
# Some of the predictor variables will be lagged versions of the 

# ar_pois
# level 0
# some sort of wrapper function, not sure of exact linkage to functions below
ar_pois <- function(dat, holdType=c("end"), nHold=672, nAhead=24*7*(60/nMin), nMin=15, date_col="datetime_rnd", resp_col="counts", time_preds=c("minute", "hour", "dow", "month")){
	time_preds <- match.arg(time_preds, several.ok=TRUE)
	
	train_ind <- ar_pois_hold(dat, holdType, nHold, returnType="ind")
	dat_train <- dat[train_ind]
	dat_test <- dat[!train_ind]
	
	dt <- dat_train[,get(date_col)]
	y <- dat_train[,get(resp_col)]
	train_mod <- ar_pois_fit(y, xreg=time_factors(dt, types=time_preds))
	
	dat_forecast <- ar_pois_forecast(train_mod, nAhead=nAhead, nMin=nMin)
	names_df <- names(dat_forecast)
	if("minute"%in%names_df){dat_forecast[,minute:=as.integer(as.character(minute))]}
	if("hour"%in%names_df){dat_forecast[,hour:=as.integer(as.character(hour))]}
	if("dow"%in%names_df){dat_forecast[,dow:=as.integer(as.character(dow))]}
	if("month"%in%names_df){dat_forecast[,month:=as.integer(as.character(month))]}
	
	dat_out <- merge(dat_test, dat_forecast, by=c('datetime_rnd', 'minute', 'hour', 'dow', 'month'), all=TRUE)
	
	return(dat_out)
}
# test_dat <- dat2[SubDivision=="C8" & datetime_rnd<="2017-06-27"]
# test <- ar_pois(test_dat)
# test[,plot(datetime_rnd, counts, col='black', type='l', ylim=range(c(counts, counts_hat)))]
# test[,lines(datetime_rnd, counts_hat, col='red')]
# dev.new(); test[,plot(counts, counts_hat)]
# test[,cor(counts, counts_hat)]

# ar_pois_hold
# level 1a
# use date range to hold out some elements
# could also use date pattern (every n-th week)
# no, don't do pattern, because pain in the ass with lagged predictors!
# if you want more hold out/ test data than can be achieved by the forecast horizon, need to do rolling or extending windows ==> ref old convo with donald
ar_pois_hold <- function(dat, holdType=c("end"), nHold=672, returnType=c("list","ind")){
	holdType <- match.arg(holdType)
	if(holdType=="end"){
		if(returnType=="ind"){
			train_ind <- rep(TRUE, nrow(dat))
			lt <- length(train_ind)
			train_ind[lt:(lt-nHold+1)] <- FALSE
			return(train_ind)
		}else{
			dat_train <- head(dat, -nHold)
			dat_test <- tail(dat, nHold)
			return(list(train=dat_train, test=dat_test))
		}
	}
}


# ar_pois_fit
# level 1b
# call GLM to fit model to training data
# as input, needs data already sub
# as output, will give model object
ar_pois_fit <- function(y, xreg=NULL, lags=c(1,2,3,4, (24*(60/nMin))+c(-1,0,1), (7*24*(60/nMin))+c(-1,0,1)), nMin=15){
	X0 <- lag_design(y=y, lags=lags)
	if(is.null(xreg)){
		X <- as.data.frame(X0)
	}else{
		stopifnot(any(class(xreg)%in%c("data.table","data.frame", "matrix")))
		X <- as.data.frame(cbind(X0, xreg))
	}
	
	mod <- glm(y~., data=X, family=poisson)
	
	return(mod)
}

# tsVec <- egDat[,counts]
# dt <- egDat[,datetime_rnd]
# test_mod <- ar_pois_fit(tsVec, xreg=time_factors(dt))
# summary(test_mod) # AIC = 28955
# cor(fitted(test_mod), tail(tsVec, -673)) # 0.4608277


# ar_pois_forecast
# level 1c
# as input, will accept a fitted model and desired forecast horizon
# will call update function

ar_pois_forecast <- function(mod, nAhead=24*7*(60/nMin), nMin=15, time_preds=c("minute", "hour", "dow", "month")){
	time_preds <- match.arg(time_preds, several.ok=TRUE)
	
	dat <- mod$data
	dt <- dat[,"datetime"]
	y <- dat[,"y"]
	
	# the time indices predictors can be made all at once for the
	# entire forecast horizon, because it only depends on the time index/ datetime
	dt_ahead <- seq(from=tail(dt, 1), by=nMin*60, length.out=nAhead+1)#[-1]
	tf_ahead <- time_factors(dt_ahead, lag_max=1, types=time_preds)
	
	# the predictors that are lagged versions of the response can only be produced for 1 time step ahead
	def_lags <- c(1,2,3,4, (24*(60/nMin))+c(-1,0,1), (7*24*(60/nMin))+c(-1,0,1))
	# y <- c(1:max(def_lags), NA)
	
	
	# loop through
	# update state vector at each time step with new prediction
	pred_vec <- rep(NA, nAhead)
	y_vec <- y

	for(i in 1:nAhead){
		y_vec <- tail(y_vec, max(def_lags))
		y_vec <- c(y_vec,NA)
		
		X_lag <- lag_design(y=y_vec, lags=def_lags, update=TRUE)
		X <- cbind(X_lag, tf_ahead[i])
		X[,y:=NULL]
		y_pred_t1 <- predict(mod, newdata=X, type='response')
		
		pred_vec[i] <- y_pred_t1
		y_vec[length(y_vec)] <- y_pred_t1
	}
	
	dt_out <- cbind(tf_ahead, counts_hat=pred_vec)
	# dt_out[,plot(datetime, count_hat, type='l')]
	setnames(dt_out, "datetime", "datetime_rnd")
	return(dt_out)
}


# ====================
# = Helper Functions =
# ====================
lag_design <- function(y, lags, nMin=15, update=FALSE){
	lag_max <- max(lags)
	lags_names <- paste0("lag",lags)
	
	tsVec0 <- y
	tsVec <- tail(y, -lag_max)
	
	
	if(update){
		tsVec_lag <- matrix(embed(tsVec0, lag_max+1)[,-1, drop=FALSE], nrow=1) #[,-(lag_max+1)]
		dimnames(tsVec_lag) <- list(NULL, paste0('lag',1:lag_max))
	}else{
		tsVec_lag <- embed(tsVec0, lag_max+1)[,-(lag_max+1)]
		dimnames(tsVec_lag) <- list(NULL, paste0('lag',lag_max:1))
	}
	
	mod_X0 <- cbind(y=tsVec, tsVec_lag[,lags_names, drop=FALSE])
	
	
	mod_X <- mod_X0[, c("y",lags_names), drop=FALSE]
	
	return(mod_X)
}


time_factors <- function(dt, types=c("minute", "hour", "dow", "month"), lag_max=24*7*(60/15)+1){
	types <- match.arg(types, several.ok=TRUE)
	d <- data.table(datetime=dt)
	if("minute"%in%types){
		d[,minute:=as.factor(minute(datetime))]
	}
	if("hour"%in%types){
		d[,hour:=as.factor(hour(datetime))]
	}
	if("dow"%in%types){
		d[,dow:=as.factor(as.integer(format.Date(datetime, format="%u"))-1)]
	}
	if("month"%in%types){
		d[,month:=as.factor(month(datetime))]
	}
	
	return(tail(d, -lag_max)) # added [] so it would print ... this still confuses me, seems to be working though
}








