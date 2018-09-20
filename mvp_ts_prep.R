library(data.table)

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


# ================
# = Write to CSV =
# ================
# save actual R object for compression, and so i don't have to re-convert to datetime class
save(dat2, file=paste0(proj_dir,"/data_int","/mvp_ts_prep.RData"))

# for csv, convert datetime to character
dat2[,datetime_rnd:=as.character(datetime_rnd)]
fwrite(dat2, paste0(proj_dir,"/data_int","/mvp_ts_prep.csv"))

