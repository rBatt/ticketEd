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

# ==========================
# = Regularize Time Series =
# ==========================
reg_datetime_seq <- seq(from=dat[,min(datetime_rnd)], to=dat[,max(datetime_rnd)], by="15 min")
reg_dt <- data.table(datetime_rnd:=reg_datetime_seq)

dat2 <- merge(reg_dt, dat, on="datetime_rnd", all=TRUE)




