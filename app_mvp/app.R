
library(shiny)
library(data.table)
library(fasttime)

library(maptools)
library(rgeos)
library(rgdal)

# ========
# = Data =
# ========
# proj_dir <- "/Users/Battrd/Documents/School&Work/Insight/parking"
# data_dir <- "data_int"

results_file <- "mvp_f_out2.csv"
# results_dir <- file.path(data_dir, results_file)
results <- fread(results_file)
results[,datetime_rnd:=fasttime::fastPOSIXct(datetime_rnd)]
setkey(results, ViolationPrecinct, datetime_rnd)

# precinct shape file
shp_path <- "./PolicePrecincts/geo_export_210823e9-ae0b-4030-807e-35d84173eb87.shp" #file.path(getwd(), "geo_export.shp")  #"geo_export_210823e9-ae0b-4030-807e-35d84173eb87.shp"
layer <- ogrListLayers(dsn=shp_path)
ogrInfo(shp_path, layer=layer)
prec_outline <- readOGR(shp_path, layer=layer)
prec_ind <- prec_outline@data$precinct %in% results[,unique(ViolationPrecinct)]
prec_outline <- prec_outline[prec_ind,]

# test plotting shape
ps <- prec_outline@polygons
precs <- prec_outline@data$precinct
sp_ps <- SpatialPolygons(ps)
po_proj <- proj4string(prec_outline)


# read in street length per precinct
street_leng_per_prec <- fread("street_leng_per_prec.csv")[precinct%in%results[,ViolationPrecinct]]
street_leng_per_prec[,street_leng:=street_leng/5280] # convert feet to miles
setnames(street_leng_per_prec, 'precinct', "ViolationPrecinct")


# =========================
# = Handy Input Varialbes =
# =========================
min_time <- results[,min(datetime_rnd)]
max_time <- results[,max(datetime_rnd)]
start_range <- c(min_time, min_time+60*60*24)
max_time_24 <- start_range[2]


# ====================
# = Figure Functions =
# ====================
getPolyCoords <- function(poly){
	# this function gets coordinates for all the polygons that are part of a single shape (sometimes multiple islands are part of a precinct, e.g., so isn't just 1 shape always)
	poly_list <- poly@polygons[[1]]@Polygons
	if(length(poly_list)>1){
		coords <- sapply(poly_list, function(x)x@coords)
		cout <- Reduce(rbind, coords)
	}else{
		cout <- poly_list[[1]]@coords
	}
	return(cout)
}
point2prec <- function(poly, x=-74, y=40.735){
	# point2prec(sp_ps[1], x=-74.01, y=40.71) # example for precinct 1
	p2 <- getPolyCoords(poly)
	ind_out <- point.in.polygon(x, y, pol.x=p2[,1], pol.y=p2[,2]) != 0
	return(ind_out)
}
findPrecinct <- function(xc, yc, all_poly=sp_ps){
	# findPrecinct(xc=-74.01, yc=40.71)
	# findPrecinct(xc=-73.985, yc=40.725) # should be precs[5] (precinct 9)
	ind <- vector('logical',length(sp_ps))
	for(s in 1:length(sp_ps)){
		ind[s] <- point2prec(sp_ps[s], x=xc, y=yc)
	}
	return(precs[ind])
}
makeMap <- function(prec=NULL, ...){
	par(mar=rep(0,4))
	plot(sp_ps)
	polygonsLabel(sp_ps, labels=precs, method='inpolygon')
	if(!is.null(prec)){
		plot(sp_ps[precs==prec], border='blue', col=adjustcolor('blue', 0.25),  add=TRUE, lwd=3)
	}
}

makeTS <- function(precinct=1, startTime=NULL, stopTime=NULL){
	if("ViolationPrecinct"!=key(results)[1]){setkey(results, ViolationPrecinct, datetime_rnd)}
	tr <- results[.(precinct)]
	if(!is.null(stopTime)){
		tr <- tr[datetime_rnd <= stopTime]
	}
	dr <- range(tr[,datetime_rnd])
	elap <- as.character(round(difftime(dr[2], dr[1], units='days')))
	if(elap=='1'){
		fhLabel <- "Today's Forecast" #"24-hour Forecast"
	}else {
		fhLabel <- paste(elap, "Forecast", sep="-day ")
	}
	
	plot(tr[,datetime_rnd], tr[,counts_hat], type='l', ylab="Tickets per 15 min", xlab="Date")
	mtext(fhLabel, side=3, adj=0.04689, line=1, font=2, cex=1.2)
}

zCol <- function (nCols, Z){
	cols <- (grDevices::colorRampPalette(c("#000099", "#00FEFF", "#45FE4F", "#FCFF00", "#FF9400", "#FF3100")))(nCols)
	colVec_ind <- cut(Z, breaks = nCols)
	colVec <- cols[colVec_ind]
	return(colVec)
}

addPoly <- function(precinct=1, startTime, stopTime, duration){
	# fill in time components
	if(missing(duration)){
		duration <- stopTime - startTime
	}
	if(missing(startTime)){
		startTime <- stopTime - duration
	}
	if(missing(stopTime)){
		stopTime <- startTime + duration
	}
	
	# check for data availability
	stopifnot(startTime >= min_time)
	stopifnot(stopTime <= max_time)
	
	tRes <- results[.(precinct)]
	t_street_leng <- street_leng_per_prec[ViolationPrecinct==list(precinct), street_leng]
	
	tStep <- diff(tRes[,datetime_rnd])[1]
	xvals <- seq(startTime, stopTime, by=tStep)
	resultsY <- tRes[datetime_rnd%in%xvals, counts_hat]
	
	getQ <- function(x){quantile(x, seq(0.1, 0.9, length.out=100))}
	colVec <- zCol(256, c(mean(resultsY), getQ(tRes[,counts_hat])))
	polyCol <- colVec[1]
	
	topLine <- list(x=xvals, y=resultsY)
	botLine <- list(x=rev(xvals), y=rep(-1,length(xvals)))
	polyLines <- rbindlist(list(topLine, botLine), use.names=TRUE)
	polygon(x=polyLines$x, y=polyLines$y, border=polyCol, col=adjustcolor(polyCol,0.2), lwd=2)
	
	baseTickets_mtext <- paste0("Total tickets = ", round(sum(resultsY)))
	tickets_leng_mtext <- paste0(baseTickets_mtext, " (", round(t_street_leng), " per mile)")
	mtext(tickets_leng_mtext, side=3, line=0, adj=0.05, font=2, cex=1.2)
	
}



# ======
# = UI =
# ======
# Define UI for app that draws a histogram ----
ui <- fluidPage(
	
	titlePanel("ticketEd: Learn to avoid parking tickets", windowTitle="ticketEd"),
	
	
	tags$script('
	  $(document).ready(function () {
	    navigator.geolocation.getCurrentPosition(onSuccess, onError);

	    function onError (err) {
	    Shiny.onInputChange("geolocation", false);
	    }
    
	   function onSuccess (position) {
	      setTimeout(function () {
	          var coords = position.coords;
	          console.log(coords.latitude + ", " + coords.longitude);
	          Shiny.onInputChange("geolocation", true);
	          Shiny.onInputChange("lat", coords.latitude);
	          Shiny.onInputChange("long", coords.longitude);
	      }, 1100)
	  }
	  });
	'),
	tags$script('
	$(document).on("keyup", function(e) {
	  if(e.keyCode == 13){
	    Shiny.onInputChange("keyPressed", Math.random());
	  }
	});
	'),
	
	
	fluidRow(
		column(5, offset=1,
			uiOutput('dropdowns')
		),
		
		column(6,
			plotOutput('map', click='map_click')
		)
	),
	
	fluidRow(align='center',
		column(12, offset=0, align='center',
			plotOutput('ticketTS')
		)
	),
	
	fluidRow(align='center',
		column(12, offset=0, align='center',
			uiOutput('dateSlider')
		)
	)
	
)


# ==========
# = Server =
# ==========
server <- function(input, output){
	# 3 ways that the precinct can be set:
	#   - dropdown menu
	#   - browser geolocation based on IP
	#   - user enter's an address, which is geolocated, and then matched to precinct
	
	# This is for the search bar part of the UI
	# It is the only UI element (as input) that doesn't react to other input or conditionals
	def_prec_entered <- reactiveValues() # this will hold precinct value based on user's address search
	observeEvent(input$keyPressed, { # every time 'enter' is pressed, this value will update, and then expression in {} will run
		
		# sign up for free API key at
		# https://developer.here.com/
		id <- 'JaLSMHSVoXr5YkHwoxLX'
		code <- 'rbcl6gPn0DqElAs9-sm0Ig'
		gc_ll <- geocodeHERE::geocodeHERE_simple(input$enterAddress, App_id=id, App_code=code)
		if(all(!is.na(gc_ll))){ # only match geolocated address (i.e., lon/lat) to precinct if lon/lat not NA or missing
			def_prec_entered$a <- findPrecinct(xc=gc_ll$Longitude, yc=gc_ll$Latitude)
		}
	})
	
	# Render the dynamic aspects of the UI
	# all of the sliders and bars etc depend on other inputs
	# except for the address search bar part of the UI, which doesn't react to any other UI elements
	# all reactive UI elements have to be in here
	# they are called 'dropdowns' here and in UI section above
	output$dropdowns <- renderUI({
		def_prec <- NULL # start off with NULL; makes it easier to code various options w/o nesting if(){}else{}
		
		# update 'default' precinct w/ user location
		# if these conditions are met
		# which basically test if it's available
		# but also if the user has tried to enter an address
		if(length(input$geolocation)>0 && input$geolocation && input$enterAddress==''){ # need && so that it stops if first condition isn't true, b/c if it's empty then can't test if T/F (will throw error); also, only use the geolocation if the user hasn't entered an address			
			def_prec <- findPrecinct(xc=input$long, yc=input$lat)
		}
		
		# update the 'default' precinct for the dropdown menu w/
		# the geolocated address => precinct if 
		# a bunch of conditions are met
		if(length(def_prec_entered$a)>0 && !is.null(def_prec_entered$a) && is.finite(def_prec_entered$a)){
			def_prec <- def_prec_entered$a
		}
		
		# a default precinct to have selected
		# default is precinct 1
		if(is.null(def_prec)){# testing for any non-finite values covers Inf, -Inf, NA, etc (as well as text input instead of numeric, though that shouldn't happen)
			def_prec <- 1
		}
		
		# panel of user inputs/ options at top left of page
		# includes, address search bar
		# precinct select dropdown
		# and forecast horizon dropdown
		# these are collectively called 'dropdowns' elsewhere (in UI) even though first is a textInput
		# (the text input affects the precinct dropdown)
		wellPanel(
			textInput('enterAddress', h5("Search Address"), value='', placeholder="35 E 21st St, New York, NY"),
			selectInput("dropdownPrecinct", h5("Select Precinct"), choices=precs, selected=def_prec),
			selectInput('dropHorizon', h5("Select Forecast Horizon"), choices=c("24-hour","7-day"))
		)
	})
	
	# time slider bar
	output$dateSlider <- renderUI({
		req(input$dropHorizon)
		
		# below sets the start of the slider to current hour (in default 1st day)
		# if the forecast horizon is 24 hours
		if(input$dropHorizon=="24-hour"){
			cur_time <- suppressWarnings({format(Sys.time(), "2017-01-01 %H:%M:%S", tz='America/New_York')})
			vec0 <- seq(from=min_time, to=max_time, by=60*15)
			vec <- unique(format(vec0, "2017-01-01 %H:%M:%S"))
			min_time2 <- vec0[which.min(abs(as.POSIXct(vec)-as.POSIXct(cur_time)))]
			start_range[1] <- min_time2
		}
		
		# defines the slider bar
		sliderInput("dateRange", label = h5("Select Dates"), min=min_time, max=c('24-hour'=max_time_24, '7-day'=max_time)[input$dropHorizon], value=start_range, timeFormat="%d-%b %H:%M", timezone='UTC', step=60*15, width="85%")
		
	})
	
	# makes the map showing precinct selected
	output$map <- renderPlot({
		makeMap(precUse<-input$dropdownPrecinct)
	})
	
	
	# adds the time series plot with polygon and ticket counts
	output$ticketTS <- renderPlot({
		req(input$dateRange, input$dropHorizon, input$dropdownPrecinct)
		p <- as.integer(input$dropdownPrecinct)
		makeTS(p, stopTime=c('24-hour'=max_time_24, '7-day'=max_time)[input$dropHorizon])
		addPoly(p, startTime=input$dateRange[1], stopTime=input$dateRange[2])
	})
	
	# output$map_coords <- renderPrint({c(input$map_click$x, input$map_click$y)})
	
}


# ==========
# = Return =
# ==========
shinyApp(ui = ui, server = server)
# runApp("app_mvp")
