
library(shiny)
library(data.table)
library(fasttime)

library(maptools)
library(rgeos)
library(rgdal)

# ========
# = Data =
# ========
proj_dir <- "/Users/Battrd/Documents/School&Work/Insight/parking"
data_dir <- "data_int"

results_file <- "mvp_f_out2.csv"
results_dir <- file.path(proj_dir, data_dir, results_file)
results <- fread(results_dir)
results[,datetime_rnd:=fasttime::fastPOSIXct(datetime_rnd)]
setkey(results, ViolationPrecinct, datetime_rnd)

# precinct shape file
shp_path <- "/Users/Battrd/Documents/School&Work/Insight/parking/data/PolicePrecincts/geo_export_210823e9-ae0b-4030-807e-35d84173eb87.shp"
layer <- ogrListLayers(shp_path)
ogrInfo(shp_path, layer=layer)
prec_outline <- readOGR(shp_path, layer=layer)
prec_ind <- prec_outline@data$precinct %in% results[,unique(ViolationPrecinct)]
prec_outline <- prec_outline[prec_ind,]

# test plotting shape
ps <- prec_outline@polygons
precs <- prec_outline@data$precinct
sp_ps <- SpatialPolygons(ps)
po_proj <- proj4string(prec_outline)





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
point2prec <- function(x=-74, y=40.735){
	if(is.null(x) | is.null(y)){
		return(NULL)
	}else{
		testObj <- SpatialPoints(cbind(x, y))
		proj4string(testObj) <- po_proj
		over(testObj, prec_outline)$precinct
	}
}
makeMap <- function(prec=NULL, ...){
	par(mar=rep(0,4))
	plot(sp_ps)
	polygonsLabel(sp_ps, labels=precs, method='inpolygon')
	if(!is.null(prec)){
		plot(sp_ps[precs==prec], border='blue', col=adjustcolor('blue', 0.25),  add=TRUE, lwd=3)
	}
}

makeTS <- function(precinct=1, startTime=NULL, stopTime=NULL, duration=NULL){
	# ind <- results[,ViolationPrecinct==precinct]
	if("ViolationPrecinct"!=key(results)[1]){setkey(results, ViolationPrecinct, datetime_rnd)}
	tr <- results[.(precinct)]
	if(!is.null(stopTime)){
		tr <- tr[datetime_rnd <= stopTime]
	}
	plot(tr[,datetime_rnd], tr[,counts_hat], type='l', ylab="Tickets per 15 min", xlab="Date")
}

zCol <- function (nCols, Z){
	cols <- (grDevices::colorRampPalette(c("#000099", "#00FEFF", "#45FE4F", "#FCFF00", "#FF9400", "#FF3100")))(nCols)
	colVec_ind <- cut(Z, breaks = nCols)
	colVec <- cols[colVec_ind]
	return(colVec)
}

addPoly <- function(precinct=1, startTime, stopTime, duration){
	# print((startTime))
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
	
	tStep <- diff(tRes[,datetime_rnd])[1]
	xvals <- seq(startTime, stopTime, by=tStep)
	resultsY <- tRes[datetime_rnd%in%xvals, counts_hat]
	
	colVec <- zCol(256, tRes[,do.call(':',as.list(range(round(counts_hat))))])
	polyCol <- colVec[as.integer(round(mean(resultsY)))]
	
	topLine <- list(x=xvals, y=resultsY)
	botLine <- list(x=rev(xvals), y=rep(-1,length(xvals)))
	polyLines <- rbindlist(list(topLine, botLine), use.names=TRUE)
	polygon(x=polyLines$x, y=polyLines$y, border=polyCol, col=adjustcolor(polyCol,0.2))
	
}


# ======
# = UI =
# ======
# Define UI for app that draws a histogram ----
ui <- fluidPage(
	
	titlePanel("ticketEd: Future parking tickets"),
	
	
	fluidRow(
		column(4, 
			wellPanel(
				# sliderInput("range", label = h5("Select Dates"), min=min_time, max=max_time, value=start_range, timeFormat="%d-%b %H:%M"),
				selectInput("dropdownPrecinct", h5("Select Precinct"), choices=precs, selected=precs[1]),
				selectInput('dropHorizon', h5("Select Forecast Horizon"), choices=c("24-hour","7-day"))
			)
		),
		
		column(5,
			plotOutput('map', click='map_click')
		)
	),
	
	fluidRow(align='center',
		column(10, offset=0, align='center',
			plotOutput('ticketTS')
		)
	),
	
	fluidRow(align='center',
		column(10, offset=0, align='center',
			# sliderInput("dateRange", label = h5("Select Dates"), min=min_time, max=max_time, value=start_range, timeFormat="%d-%b %H:%M")
			uiOutput('dateSlider')
		)
	)

	# sidebarLayout(
#
# 		sidebarPanel(width=5,
# 				sliderInput("range", label = h5("Select Dates"), min=min_time, max=max_time, value=start_range, timeFormat="%d-%b %H:%M"),
# 				selectInput("dropdownPrecinct", h5("Select Precinct"), choices=precs, selected=precs[1])
# 		),
#
# 		mainPanel('Parking Map')
#
#
#
# 	),
	
	
	# fluidRow(column(4, offset=4,
#
# 			plotOutput('map', click='map_click')
#
# 		)
# 	),
	# fluidRow(column(4, offset=4,
# 			textOutput('map_coords')
# 		)
	# )
	
	
	
)

# ==========
# = Server =
# ==========
# Define server logic required to draw a histogram ----
server <- function(input, output){
	
	# precUse <- function(){return(1)}
	
	# doesn't work
	# precUse <- reactiveValues()
	# precUse$prec <- point2prec(input$map_click$x, input$map_click$y)
	
	# precUse <- reactiveValues(point2prec(input$map_click$x, input$map_click$y)) # doesn't work
	
	# precUse <- point2prec(input$map_click$x, input$map_click$y) # doesn't work
	
	# output$map <- makeMap()
	
	# precUse <- eventReactive(input$map_click, {
	# 	point2prec(input$map_click$x, input$map_click$y) # works, but always overwrite to default
	# })
	
	# dm <- reactive({
# 		if(is.null(input$dropHorizon)){
# 			max_time
# 		}else{
# 			c('24-hour'=max_time_24, '7-day'=max_time)[input$dropHorizon]
# 		}
# 	})
	
	
	output$dateSlider <- renderUI({
		
		sliderInput("dateRange", label = h5("Select Dates"), min=min_time, max=c('24-hour'=max_time_24, '7-day'=max_time)[input$dropHorizon], value=start_range, timeFormat="%d-%b %H:%M")
		
	})
	
	output$map <- renderPlot({
		
		makeMap(precUse<-input$dropdownPrecinct)
		
	})
	
	observe({
		# print(input$dateRange)
		print(input$dropHorizon)
	})
	
	output$ticketTS <- renderPlot({
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