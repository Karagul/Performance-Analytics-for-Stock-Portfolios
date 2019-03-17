
# Stock Market Case in R
rm(list=ls(all=T)) 

require(RPostgreSQL) 
require(DBI)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="stockmarketreader"
                 ,password="read123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="stockmarkethw2"
)

#custom calendar
qry='SELECT * FROM custom_calendar ORDER by date'
ccal<-dbGetQuery(conn,qry)
#eod prices and indices
qry1="SELECT symbol,date,adj_close FROM eod_indices WHERE date BETWEEN '2011-12-30' AND '2017-12-31'"
qry2="SELECT ticker,date,adj_close FROM eod_quotes WHERE date BETWEEN '2011-12-30' AND '2017-12-31'"
eod<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
dbDisconnect(conn)

#Explore
head(ccal)
tail(ccal)
nrow(ccal)

head(eod)
tail(eod)
nrow(eod)

head(eod[which(eod$symbol=='SP500TR'),])

#Add one data item (for 2011-12-30) for monthly data
eod_row<-data.frame(symbol='SP500TR',date=as.Date('2011-12-30'),adj_close=2158.94)
eod<-rbind(eod,eod_row)
tail(eod)

# Use Calendar 
tdays<-ccal[which(ccal$trading==1),,drop=F]

# Percentage of completeness
pct<-table(eod$symbol)/(nrow(tdays)-1)
selected_symbols_daily<-names(pct)[which(pct>=0.99)]
eod_complete<-eod[which(eod$symbol %in% selected_symbols_daily),,drop=F]

#check
head(eod_complete)
tail(eod_complete)
nrow(eod_complete)

#YOUR TURN: perform all these operations for monthly data

#Create eom and eom_complete
mdays<-ccal[which(ccal$trading==1 & ccal$eom==1),,drop=F]
#check
head(mdays)
tail(mdays)
row(mdays)

# Transform (Pivot)
require(reshape2) 
eod_pvt<-dcast(eod_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)
#check
eod_pvt[1:10,1:5]
ncol(eod_pvt) 
nrow(eod_pvt)

# Merge with Calendar -----------------------------------------------------
eod_pvt_complete<-merge.data.frame(x=tdays[,'date',drop=F],y=eod_pvt,by='date',all.x=T)
eom_pvt_complete<-merge.data.frame(x=mdays[,'date',drop=F],y=eod_pvt,by='date',all.x=T)


#check
eod_pvt_complete[1:10,1:5] 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

eom_pvt_complete[1:10,1:5]
ncol(eom_pvt_complete)
nrow(eom_pvt_complete)

#use dates as row labels
rownames(eod_pvt_complete)<-eod_pvt_complete$date
eod_pvt_complete$date<-NULL
eod_pvt_complete[1:10,1:5] 

rownames(eom_pvt_complete)<-eom_pvt_complete$date
eom_pvt_complete$date<-NULL
eom_pvt_complete[,1:5]

# Missing Data Imputation 

# Replace a few missing (NA or NaN) data items with previous data (max upto 3 rows)
require(zoo)
eod_pvt_complete<-na.locf(eod_pvt_complete,na.rm=F,fromLast=F,maxgap=3)
#re-check
eod_pvt_complete[1:10,1:5] 

# Calculating Returns
require(PerformanceAnalytics)
eod_ret<-CalculateReturns(eod_pvt_complete)
#check
eod_ret[1:10,1:4] 

#Remove the first row
eod_ret<-tail(eod_ret,-1) 
#check
eod_ret[1:10,1:4] 

#Calculate eom_ret (monthly returns)
eom_ret <- CalculateReturns(eom_pvt_complete)
eom_ret<- tail(eom_ret,-1)
eom_ret[1:10,1:4]

# Check for extreme returns
#Define a function colMax
colMax <- function(data) sapply(data, max, na.rm = TRUE)

max_daily_ret<-colMax(eod_ret)
max_daily_ret[1:10] 
selected_symbols_daily<-names(max_daily_ret)[which(max_daily_ret<=1.00)]
length(selected_symbols_daily)

#Subset eod_ret by keeping colnames with max daily returns <=1
eod_ret<-eod_ret[,which(colnames(eod_ret) %in% selected_symbols_daily)]
#check
eod_ret[1:10,1:4]
ncol(eod_ret)
nrow(eod_ret)

#Subset eom_ret data
max_monthly_return <- colMax(eom_ret)
max_monthly_return[1:10]
selected_symbols_monthly<-names(max_monthly_return)[which(max_monthly_return<=1.00)]
length(selected_symbols_monthly)
eom_ret <- eom_ret[,which(colnames(eom_ret) %in% selected_symbols_monthly)]
eom_ret[1:10,1:4]

##---------------------------------------------------------------

# Export data into CSV
write.csv(eod_ret,'C:/Temp/eod_ret.csv')
write.csv(eom_ret,'C:/Temp/eom_ret.csv')


# Tabular Return Data Analytics

#Considering 'SP500TR' and c('AAPL','MSFT','GOOG','INTC','AMZN')
#Converting data frames to xts (extensible time series) 
Ra<-as.xts(eom_ret[,c('AAPL','MSFT','GOOG','INTC','AMZN'),drop=F])
Rb<-as.xts(eom_ret[,'SP500TR',drop=F]) #benchmark
#check
head(Ra)
head(Rb)

# Stats
table.Stats(Ra)

# Distributions
table.Distributions(Ra)

#for daily returns: scale =252; for monthly returns: scale = 12
# Returns
table.AnnualizedReturns(cbind(Ra,Rb),scale=12) 

# Accumulate Returns
acc_Ra<-Return.cumulative(Ra)
acc_Ra
acc_Rb<-Return.cumulative(Rb)
acc_Rb

# Capital Assets Pricing Model
table.CAPM(Ra,Rb)

# Graphical Return Data Analytics
# Cumulative returns chart
chart.CumReturns(Ra,legend.loc = 'topleft')
chart.CumReturns(Rb,legend.loc = 'topleft')

chart.CumReturns(cbind(Rb,Ra),legend.loc = 'topleft')

#Box plots
chart.Boxplot(cbind(Rb,Ra))

chart.Drawdown(cbind(Rb,Ra),legend.loc = 'bottomleft')

# MV Portfolio Optimization

# witholding last 252 trading days
Ra_training<-head(Ra,-252)
Rb_training<-head(Rb,-252)

# use the last 252 trading days for testing
Ra_testing<-tail(Ra,252)
Rb_testing<-tail(Rb,252)


#optimize the MV (Markowitz 1950s) portfolio weights based on training
table.AnnualizedReturns(Rb_training)
mar<-mean(Rb_training) #we need daily minimum acceptabe return

require(PortfolioAnalytics)
require(ROI) 
require(ROI.plugin.quadprog) 
pspec<-portfolio.spec(assets=colnames(Ra_training))
pspec<-add.objective(portfolio=pspec,type="risk",name='StdDev')
pspec<-add.constraint(portfolio=pspec,type="full_investment")
pspec<-add.constraint(portfolio=pspec,type="return",return_target=mar)

#optimize portfolio
opt_p<-optimize.portfolio(R=Ra_training,portfolio=pspec,optimize_method = 'ROI')

#extract weights
opt_w<-opt_p$weights

#apply weights to test returns
Rp<-Rb_testing 
#define new column
Rp$ptf<-Ra_testing %*% opt_w

#check
head(Rp)
tail(Rp)

#Compare basic metrics
table.AnnualizedReturns(Rp)

# Chart Hypothetical Portfolio Returns
chart.CumReturns(Rp,legend.loc = 'topleft')
