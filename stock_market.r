rm(list=ls(all=T)) # this just removes everything from memory
#install.packages("RPostgreSQL")
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
 ,user="stockmarketreader"
 ,password="read123"
 ,host="localhost"
 ,port=5432
 ,dbname="stk_mkt"
)

#custom calendar
qry="SELECT * FROM custom_calendar where date BETWEEN '2012-12-30' AND '2018-03-
31' ORDER by date"
ccal<-dbGetQuery(conn,qry)

#eod prices and indices
qry1="SELECT symbol,date,adj_close FROM eod_indices WHERE date BETWEEN '2012-12-30' AND '2018-03-31'"
qry2="SELECT ticker,date,adj_close FROM eod_quotes WHERE date BETWEEN '2012-12-30' AND '2018-03-31'"
eod<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
dbDisconnect(conn)
head(eod[which(eod$symbol=='SP500TR'),])

#For monthly we may need one more data item (for 2011-12-30)
eod_row<-data.frame(symbol='SP500TR',date=as.Date('2012-12-30'),adj_close=2505.44)
eod<-rbind(eod,eod_row)
tail(eod); dim(eod)
tdays<-ccal[which(ccal$trading==1),,drop=F]
head(tdays)
nrow(tdays)-1
pct<-table(eod$symbol)/(nrow(tdays)-1)
selected_symbols_daily<-names(pct)[which(pct>=0.99)]
eod_complete<-eod[which(eod$symbol %in% selected_symbols_daily),,drop=F]
require(reshape2)
eod_pvt<-dcast(eod_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean,
fill=NULL)
eod_pvt[1:10,1:5]

# Merge with Calendar
eod_pvt_complete<-merge.data.frame(x=tdays[,'date',drop=F],y=eod_pvt,by='date',all.x=T)

#use dates as row names and remove the date column
rownames(eod_pvt_complete)<-eod_pvt_complete$date
eod_pvt_complete$date<-NULL
require(zoo)
eod_pvt_complete<-na.locf(eod_pvt_complete,na.rm=F,fromLast=F,maxgap=3)
require(PerformanceAnalytics)
eod_ret<-CalculateReturns(eod_pvt_complete)
eod_ret[1:10,1:4]
colMax <- function(data) sapply(data, max, na.rm = TRUE)
max_daily_ret<-colMax(eod_ret)
max_daily_ret[1:10] #first 10 max returns
selected_symbols_daily<-names(max_daily_ret)[which(max_daily_ret<=1.00)]
length(selected_symbols_daily)
eod_ret<-eod_ret[,which(colnames(eod_ret) %in% selected_symbols_daily)]
eod_ret[1:10,1:4] 
inds <- which(names(eod_ret) %in%
c('BBY','BAH','BA','HD','HIG','HCA','PAYX','PFG','PZZA','WY','WAT','WEC','RTN','RE','ROLL'))
Ra<-as.xts(eod_ret[,inds,drop=F])
Rb<-as.xts(eod_ret[,'SP500TR',drop=F]) #benchmark
table.AnnualizedReturns(cbind(Rb,Ra),scale=252)
acc_Ra<-Return.cumulative(Ra)
acc_Rb<-Return.cumulative(Rb)
chart.CumReturns(Ra,legend.loc = 'topleft')
chart.CumReturns(Rb,legend.loc = 'topleft')
chart.Drawdown(Ra,legend.loc = 'bottomleft')

# withold the last 252 trading days
Ra_training<-head(Ra,-61)
Rb_training<-head(Rb,-61)

# use the last 252 trading days for testing
Ra_testing<-tail(Ra,61)
Rb_testing<-tail(Rb,61)
chart.CumReturns(cbind(Ra_testing, Rb_testing), legend.loc='left')
table.AnnualizedReturns(Rb_training)
mar<-mean(Rb_training) 

#require(PortfolioAnalytics)
#require(ROI.plugin.quadprog)
pspec<-portfolio.spec(assets=colnames(Ra_training))
pspec<-add.objective(portfolio=pspec,type="risk",name='StdDev')
pspec<-add.constraint(portfolio=pspec,type="full_investment")
pspec<-add.constraint(portfolio=pspec,type="return",return_target=mar)

#optimize portfolio
opt_p<-optimize.portfolio(R=Ra_training, portfolio=pspec, optimize_method = 'ROI')

#extract weights
opt_w<-opt_p$weights
sum(opt_w)

#apply weights to test returns
Rp<-Rb_testing 

#define new column that is the dot product of the two vectors
Rp$ptf<-Ra_testing %*% opt_w

#check
head(Rp)
tail(Rp)

#Compare basic metrics
table.AnnualizedReturns(Rp)

# Chart Hypothetical Portfolio Returns 
chart.CumReturns(Rp,legend.loc = 'topleft')
