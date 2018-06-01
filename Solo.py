from pylab import *
import pandas as pd
import numpy as np
import pandas_datareader.data as web
import datetime
import linecache
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
from pandas.tseries.offsets import BDay

import atexit
from time import clock
import time

import sqlite3

#conn=sqlite3.connect("CompanyHist.db")
#curs=conn.cursor()

from subprocess import call
from multiprocessing.pool import ThreadPool as Pool

def secondsToStr(t):
	return "%d:%02d:%02d.%03d" %reduce(lambda ll,b : divmod(ll[0],b) + ll[1:],[(t*1000,),1000,60,60])

line = "="*40
def log(s, elapsed=None):
	print line
	print secondsToStr(clock()), '-', s
	if elapsed:
		print "Elapsed time:", elapsed
	print line
	print

def endlog():
	end = clock()
	elapsed = end-start
	log("End Program", secondsToStr(elapsed))

def now():
	return secondsToStr(clock())

start = clock()
atexit.register(endlog)
log("Start Program")

def Datapull(Stock):
	try:
		while True:
			df = web.DataReader(Stock,'yahoo',start_time,end_time)
			delta = df['Adj Close'].diff()
			df.insert(4,'Symbol',Stock)
			#-----------
			dUp, dDown = delta.copy(), delta.copy()
			dUp[dUp < 0] = 0
			dDown[dDown > 0] = 0
			RolUp_mean= dUp.rolling(window=14,center=False).mean()
			RolDown_mean= dDown.rolling(window=14,center=False).mean().abs()
			RolUp_std= dUp.rolling(window=14,center=False).std()
			RolDown_std= dDown.rolling(window=14,center=False).std()
			RolUp_mad= dUp.rolling(window=14,center=False).apply(mad)
			RolDown_mad= dDown.rolling(window=14,center=False).apply(mad)
			RS_mean = RolUp_mean / RolDown_mean
			RS_std = RolUp_std / RolDown_std
			RS_mad = RolUp_mad / RolDown_mad
			monthhigh = df['Adj Close'].rolling(window=30, center=False).max()

			rsi_mean= 100.0 - (100.0 / (1.0 + RS_mean))
			rsi_std= 100.0 - (100.0 / (1.0 + RS_std))
			rsi_mad= 100.0 - (100.0 / (1.0 + RS_mad))
			df.insert(5, 'RSI', rsi_mean)
			df.insert(6, 'STD', rsi_std)
			df.insert(7, 'MAD', rsi_mad)
			df.insert(8, 'monthhigh', monthhigh)
			return df, df['Adj Close'].tail(1)[0], df['Volume'].tail(1)[0],df['monthhigh'].tail(1)[0]

	except:
		print 'Datapull'
		#badline+='%s\n'%Stock
def last_buy(df):
	temp = df['RSI'].tail(1)[0]
	i=0
	Buy=False
	while Buy==False:
		i+=1
		temp = df['RSI'].tail(i)[0]
		if df['RSI'].tail(i)[0]/df['STD'].tail(i)[0]>0.9:
			Hold = True
		elif i>150:
			BuyDate = datetime.datetime.now().date() - BDay(i-1) #datetime.timedelta(days=i-1)
			return BuyDate			
		elif temp<30:
			Buy = True
			temp = df['RSI'].tail(i)[0]			
			BuyDate = datetime.datetime.now().date() - BDay(i-1) #datetime.timedelta(days=i-1)
			line = "%s\t%f\t%f\t%f\t%f\n"%(BuyDate, df['Adj Close'].tail(i)[0], df['RSI'].tail(i)[0],df['STD'].tail(i)[0], df['MAD'].tail(i)[0])
			return BuyDate 
def last_sell(df):
	Sell = False
	temp = df['RSI'].tail(1)[0]
	i=0
	while Sell==False:
		i+=1
		temp = df['RSI'].tail(i)[0]
		if i>150:
			SellDate = datetime.datetime.now().date() - BDay(i-1) #datetime.timedelta(days=i-1)
			return SellDate			

		if df['MAD'].tail(i+1)[0] > 69 and df['MAD'].tail(i)[0]>69:
			if df['STD'].tail(i)[0]/df['MAD'].tail(i)[0] < 0.95:
				Sell = True
				typ = 1
				SellDate = datetime.datetime.now().date() - BDay(i-1) #datetime.timedelta(days=i-1)
				lin = "%s\t%f\t%f\t%f\t%f\n"%(SellDate, df['Adj Close'].tail(i)[0], df['RSI'].tail(i)[0],df['STD'].tail(i)[0], df['MAD'].tail(i)[0])
				return SellDate
			else:
				if df['STD'].tail(i)[0]/df['MAD'].tail(i)[0] < 1:
					Hold = True
				else:
					ratio_check = check_avg(df)[0]
					if 0.99 < ratio_check and ratio_check < 1.01:
						Sell = True
						SellDate = datetime.datetime.now().date() - BDay(i-1) #datetime.timedelta(days=i-1)
						lin = "%s\t%f\t%f\t%f\t%f\n"%(SellDate, df['Adj Close'].tail(i)[0], df['RSI'].tail(i)[0],df['STD'].tail(i)[0], df['MAD'].tail(i)[0])
						return SellDate
					else:
						Hold = True			
def Two_Weeks(df,i):
	if df['RSI'].tail(i)[0] < 30:
   		if df['RSI'].tail(i)[0]/df['STD'].tail(i)[0]>0.9:
			rec = 'Low Hold'
		else:
			if df['Adj Close'].ix[0] *1.3 < df['Adj Close'].ix[-1]:
				rec = 'Buy'
			else:
				rec = 'Flat'
	else: #if df['RSI'].tail(i)[0] >= 30:
		if df['MAD'].tail(i+1)[0] > 69 and df['MAD'].tail(i)[0]>69:
			if df['STD'].tail(i)[0]/df['MAD'].tail(i)[0] < 0.95:
				rec = 'Sell: 1'
			else: 
				if df['STD'].tail(i)[0]/df['MAD'].tail(i)[0] < 1:
					rec = 'High Hold'
				else:
					ratio_check = check_avg(df)[0]
					if 0.99 < ratio_check and ratio_check < 1.01:
						rec = 'Sell: 2'
					else:
						rec = 'Peaking'
		else:   
			rec = 'Mid Hold'
	return rec


def Chop(data, recommend, dur):
	daycount = 0
	for i in range(dur):
		if recommend == 'Sell':
			rec = Two_Weeks(data,i)
			if rec == 'Sell':
				daycount+=1	
		elif recommend == 'Buy':
			rec = Two_Weeks(data,i)
			if rec == 'Buy':
				daycount+=1	
		else:
			break
	return daycount
def mad(data, axis=None):
	return np.mean(np.absolute(data - np.mean(data, axis)), axis)

def check_avg(dataframe):
	threeday = dataframe['MAD'].tail(3).mean()
	ratio_avg = threeday/dataframe['MAD'].tail(1)
	return ratio_avg

	
start_time = datetime.datetime.now().date() - datetime.timedelta(days=1500)
end_time = datetime.datetime.now().date() # - datetime.timedelta(days=12)


print start_time, end_time
timeout=time.time()+20
fp='companies.txt'
#outfile = open('%s.txt'%end_time,'w')
line=''
with open(fp) as fil:
    content = fil.readlines()

pool=Pool(4)
print datetime.datetime.now()

def PoolPull(ticker): #,dowc,spc, dbuy, dsell, sbuy, ssell):
	try:
		conn=sqlite3.connect("CompanyHist.db")
		curs=conn.cursor()

		#ticker=ticker[0]
		data,lastprice, volume, monthhigh = Datapull(ticker)
		MonthHigh=float(monthhigh)
		StopLoss = (lastprice - MonthHigh)/MonthHigh
		#Crash=False
		recommend=Two_Weeks(data,1)
		SellDate = last_sell(data).to_pydatetime().date()
		BuyDate = last_buy(data).to_pydatetime().date()
		volume= float(volume) 
		curs.execute(""" UPDATE stocks SET BuyDate=?, SellDate=?, Recommendation=?, Volume= ?, price =?, REAL=? WHERE symbolyahoo = ?""", (BuyDate, SellDate,recommend,volume, lastprice, StopLoss, ticker))
		conn.commit()
		conn.close()
		print 'Retrieved:', ticker
	except Exception, e:
		conn.close()
		print 'Main Loop', str(e), ticker
		#badline+='%s\n'%ticker

def ind_avg():
	conn=sqlite3.connect("CompanyHist.db")
        curs=conn.cursor()
	DOW, DOWlast, DOWvol, DOWHigh = Datapull('^DJI')
	SP, SPlast, SPvol, SPHigh = Datapull('^GSPC')
	DOWSell = last_sell(DOW).to_pydatetime().date()
        DOWBuy = last_buy(DOW).to_pydatetime().date()
	DOWrec = Two_Weeks(DOW,1)
	DOWvol = float(DOWvol)
	DOWStop = (DOWlast-DOWHigh)/(DOWHigh)
	SPSell = last_sell(SP).to_pydatetime().date()
        SPBuy = last_buy(SP).to_pydatetime().date()
	SPrec = Two_Weeks(SP,1)
        SPvol = float(SPvol)	
        SPStop = (SPlast-SPHigh)/(SPHigh)
	curs.execute(""" UPDATE stocks SET BuyDate=?, SellDate=?, Recommendation=?, Volume= ?, price =?, REAL=? WHERE symbolyahoo = ?""", (DOWBuy, DOWSell,DOWrec,DOWvol, DOWlast, DOWStop, '^DJI'))
	curs.execute(""" UPDATE stocks SET BuyDate=?, SellDate=?, Recommendation=?, Volume= ?, price =?, REAL=? WHERE symbolyahoo = ?""", (SPBuy, SPSell,SPrec,SPvol, SPlast, SPStop, '^GSPC'))
        conn.commit()
        conn.close()
	return DOWStop ,SPStop, DOWBuy, DOWSell, SPBuy,SPSell


#badline=''
if __name__=='__main__':
	company = [x.strip() for x in content]
	#DOWcomp, SPcomp, DOWBuy, DOWSell, SPBuy,SPSell = ind_avg()
	#for ticker in company:
	rsi=pool.map(PoolPull,company) #, DOWcomp,SPcomp, DOWBuy, DOWSell, SPBuy,SPSell) #,company)
        #rsi=pool.apply_async(PoolPull, args=(company, DOWcomp,SPcomp, DOWBuy, DOWSell, SPBuy,SPSell)) #,company)

#outfile = open('FAILED.txt','w')
#outfile.write(badline)
