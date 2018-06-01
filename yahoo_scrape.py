import pandas as pd
import numpy as np
import pandas_datareader.data as web
import datetime
import csv
import atexit
from time import clock
import sqlite3
from multiprocessing.pool import ThreadPool as Pool

#Stock Data Pull Timeframe
start_time = datetime.datetime.now().date() - datetime.timedelta(days=2000)
end_time = datetime.datetime.now().date()

#Load List of Companies
fp='companies.txt'%end_time
with open(fp) as fil:
    content = fil.readlines()

#Set Number Of CPU Cores    
pool=Pool(4)

#Setup runtime clock
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

#Stock Pulling
def Datapull(Stock):
    try:
        while True:
            df = web.DataReader(Stock,'yahoo',start_time,end_time)
            delta = df['Adj Close'].diff()
            df.index = pd.to_datetime(df.index)
            dUp, dDown = delta.copy(), delta.copy()
            dUp[dUp < 0] = 0
            dDown[dDown > 0] = 0
            RolUp_mean= dUp.rolling(window=14,center=False).mean()
            RolDown_mean= dDown.rolling(window=14,center=False).mean().abs()
            RS_mean = RolUp_mean / RolDown_mean
            rsi_mean= 100.0 - (100.0 / (1.0 + RS_mean))
            df.insert(6, 'RSI', rsi_mean)
            return df 
    except:
        print 'Datapull: %s'%Stock	

def PoolPull(ticker):
    try:
        conn=sqlite3.connect("stock_history.db")
        curs=conn.cursor()
        data = Datapull(ticker)
        data.columns = data.columns.str.replace(' ', '_')
        data.to_sql(name=ticker, con=conn, if_exists='append')
        conn.commit()
        conn.close()
        print 'Retrieved:', ticker
    except Exception, e:
        conn.close()
        print 'Main Loop', str(e), ticker
        
if __name__=='__main__':
    company = [x.strip() for x in content]
    rsi=pool.map(PoolPull,company)
