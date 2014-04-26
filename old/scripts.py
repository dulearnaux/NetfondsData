# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 13:05:23 2014

@author: Dushan
"""
import os
os.chdir('D:\\Google Drive\\Python\\FinDataDownload')

#import single_intraday_pull as sin
import multi_intraday_pull as mul
import datetime as dt

date = dt.datetime(2014,3,17)

#AAPL = sin.single_intraday_pull('AAPL',date)

end = dt.datetime.now() - dt.timedelta(days=2)
start = end - dt.timedelta(days=8)

AAPLp,AAPLt,AAPLb = mul.multi_intraday_pull('ACH.N',start.date(), end.date(),extract='all')


AAPLb.to_csv('D:\\Financial Data\\Netfonds\\AAPLb.csv')

#AAPL['bid'].plot()
#
#
#AAPL.groupby
#diff=AAPL[['time']][(AAPL['time']>dt.datetime(2014,03,18)) & (AAPL['time']<dt.datetime(2014,03,19))]
#
#avg = (diff - diff.shift(1))