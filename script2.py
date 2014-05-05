# -*- coding: utf-8 -*-

"""multi thread"""


#import pandas as pd
#import numpy as np
import os
os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
#import multi_intraday_pull as mul
#import Netfonds_Ticker_List as NTL   
import time 
#import pull_tickdata as pl
import pull_tickdata2 as pl2
import tick_data_combine_dates_hdf as comb
import compress_data as compress
import check_error_file as chk
import zipped_data_to_hdf as ziphdf
#import shutil as shutil #moveing files
#import re # regular expressions
#import multiprocessing
import google_data_pull as ggl
import pull_tick_data_parallel as plparra


#multiprocessing.freeze_support()
#pl.pull_tickdata(['ETF', 'SPX', 'NYSE', 'AMEX', 'NASDAQ'],  mktdata='all')

pl2.pull_tickdata(['ETF', 'SPX', 'NYSE', 'AMEX', 'NASDAQ'],  mktdata='combined')
directories = ['D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\ETF',
               'D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\SPX',
               'D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined']
comb.tick_data_combine_dates_multi(directories=directories)
##comb.tick_data_combine_dates_multi(['TVLYQ.O'])
compress.compress_data_directory(directories, 'bz2')
##pl2.pull_tickdata(['ETF'],  mktdata='combined')
##ziphdf.zip_data_to_hdf_multi_ticker(directories,'bz2')
chk.check_error_file()
#
ndays=ggl.get_google_days('VIX')
content=ggl.get_quote(symbol='VIX',interval='60',period=ndays)
ndays=ggl.get_google_days('SPX') 
content=ggl.get_quote(symbol='SPX',interval='60',period=ndays)

#
#direct = 'D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\SPX'
#comb.tick_data_combine_dates_multi(directory=direct)

#if __name__=='__main__':
#    start1 = time.time()
#    plparra.setup_parallel(toget=['ETF'])
#    print str((time.time()-start1)/60)
#    start2 = time.time()
#    pl2.pull_tickdata(['ETF'],  mktdata='combined')
#    print 'start1='+str((start2-start1)/60)+ '   start2='+str((time.time()-start2)/60)

