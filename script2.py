# -*- coding: utf-8 -*-



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
import tick_data_combine_dates_hdf_parallel as comb_mp
import compress_data as compress
import compress_data_parallel as compress_mp
import check_error_file as chk
import zipped_data_to_hdf as ziphdf
#import shutil as shutil #moveing files
#import re # regular expressions
#import multiprocessing
import google_data_pull as ggl
import pull_tick_data_parallel as pull_parra
import Netfonds_Ticker_List as NTL 




datapull=True
hdfcreate=False
tarcreate=True

if __name__=='__main__':
    
    exper = ''#'\\temp'
#    exper = '\\temp'
    directories = ['D:\\Financial Data\\Netfonds%s\\DailyTickDataPull\\Combined\\ETF'%exper,
                   'D:\\Financial Data\\Netfonds%s\\DailyTickDataPull\\Combined\\SPX'%exper,
                   'D:\\Financial Data\\Netfonds%s\\DailyTickDataPull\\Combined'%exper]
#    directories = ['D:\\Financial Data\\Netfonds%s\\DailyTickDataPull\\Combined\\ETF'%exper]
                   
    baseDir = 'D:\\Financial Data\\Netfonds%s\\DailyTickDataPull'%exper

    #get list of tickers
    toget = ['ETF','SPX','NYSE','AMEX','NASDAQ']
#    toget = ['ETF']
    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    start1 = time.time()
    if datapull:
        pull_parra.setup_parallel(tickers, mktdata='combined', n_process=16, baseDir =baseDir, supress='yes')
    start2=time.time()
    if hdfcreate:
        comb_mp.combine_dates_multi_process_wrapper(directories=directories, n_process=4   )
    #    comb_mp.combine_dates_multi_process_wrapper(TICKERs=['GLD.A'], n_process=1   )    
    start3=time.time()
    if tarcreate:
        compress_mp.compress_tickers_parallel(tickers=None, directories=directories, compression='bz2', complevel=9, n_process=3)
    end = time.time()
#    chk.check_error_file()
    
    ndays=ggl.get_google_days('VIX')
    content=ggl.get_quote(symbol='VIX',interval='60',period=ndays)
    ndays=ggl.get_google_days('SPX') 
    content=ggl.get_quote(symbol='SPX',interval='60',period=ndays)
    ndays=ggl.get_google_days('VXV')
    content=ggl.get_quote(symbol='VXV',interval='60',period=ndays)
    
    print 'TOTAL TIME TO RUN'
    print 't_pull=%5.1f  t_comb=%5.1f  t_comp=%5.1f  t_total=%5.1f'%((start2-start1)/60,(start3-start2)/60,(end-start3)/60,(end-start1)/60)  

##    ziphdf.zip_data_to_hdf_multi_ticker(directories,'bz2')
