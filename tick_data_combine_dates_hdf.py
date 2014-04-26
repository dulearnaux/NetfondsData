# -*- coding: utf-8 -*-

import pandas as pd
import os 
import time
import Netfonds_Ticker_List as NTL
import get_lists as getl
  

def tick_data_combine_dates_single(TCKR, listdir, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Combines all tickdata files for the ticker in the directory, default = current.
    """
        
    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)
    #directory='D:\Financial Data\Netfonds\DailyTickDataPull\Combined'
    #get list of files for ticker = TCKR
    files = getl.get_file_list(TCKR, listdir, directory)
    if files==1:
        return 1

    if not(os.path.isfile(directory+'\\'+TCKR+'.combined.h5')):
        store = pd.HDFStore(directory+'\\'+TCKR+'.combined.h5')
        for fl in files:
            if 'combined' in fl:
                continue
            temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
            if len(temp)==0:
                continue
            temp=temp[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
            temp.index=pd.to_datetime(temp.index)
            temp = temp.sort_index()
            files.remove(fl)
            break
        store.append('dataframe', temp, format='table', complib='blosc', complevel=9,expectedrows=len(temp))
        store.close()
        
    store = pd.HDFStore(directory+'\\'+TCKR+'.combined.h5')
    if len(store.dataframe)==0:
        maxdate=pd.datetime.min.date()
    else:
        maxdate = store['dataframe'].index.max().date()
#    dfcombined = pd.read_hdf(directory+'\\'+TCKR+'.combined.h5','dataframe')
#    maxdate=dfcombined.ix[max(dfcombined.index)].index.date()
    df = pd.DataFrame()
    for fl in files:
        if 'combined' in fl:
            continue
        date=pd.datetime.strptime(fl.replace('.csv','').replace(TCKR+'.',''),'%Y%m%d').date()
        if date > maxdate:
            temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
            if len(temp)==0:
                continue
            temp=temp[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
            df = df.append(temp)
    
    if len(df)>0:
        if type(df.index) != pd.tseries.index.DatetimeIndex:
            df.index = pd.to_datetime(df.index)
            #print '%-8s:converted index to DatatimeIndex' %TCKR
    
        #check new index is greater than maxdate
        if df.index.min().date()<=maxdate:
            df=df.ix[df.index > (pd.Timestamp(maxdate)+pd.offsets.Day(1))]
            print TCKR + ' csv files have union data with h5 file'
        df['index'] = df.index    
        df = df.drop_duplicates()  
        del df['index']
        df = df.sort_index()         
        store.append('dataframe', df, format='table',  complib='blosc', complevel=9, expectedrows=len(df))
    store.close()
   
    os.chdir(start_dir)
    return 0
    
    
def tick_data_combine_dates_multi(TCKR=None, directories=None):
    """
    combines files across dates into single file for tickers in TCKR
    TCKR can represent an index (e.g. 'SPX', 'ETF', 'NYSE', 'NASDAQ', 'AMEX')
        * multiple incies must be passed as a list
    if TCKR is not passed, acts on all files in the directory
    if directory is not passed, act within the current directory
    """    
    start_dir = os.getcwd() # save starting directory so we can revert back at end of function
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')


    start=time.time()
    if directories==None:
        directories=[start_dir]
    if type(directories)!= list:
        directories = [directories]
        print 'converted directories input to a list'
    for directory in directories:
            
        if TCKR==None: #get list of tickers in the directory
            TCKR, listdir = getl.get_list_tickers_in_dir(directory)
        else:
            TCKR = NTL.get_netfonds_tickers(TCKR)
            TCKR = TCKR.ticker.values.tolist()
        i=0    
        for tckr in TCKR:
            i=i+1
            tick_data_combine_dates_single(tckr, listdir, directory)
            print '%-8s:dates combined, '%tckr +str(i)+' of '+str(len(TCKR)) + ', in time=%7.3f'%((time.time()-start)/60)+' mins'
         
        os.chdir(start_dir)
        TCKR=None
        print 'Combine dates completed after '+str((time.time()-start)/60) + ' mins'
        
    return
    
if __name__=='__main__':
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    directories = ['D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined']
    tick_data_combine_dates_multi(directories=directories)    
    
        