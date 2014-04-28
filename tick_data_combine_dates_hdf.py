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

    #get list of files for ticker = TCKR
    files = getl.get_csv_file_list(TCKR, listdir, directory)
    if files==1:
        return 1

    """
    run case for no H5 file.
    if file doesn't exist, create it with the 1st csv data file, then close.    
    """
    if not(os.path.isfile(directory+'\\'+TCKR+'.combined.h5')):
        store = pd.HDFStore(directory+'\\'+TCKR+'.combined.h5')
        for fl in files: #find 1st file to create appendable h5 with, then break out of loop.
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
        
    
    
    """
    Now can run case where H5 file exists
    """
    store = pd.HDFStore(directory+'\\'+TCKR+'.combined.h5')

    #get list of existing dates in the HDF5 data store
    if len(store.dataframe)==0:
        olddates = []
    else:
        olddates= list(pd.Series(store.dataframe.index).map(pd.Timestamp.date).unique())
    
    #get list of files to read in
    for fl in files:
        if 'combined' in fl:
            continue
        date=pd.datetime.strptime(fl.replace('.csv','').replace(TCKR+'.',''),'%Y%m%d').date()
        if date in olddates:
            files.remove(fl)
        
    #read in the files to 'df'
    df = pd.DataFrame()
    for fl in files:
        temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
        if len(temp)==0:
            continue
        temp=temp[['bid', 'bid_depth', 'bid_depth_total', 'offer', 'offer_depth', 'offer_depth_total', 'price', 'quantity']]
        df = df.append(temp)
    
    if len(df)>0:
        #convert index to timeindex
        if type(df.index) != pd.tseries.index.DatetimeIndex:
            df.index = pd.to_datetime(df.index)  
         
        #clean and append to the H5 data store.        
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
    directories = ['D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\ETF']
    tick_data_combine_dates_multi(directories=directories)    
    
        