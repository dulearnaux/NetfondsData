# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 12:37:58 2014

@author: Dushan
"""


import pandas as pd
import os  


def get_list_tickers_in_dir(directory=None):
    """
    gets teh list of tickers in the directory
    """
    import os
    import re
    start_dir = os.getcwd()

    if directory == None:
        directory = start_dir
        
    listdir=os.listdir(directory)
    TCKRS=[]
    for ls in listdir:
        if not ls.endswith('.csv'):
            continue
        
        check = re.match("[A-Z]*?\.[A-Z]",ls)
        if check==None:
            continue
        TCKRS.append(check.group())
    
    #remove duplicates
    TCKRS = list(set(TCKRS))
    os.chdir(start_dir)
    return TCKRS    
    

def get_file_list(TCKR, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Returns the list of files with ticker=TCKR
    """
    import os
    import re
    
    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)
    
    listdir = os.listdir(directory)
    ls = str(listdir)
    
    #search for single run files    
    check = re.search('\''+TCKR+'combined.h5\', ',ls)
    if check == None:
        print 'Error: ticker='+TCKR+' not found in:'
        print directory
        return 1
    files=[]
    while check !=None:
        s = str(check.group())
        s=s.replace("'","")
        s=s.replace(", ","")
        files.append(s)
        ls=ls.replace(check.group(),'xxxxx, ',1)            
        check = re.search('\''+TCKR+'combined.h5\', ',ls)     
    
    #search for combined files
    check = re.search('\''+TCKR+'\.combined.csv\', ',ls)
    if check !=None:
        s = str(check.group())
        s=s.replace("'","")
        s=s.replace(", ","")
        files.append(s)

    os.chdir(start_dir)
    return files
    

def tick_data_convert_dates_single(TCKR, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Combines all tickdata files for the ticker in the directory, default = current.
    """
    
    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)
    
    #get list of files for ticker = TCKR
    files = os.path.isfile(TCKR+'.combined.h5')
    if not files:
        print 'Error: '+  TCKR+'.combined.h5'  + '  not found'      
        return 1
    size1 = os.path.getsize(TCKR+'.combined.h5')
 
    df = pd.read_hdf(TCKR+'.combined.h5', 'dataframe')
    os.remove(TCKR+'.combined.h5')
    
#    if 'time'in df.columns.values:
#        df.index = pd.to_datetime(df['time'])
#        del df['time']
#        print TCKR + ' deleted time'
#    if 'daysecs' in df.columns.values:
#        del df['daysecs']
#        print TCKR + ' deleted daysecs'
#    if 'timeopen' in df.columns.values:
#        del df['timeopen']
#        print TCKR + ' deleted timeopen'
#    if 'timeclose' in df.columns.values:
#        del df['timeclose']
#        print TCKR + ' deleted timeclose'
#    if 'date' in df.columns.values:
#        del df['date']
#        print TCKR + ' deleted date'
#
#    df.index = pd.to_datetime(df.index)
#    print TCKR + ' converted index to timeseries'
    
    store = pd.HDFStore(TCKR+'.combined.h5')
    store.append('dataframe', df, format='table', complib='blosc', complevel=9, expectedrows=len(df))
    store.close()
    #df.to_hdf(TCKR+'.combined.h5', 'dataframe', mode='w',format='table',complib='blosc', complevel=9)
    size2 = os.path.getsize(TCKR+'.combined.h5')
    print TCKR + 'wrote to hdf file. size change=' +str(float(size2)/float(size1))
    
    df2=pd.read_hdf(TCKR+'.combined.h5', 'dataframe')
    (df2==df).all()
    if (df2.index==df.index).all():
        print TCKR + ' Indexes match!'
  
    os.chdir(start_dir)
    return 0
    
    
def tick_data_convert_dates_multi(TCKR=None, directory=None):
    """
    combines files across dates into single file for ticker=TCKR
    TCKR can represent an index (e.g. 'SPX', 'ETF', 'NYSE', 'NASDAQ', 'AMEX')
        * multiple incies must be passed as a list
    if TCKR is not passed, acts on all files in the directory
    if directory is not passed, act within the current directory
    """    
    
    import os
    start_dir = os.getcwd() # save starting directory so we can revert back at end of function
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    import Netfonds_Ticker_List as NTL
    import time
    


    if directory==None:
        directory=start_dir
        
    if TCKR==None: #get list of tickers in the directory
        TCKR = get_list_tickers_in_dir(directory)
    else:
        TCKR = NTL.get_netfonds_tickers(TCKR)
    i=0    
    for tckr in TCKR:
        i=i+1
        start = time.time()
        tick_data_convert_dates_single(tckr, directory)
        print 'Completed ticker='+tckr+', '+str(i)+' of '+str(len(TCKR)) +' time='+ str(time.time()-start)
     
    os.chdir(start_dir)
    return
    
if __name__ =='__main__':
    import time
    directory = 'D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined'
    start = time.time()
    tick_data_convert_dates_multi(None, directory)
    print 'DONE, time=' +str(time.time()-start) 

  