# -*- coding: utf-8 -*-

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
    check = re.search('\''+TCKR+'\.[0-9]*\.csv\', ',ls)
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
        check = re.search("\'"+TCKR+"\.[0-9]*\.csv\', ",ls)     
    
    #search for combined files
    check = re.search('\''+TCKR+'\.combined.csv\', ',ls)
    if check !=None:
        s = str(check.group())
        s=s.replace("'","")
        s=s.replace(", ","")
        files.append(s)

    os.chdir(start_dir)
    return files
    

def tick_data_combine_dates_single(TCKR, directory=None):
    """
    Input: single ticker in format 'TICKER.X', where X is netfonds exchange letter (N:NYSE,O:NASDAQ,A:AMEX)
    Combines all tickdata files for the ticker in the directory, default = current.
    """

    start_dir = os.getcwd() #save start dir so we can revert back at the end of program
    if directory==None:
        directory = start_dir
    
    os.chdir(directory)
    
    #get list of files for ticker = TCKR
    files = get_file_list(TCKR, directory)
    if files==1:
        return 1

    df = pd.DataFrame()
    for fl in files:
        temp = pd.read_csv(directory+'\\'+fl, header=0, index_col=0)
        df=df.append(temp)
  

    if 'time'in df.columns.values:
        df.index = pd.to_datetime(df['time'])
        del df['time']
        print TCKR + ' deleted time'
    if 'daysecs' in df.columns.values:
        del df['daysecs']
        print TCKR + ' deleted daysecs'
    if 'timeopen' in df.columns.values:
        del df['timeopen']
        print TCKR + ' deleted timeopen'
    if 'timeclose' in df.columns.values:
        del df['timeclose']
        print TCKR + ' deleted timeclose'
    if 'date' in df.columns.values:
        del df['date']
        print TCKR + ' deleted date'
    if 'board' in df.columns.values:
        del df['board']
        print TCKR + ' deleted board'
    if 'buyer' in df.columns.values:
        del df['buyer']
        print TCKR + ' deleted buyer'
    if 'initiator' in df.columns.values:
        del df['initiator']
        print TCKR + ' deleted initiator'
    if 'seller' in df.columns.values:
        del df['seller']
        print TCKR + ' deleted seller'
    if 'source' in df.columns.values:
        del df['source']
        print TCKR + ' deleted source'

    df=df.drop_duplicates() 
    df.index = pd.to_datetime(df.index)
    print TCKR + ' converted index to timeseries'


    df.to_hdf(directory+'\\'+TCKR+'.combined.h5',  'dataframe',mode='w',format='table',complib='blosc', complevel=9)
    
    os.chdir(start_dir)
    return 0
    
    
def tick_data_combine_dates_multi(TCKR=None, directory=None):
    """
    combines files across dates into single file for ticker=TCKR
    TCKR can represent an index (e.g. 'SPX', 'ETF', 'NYSE', 'NASDAQ', 'AMEX')
        * multiple incies must be passed as a list
    if TCKR is not passed, acts on all files in the directory
    if directory is not passed, act within the current directory
    """    
    import pandas as pd
    import os
    start_dir = os.getcwd() # save starting directory so we can revert back at end of function
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    import Netfonds_Ticker_List as NTL
    


    if directory==None:
        directory=start_dir
        
    if TCKR==None: #get list of tickers in the directory
        TCKR = get_list_tickers_in_dir(directory)
    else:
        TCKR = NTL.get_netfonds_tickers(TCKR)
    i=0    
    for tckr in TCKR:
        i=i+1
        tick_data_combine_dates_single(tckr, directory)
        print 'Completed ticker='+tckr+', '+str(i)+' of '+str(len(TCKR))
     
    os.chdir(start_dir)
    return
    
    
    
        
        
        
    