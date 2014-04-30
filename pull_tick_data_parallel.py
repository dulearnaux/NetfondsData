# -*- coding: utf-8 -*-
import pandas as pd
#import numpy as np
import os
os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
import multi_intraday_pull2 as mul
import Netfonds_Ticker_List as NTL   
import time as tm  
import multiprocessing
import sys
import time

"""
try using multiprocessing.Queue() to get ticker latest_dates as they get done
can also try writing to the ouput file
http://stackoverflow.com/questions/8329974/can-i-get-a-return-value-from-multiprocessing-process
http://stackoverflow.com/questions/11515944/how-to-use-multiprocessing-queue-in-python

if files in mproc are used, need to check if they exist, read them instead of normal location. 
this means interupptiosn will be handled.
need to delete mproc files at the end of the program.
"""


def setup_parallel(toget=['SPX','ETF'], mktdata='combined', n_process=3):
    
    
    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    #break up problem into thirds
    length = len(tickers)
    index=[]
    df_list=[]
    for i in range(n_process):
        index.append(range(i,length, n_process))               
        df = tickers.loc[index[i]] 
        df.index=range(len(df))
        df_list.append(df)

    jobs=[]
    start = time.time()
    for tickers in df_list:
        p = multiprocessing.Process(target=pull_tickdata_parallel, args=(tickers,'combined', length, start))
        jobs.append(p)
        p.start()
    
    for j in jobs:
        j.join()
        
    #combine files into single file

    
    
    
    
    


#def pull_tickdata_parallel(toget=['SPX','ETF'], mktdata='combined'):
def pull_tickdata_parallel(tickers, mktdata='combined',nTot=0,sTime=0):
    """
    pulls intraday data, for multiple days, for specified tickers, from netfonds.com
    """
    
    
    exper = 'temp\\' # used to change directory when experimenting with code.
    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'
#    start_time=tm.time()  
    mktdata=mktdata.lower() #convert to lower case
    
    #get todays date, but with time of day set to zero    
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    datestr = date.strftime('%Y%m%d')
    
#    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    log = pd.DataFrame()
    ndays = 18
    if os.path.isfile(directory+'\\latest_dates\\latest_dates.csv'):        
        log = pd.read_csv(directory+'\\latest_dates\\latest_dates.csv', index_col = 0, header=0)
        log['latest_date'] = pd.to_datetime(log['latest_date'])
        latest_date = log['latest_date']
        #log['ticker']=log.index
        #latest_date.index=log['ticker']
        latest_date.index=log.index        
        latest_default = date - pd.offsets.BDay(ndays) 
    else:
        S =  tickers
        S.index = tickers
        temp = pd.Series(len(tickers)*[date - pd.offsets.BDay(ndays) ], index= S.index)
        df = pd.concat([S,temp], axis=1)
        df.columns = ['ticker','latest_date']
        latest_date = df['latest_date'] 
        latest_default = date - pd.offsets.BDay(ndays)

    pName = multiprocessing.current_process().name    
    
    log_file_output = open(directory+'\\mproc\\logfile'+datestr+'.'+pName+'.txt','w')
    log_file_output2 = open(directory+'\\mproc\\logfile.'+pName+'+.txt','a')
    
    #i=0
    
        
    
    
    for i in tickers.index:
        #i +=1
        name = tickers['ticker'][i]
        folder=tickers['folder'][i]
        #name='AAPL.O'
        #get start date
        if (name in latest_date):
            start_date = (latest_date[name] + pd.offsets.BDay(1))
        else:
            start_date = latest_default
        if start_date>date:
            print pName+ ':Iteration='+str(i) +' : Already collected data for '+name
            sys.stdout.flush()            
            continue
        
        #pull intraday data from the web for the current stock or index
        #positions, trades, combined 
        data = mul.multi_intraday_pull2(name, pd.datetime.date(start_date), date.date(), 30,mktdata, folder, directory)
        
        '''do not alter latest date'''
        if name in latest_date:
            latest_date[name] = date #reset the latest date
        else:
            latest_date=latest_date.append(pd.Series(date, index=[name]))
            log_file_output.write(name+' added to the ticker list on '+datestr + '\n')
            log.to_csv(directory+'\\mproc\\latest_dates'+datestr+'.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])
            log.to_csv(directory+'\\mproc\\latest_dates.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])            
        tempstr = pName+ ": Daily files written: "+name +': Iter='+str(i) +' completed: Starts:ends='+ start_date.strftime('%Y-%m-%d')+':'+date.strftime('%Y-%m-%d')
        
        print tempstr
        print pName + ' :' +str(i)+' of '+str(len(tickers))+' complete in '+str((tm.time()-sTime)/60)+' minutes'
        log_file_output.write(tempstr + '\n')
        log_file_output2.write(tempstr + '\n')
        sys.stdout.flush()
    

    log_file_output.close()
    log_file_output2.close()
    log=latest_date
    log.to_csv(directory+'\\mproc\\latest_dates'+datestr+'.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])
    log.to_csv(directory+'\\mproc\\latest_dates.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])    

    return 
    
if __name__=='__main__':        
    ls=setup_parallel(toget=['ETF'], mktdata='combined', n_process=3)
    print 'hey'