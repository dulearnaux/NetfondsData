# -*- coding: utf-8 -*-
import pandas as pd
#import numpy as np
import os
os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
import multi_intraday_pull2 as mul
import Netfonds_Ticker_List as NTL   
import multiprocessing
import sys
import time

"""
try using multiprocessing.Queue() to get ticker latest_dates as they get done
can also try writing to the ouput file
http://stackoverflow.com/questions/8329974/can-i-get-a-return-value-from-multiprocessing-process
http://stackoverflow.com/questions/11515944/how-to-use-multiprocessing-queue-in-python

"""


def setup_parallel(toget=['SPX','ETF'], mktdata='combined', n_process=3):
    
    #some args for the write file
    exper = 'temp\\' # used to change directory when experimenting with code.
    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    datestr = date.strftime('%Y%m%d')
    
    #get list of tickers
    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    #break up problem into thirds, or number of processes
    length = len(tickers)
    index=[]
    df_list=[]
    for i in range(n_process):
        index.append(range(i,length, n_process)) 
#        index.append(range(i,6, n_process))               
        df = tickers.loc[index[i]] 
        df.index=range(len(df))
        df_list.append(df)
    
    queue = multiprocessing.Queue()
    start = time.time()
    
    #read in latest_dates
    latest_dates_df = pd.read_csv(directory+'\\latest_dates\\latest_dates.csv', index_col = 0, header=0)
    latest_dates_df['latest_date'] = pd.to_datetime(latest_dates_df['latest_date'])  
    print 'Read Latest_dates using pd.read_csv'    
    
    #start the writing file process
    w = multiprocessing.Process(target=write_latest_dates, args=(queue,latest_dates_df, directory, date, datestr, length))    
    w.start()  
    
    #start the pull data processes
    jobs=[]
    for tickers in df_list:
        p = multiprocessing.Process(target=pull_tickdata_parallel, args=(queue, tickers,latest_dates_df, 'combined', length, start, directory))
        jobs.append(p)
        p.start()
    
    for j in jobs:
        j.join()
        
    print 'Joined other threads'
    queue.put('DONE')  #end the while loop in process 'w'
    w.join() #wait for join to happen
    print 'Joined the write thread'
    
    
    
def write_latest_dates(queue,latest_dates_df, directory, date, datestr, length):
    print 'Entered write_lates_dates function'    
    
#    exper = 'temp\\' # used to change directory when experimenting with code.
#    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'

#    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
#    datestr = date.strftime('%Y%m%d')
    
#    latest_dates_d = {}
        
    log_file_output = open(directory+'\\logfiles\\logfile'+ datestr +'.txt','w')
    log_file_output2 = open(directory+'\\logfiles\\logfile.txt','a')
    i=0
    while True:
        ret = queue.get()         

        if (type(ret) == tuple):
            i = i+1
            msg, tempstr = ret
#            print 'IN WRITE_FILES: %s' %msg            
#            latest_dates_d.update(msg)
            if msg.keys()[0] in latest_dates_df.index:
                latest_dates_df.ix[msg.keys()[0]]=msg.values()[0]
            else:
                latest_dates_df.set_value(index=msg.keys()[0], col='latest_date',value=msg.values()[0])
                print 'Added %s to latest_date file' %msg.keys()[0]
                
#            latest_dates_df.to_csv(directory+'\\latest_dates\\latest_dates.csv', cols=['latest_date'], index_label=['ticker'])            
            latest_dates_df.to_csv(directory+'\\latest_dates\\latest_dates.csv')            
            ind = tempstr.index('Iter=')
            tempstr=tempstr.replace(tempstr[ind:(ind+10)], 'Iter=%5d of %5d'%(i,length) )            
            print tempstr
            sys.stdout.flush()
            
            log_file_output.write(tempstr + '\n')
            log_file_output2.write(tempstr + '\n')            
            del msg[msg.keys()[0]]
            
        elif (ret == 'DONE'): #'DONE' is passed to queue from the main function when the data pull processed join()
            break
    
        else:
            print 'Error: ret from queue not as ecpected'
            print ret
            break
    
    log_file_output.close()
    log_file_output2.close()  
    return #latest_dates_d
    


#def pull_tickdata_parallel(toget=['SPX','ETF'], mktdata='combined'):
def pull_tickdata_parallel(queue, tickers, latest_date, mktdata='combined',nTot=0,sTime=0, directory=''):
    """
    pulls intraday data, for multiple days, for specified tickers, from netfonds.com
    """ 
#    exper = 'temp\\' # used to change directory when experimenting with code.
#    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'
#    start_time=time.time()  
    mktdata=mktdata.lower() #convert to lower case
    
    #get todays date, but with time of day set to zero    
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
#    datestr = date.strftime('%Y%m%d')
    
#    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
#    log = pd.DataFrame()
    ndays = 18
#    if os.path.isfile(directory+'\\latest_dates\\latest_dates.csv'):        
#        log = pd.read_csv(directory+'\\latest_dates\\latest_dates.csv', index_col = 0, header=0)
#        log['latest_date'] = pd.to_datetime(log['latest_date'])
#        latest_date = log['latest_date']
#        latest_date.index=log.index        
#        latest_default = date - pd.offsets.BDay(ndays) 
#    else:
#        S =  tickers
#        S.index = tickers
#        temp = pd.Series(len(tickers)*[date - pd.offsets.BDay(ndays) ], index= S.index)
#        df = pd.concat([S,temp], axis=1)
#        df.columns = ['ticker','latest_date']
#        latest_date = df['latest_date'] 
#        latest_default = date - pd.offsets.BDay(ndays)

    pName = multiprocessing.current_process().name    
    
#    log_file_output = open(directory+'\\mproc\\logfile'+datestr+'.'+pName+'.txt','w')
#    log_file_output2 = open(directory+'\\mproc\\logfile.'+pName+'+.txt','a')

    for i in tickers.index:
        name = tickers['ticker'][i]
        folder=tickers['folder'][i]
        #get start date
        if (name in latest_date.index):
            start_date = (latest_date.latest_date.ix[name] + pd.offsets.BDay(1))
        else:
            start_date = date - pd.offsets.BDay(ndays)
            
        if start_date>date:
            print pName+ ':Iteration='+str(i) +' : Already collected data for '+name
            sys.stdout.flush()            
            continue
        
        #pull intraday data from the web for the current stock or index
        #positions, trades, combined 
        data = mul.multi_intraday_pull2(name, pd.datetime.date(start_date), date.date(), 30,mktdata, folder, directory)
        
        '''do not alter latest date'''
#        if name in latest_date:
#            latest_date[name] = date #reset the latest date
#        else:
#            latest_date=latest_date.append(pd.Series(date, index=[name]))
#            log_file_output.write(name+' added to the ticker list on '+datestr + '\n')
#        log.to_csv(directory+'\\mproc\\latest_dates'+datestr+'.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])
#        log.to_csv(directory+'\\mproc\\latest_dates.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])          
            
        print pName+ ": Daily files written: "+name +': Iter=%5d'%i +' completed: Starts:ends='+ start_date.strftime('%Y-%m-%d')+':'+date.strftime('%Y-%m-%d')
        tempstr = pName + ': Iter=%5d'%i+' complete in %5.2f min'%((time.time()-sTime)/60)        
        to_pass = ({name:date}, tempstr)
        queue.put(to_pass)  
#        print tempstr
#       print pName + ': Iter=%5d'%i+' of '+str(len(tickers))+' complete in '+str((time.time()-sTime)/60)+' minutes'
#        log_file_output.write(tempstr + '\n')
#        log_file_output2.write(tempstr + '\n')
        sys.stdout.flush()
    

#    log_file_output.close()
#    log_file_output2.close()
#    log=latest_date
#    log.to_csv(directory+'\\mproc\\latest_dates'+datestr+'.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])
#    log.to_csv(directory+'\\mproc\\latest_dates.'+pName+'.csv', header=['latest_date'], index_label=['ticker'])    

    return 
    
if __name__=='__main__':        
    ls=setup_parallel(toget=['ETF'], mktdata='combined', n_process=6)
    print 'hey'