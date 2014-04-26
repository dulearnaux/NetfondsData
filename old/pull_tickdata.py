# -*- coding: utf-8 -*-



def pull_tickdata(toget=['SPX','ETF'], mktdata='all'):
    """
    pulls intraday data, for multiple days, for specified tickers, from netfonds.com
    """
    import pandas as pd
    #import numpy as np
    import os
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    import multi_intraday_pull as mul
    import Netfonds_Ticker_List as NTL   
    import time as tm  
    
    exper = 'temp\\' # used to change directory when experimenting with code.
    dataURL = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'
    start_time=tm.time()  
    mktdata=mktdata.lower() #convert to lower case
    
    #get todays date, but with time of day set to zero    
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    datestr = date.strftime('%Y%m%d')
    
    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    log = pd.DataFrame()
    ndays = 18
    if os.path.isfile(dataURL+'\\latest_dates.csv'):        
        log = pd.read_csv(dataURL+'\\latest_dates.csv', index_col = 0, header=0)
        log['latest_date'] = log['latest_date'].map(lambda x: pd.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        latest_date = log['latest_date']
        latest_default = date - pd.offsets.BDay(ndays) 
    else:
        S =  tickers
        S.index = tickers
        temp = pd.Series(len(tickers)*[date - pd.offsets.BDay(ndays) ], index= S.index)
        df = pd.concat([S,temp], axis=1)
        df.columns = ['ticker','latest_date']
        latest_date = df['latest_date'] 
        latest_default = date - pd.offsets.BDay(ndays)
    
    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull\\'
    log_file_output = open(directory+'logfile'+datestr+'.txt','w')
    log_file_output2 = open(directory+'logfile.txt','a')
    
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
            print 'Iteration='+str(i) +' : Already collected data for '+name
            continue
        
        #pull intraday data from the web for the current stock or index
        #positions, trades, combined 
        data = mul.multi_intraday_pull(name, pd.datetime.date(start_date), date.date(), 30,mktdata)
        
        #write individual data to disk
        if mktdata=='position' or mktdata=='all':
            data[0].to_csv('D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull\\Positions\\'+folder+name+'.'+date.strftime('%Y%m%d')+'.csv')
        if mktdata=='trades' or mktdata=='all':
            data[1].to_csv('D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull\\Trades\\'+folder+name+'.'+date.strftime('%Y%m%d')+'.csv') 
        if mktdata=='combined' or mktdata=='all':
            data[2].to_csv('D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull\\Combined\\'+folder+name+'.'+date.strftime('%Y%m%d')+'.csv')
        else:
            print "Error: unable to recognise mktdata type. Should be 'position','trades' or'all'"
            break
        
        latest_date[name] = date #reset the latest date
        latest_date.to_csv(dataURL+'\\latest_dates'+datestr+'.csv',header=True)
        latest_date.to_csv(dataURL+'\\latest_dates.csv', header=True)
        
        tempstr = "Daily files written: "+name +': Iteration='+str(i) +' completed: Data Starts:ends='+ start_date.strftime('%Y-%m-%d')+':'+date.strftime('%Y-%m-%d')
        
        print tempstr
        print str(i)+' of '+str(len(tickers))+' complete in '+str((tm.time()-start_time)/60)+' minutes'
        log_file_output.write(tempstr + '\n')
        log_file_output2.write(tempstr + '\n')
    

    log_file_output.close()
    log_file_output2.close()
    log=latest_date
    log.to_csv(dataURL+'\\latest_dates'+datestr+'.csv', header=True)
    log.to_csv(dataURL+'\\latest_dates.csv', header=True)    

    return 




    