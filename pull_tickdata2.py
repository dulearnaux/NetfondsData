# -*- coding: utf-8 -*-



def pull_tickdata(toget=['SPX','ETF'], mktdata='combined'):
    """
    pulls intraday data, for multiple days, for specified tickers, from netfonds.com
    """
    import pandas as pd
    #import numpy as np
    import os
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')
    import multi_intraday_pull2 as mul
    import Netfonds_Ticker_List as NTL   
    import time as tm  
    
    exper = ''#'temp\\' # used to change directory when experimenting with code.
    directory = 'D:\\Financial Data\\Netfonds\\'+exper +'DailyTickDataPull'
    start_time=tm.time()  
    mktdata=mktdata.lower() #convert to lower case
    
    #get todays date, but with time of day set to zero    
    date = pd.datetime.strptime(pd.datetime.now().strftime('%Y%m%d'),'%Y%m%d')  - pd.offsets.BDay(1)
    datestr = date.strftime('%Y%m%d')
    
    tickers = NTL.get_netfonds_tickers(toget) #get list of tickers from files or internet
    
    log = pd.DataFrame()
    ndays = 18
    if os.path.isfile(directory+'\\latest_dates\\latest_dates.csv'):        
        log = pd.read_csv(directory+'\\latest_dates\\latest_dates.csv', index_col = 0, header=0)
        log['latest_date'] = log['latest_date'].map(lambda x: pd.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
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
    
    log_file_output = open(directory+'\\logfiles\\logfile'+datestr+'.txt','w')
    log_file_output2 = open(directory+'\\logfiles\\logfile.txt','a')
    
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
        data = mul.multi_intraday_pull2(name, pd.datetime.date(start_date), date.date(), 30,mktdata, folder, directory)
        
        '''do not alter latest date'''
        if name in latest_date:
            latest_date[name] = date #reset the latest date
        else:
            latest_date=latest_date.append(pd.Series(date, index=[name]))
            log_file_output.write(name+' added to the ticker list on '+datestr + '\n')
        latest_date.to_csv(directory+'\\latest_dates\\latest_dates'+datestr+'.csv',header=['latest_date'], index_label=['ticker'])
        latest_date.to_csv(directory+'\\latest_dates\\latest_dates.csv', header=['latest_date'], index_label=['ticker'])
        
        tempstr = "Daily files written: "+name +': Iter='+str(i) +' completed: Starts:ends='+ start_date.strftime('%Y-%m-%d')+':'+date.strftime('%Y-%m-%d')
        
        print tempstr
        print str(i)+' of '+str(len(tickers))+' complete in '+str((tm.time()-start_time)/60)+' minutes'
        log_file_output.write(tempstr + '\n')
        log_file_output2.write(tempstr + '\n')
    

    log_file_output.close()
    log_file_output2.close()
    log=latest_date
    log.to_csv(directory+'\\latest_dates\\latest_dates'+datestr+'.csv', header=['latest_date'], index_label=['ticker'])
    log.to_csv(directory+'\\latest_dates\\latest_dates.csv', header=['latest_date'], index_label=['ticker'])    

    return 
