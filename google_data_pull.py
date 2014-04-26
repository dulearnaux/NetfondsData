# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 17:52:16 2014

@author: Dushan
"""
import urllib
import re
import time
import pandas as pd
import os
import datetime as dt
import matplotlib.pyplot as plt 


def get_quote(symbol='VIX',xchange='',interval='1',sessions='ext_hours', period='100d',fields='d,c,v,o,h,l'):
    base_url = 'http://www.google.com/finance/getprices?'

    if period == '0d':
        print 'GOOGLE FINANCE:'+symbol+': already got the data'
        return "didnt't run"
        
    if ((pd.datetime.today().hour>=8) and (pd.datetime.today().hour<=18)):
        print symbol+': cant use google finance during market hours'
        return        
        
    customURL=''
    if symbol !='':
        customURL += 'q='+symbol
    if xchange != '':
        customURL += '&x='+xchange
    if interval != '':
        customURL += '&i='+interval
    if sessions != '':
        customURL += '&sesseions='+sessions
    if period != '':
        customURL += '&p='+period
    if fields != '':
        customURL += '&f='+fields
    
    n=1
#    start=time.time()
#    for i in range(n):
#            urllib.urlretrieve(base_url+customURL,'D:\\Financial Data\\Google\\temp.csv')    
#            df = pd.read_csv('D:\\Financial Data\\Google\\temp.csv')
#            os.remove('D:\\Financial Data\\Google\\temp.csv')
#    dur1 = time.time()-start        
#    print 't1='+str(dur1)

    start=time.time()    
    for i in range(n):    
        content = urllib.urlopen(base_url + customURL).read()
        
        #SEARCH FOR COLUMN NAMES
        tempstr = re.search('COLUMNS=[A-Z,=]+?\n',content)
        if tempstr==None:
            print "No match found for 'columns=...\n' in contents"
        else:
            colnames = (tempstr.group().replace('COLUMNS=','').replace('\n','')).split(',')
            
        #SEARCH FOR INTERVAL
        tempstr = re.search('INTERVAL=[0-9]+?\n',content)
        if tempstr==None:
            print "No match found for 'interval=[num]\n' in contents"
        else:
            intval = (tempstr.group().replace('INTERVAL=','').replace('\n','')).split(',')   
        
        #SEARCH FOR TIMEZONE OFFSET
        tempstr = re.search('TIMEZONE_OFFSET=\-*[0-9]+?\n',content)
        if tempstr==None:
            print "No match found for 'TIMEZON_OFFSET=[num]\n' in contents"
        else:
            TZoffset = (tempstr.group().replace('TIMEZONE_OFFSET=','').replace('\n','')).split(',')   
 
        #SEARCH FOR MARKET_OPEN_MINUTE
        tempstr = re.search('MARKET_OPEN_MINUTE=[0-9]+?\n',content)
        if tempstr==None:
            print "No match found for 'MARKET_OPEN_MINUTE=[num]\n' in contents"
        else:
            mkt_open = (tempstr.group().replace('MARKET_OPEN_MINUTE=','').replace('\n','')).split(',') 
            
        #SEARCH FOR MARKET_CLOSE_MINUTE
        tempstr = re.search('MARKET_CLOSE_MINUTE=[0-9]+?\n',content)
        if tempstr==None:
            print "No match found for 'MARKET_CLOSE_MINUTE=[num]\n' in contents"
        else:
            mkt_close = (tempstr.group().replace('MARKET_CLOSE_MINUTE=','').replace('\n','')).split(',') 
         
        #ISOLATE DATA
        tempstr = re.search('\na[0-9]+,[.\n]*',content)
        if tempstr==None:
            print "No match found for 'MARKET_CLOSE_MINUTE=[num]\n' in contents"
        else:
            data = content[tempstr.start():] 
            tempstr = re.findall('\na[0-9]+?,',data)
            for i in range(len(tempstr)):
                tempstr[i] = int(tempstr[i].replace('\na','').replace(',',''))
                
            POSIXdays=tempstr
            daytimes =[]
            for day in POSIXdays:
                daytimes.append(dt.datetime.fromtimestamp(day))
            POSIXdays.append(0) #so we can go through the loop below without exceeding index
        
        
        #go through 'data' and create dataframe
        datalines = data.split('\n')
        df = pd.DataFrame(columns=colnames) 
        dfline = pd.DataFrame(columns=colnames)
        j=0
        daycount=0
        Index=[]
        for i in range(len(datalines)):                        
            if datalines[i]=='':
                j+=1                
                continue
            line = datalines[i].split(',')
            #df.index[i-j]=i-j
            if line[0]=='a'+str(POSIXdays[daycount]):                                
                tempdate= daytimes[daycount]
                line[0]=tempdate.date()
                daycount+=1
            elif line[0].startswith('TIMEZONE'):
                j+=1
                continue
            else:
                tempdate = dt.timedelta(seconds=int(line[0])*int(intval[0]))+daytimes[daycount-1]
                line[0]=tempdate.date()
            if len(colnames)==len(line):
                line[1:]=map(float,line[1:]) #exclude 0, it is a date
                dfline = pd.DataFrame(line,index=colnames).transpose()  
                df = df.append(dfline)
                Index.append(tempdate)
            else:
                print 'Error:number of fields does not match, i='+ str(i)
             
        df.index = Index
        datestr=pd.datetime.today().date().strftime('%Y%m%d')
        df.to_csv('D:\\Financial Data\\Google\\'+symbol+'.'+datestr+'.csv')
        
        #append the new data to old date, removing duplicates        
        dfcombined = pd.read_csv('D:\\Financial Data\\Google\\'+symbol+'.combined.csv',index_col=0,header=0)
        dfcombined.index = pd.to_datetime(dfcombined.index) 
        dfcombined['DATE'] = pd.to_datetime(dfcombined['DATE'])
        dfcombined['DATE'] = dfcombined['DATE'].map(lambda x: x.date())
        dfcombined = dfcombined.append(df)
        dfcombined['index'] = dfcombined.index
        dfcombined = dfcombined.drop_duplicates() 
        del dfcombined['index']
        dfcombined.to_csv('D:\\Financial Data\\Google\\'+symbol+'.combined.csv')
        
    
#    m = re.search('id="ref_694653_l".*?>(.*?)<', content)
#    if m:
#        quote = m.group(1)
#    else:
#        quote = 'no quote available for: ' + symbol
    return content
    
def get_google_days(TCKR):
    path='D:\\Financial Data\\Google\\'+TCKR+'.combined.csv'
    if os.path.isfile(path):
        
        df = pd.read_csv(path,index_col=0,header=0) 
        latest_date=df[df.index==max(df.index)]['DATE']
        latest= pd.datetime.strptime(latest_date[0],'%Y-%m-%d')
        ndays = pd.datetime.today().date()-latest.date()
        return str(ndays.days) + 'd'
    else:
        return '100d'
    
if __name__=='__main__':
    ndays=get_google_days('VIX')
#    if ndays=='0d':
#        pass
#    else:
#        content=get_quote(interval='60',period=ndays)
    content=get_quote(interval='60',period=ndays)
    
    