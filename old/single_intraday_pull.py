# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 12:30:45 2014

@author: Dushan
"""

    
def reporthook(a,b,c): 
    # ',' at the end of the line is important!
    print "% 3.1f%% of %d bytes\r" % (min(100, float(a * b) / c * 100), c),

def single_intraday_pull(TCKR,date,extract='all'):
    """
    pulls intraday data, for one day, from netfonds.com
    """
    import pandas as pd
    import datetime as dt
    from urllib import urlretrieve as ul
    import os
    import numpy as np   
    import requests
    import csv
    #from urllib2 import URLError
    import time
    
    if type(date)!= type(pd.datetime(1,1,1).date()):
        print "Error:date is not datetime.datetime type"
        return 1
    
    datestr = date.strftime('%Y%m%d')    
    #time0=time.time()
    if(extract=='all' or extract=='position'):     
        url = ('http://hopey.netfonds.no/posdump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')        

        df = pd.read_csv(url %TCKR)

        head='time,bid,bid_depth,bid_depth_total,offer,offer_depth,offer_depth_total'.split(',')
        if not (head==df.columns.values).all():
            print "Error: problem pulling data from website on date="+ datestr
            return 1
        elif len(df)==0:
            print "Market Closed or No Data on date="+ datestr
            return 0
        
        TCKRp = df
        TCKRp['time'] = pd.to_datetime(TCKRp['time'], format='%Y%m%dT%H%M%S')
        
        TCKRp = TCKRp.drop_duplicates()
        TCKRp['daysecs'] = TCKRp['time'].map(lambda x: x.second+x.minute*60+x.hour*60*60)          
        TCKRp['date'] = TCKRp['time'].map(lambda x: x.date()) 
        TCKRp.index = TCKRp['time']; del TCKRp['time']
        #print 'time positions='+str(time.time()-time0)
        #time0=time.time()
        
    if (extract=='all' or extract=='trades'):
        url = ('http://hopey.netfonds.no/tradedump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')     
        df = pd.read_csv(url %TCKR)
        head='time,price,quantity,board,source,buyer,seller,initiator'.split(',')
        if not (head==df.columns.values).all():
            print "Error: problem pulling data from website on date="+ datestr
            return 1
        elif len(df)==0:
            print "Market Closed or No Data on date="+ datestr
            return 0
        
        TCKRt = df 
        TCKRt['time'] = pd.to_datetime(TCKRt['time'], format='%Y%m%dT%H%M%S')
        
        TCKRt = TCKRt.drop_duplicates()
        TCKRt['daysecs'] = TCKRt['time'].map(lambda x: x.second+x.minute*60+x.hour*60*60)          
        TCKRt['date'] = TCKRt['time'].map(lambda x: x.date()) 
        TCKRt.index = TCKRt['time']; del TCKRt['time']
        #print 'time trades='+str(time.time()-time0)
        #time0=time.time()
        
        

        
        
        
#        url = ('http://hopey.netfonds.no/tradedump.php?date=' 
#                +datestr+'&paper=%s&csv_format=csv')        
#        ul(url %TCKR, 'TCKRt.csv')
#        #s = os.stat('TCKRt.csv')
#        f = open('TCKRt.csv')
#        firstline=f.readline()
#        secondline=f.readline()
#        f.close()
#        head='time,price,quantity,board,source,buyer,seller,initiator\n'
#        if firstline!=head:
#            print "Error: problem pulling data from website on date="+ datestr
#            return 1
#        elif secondline=='':
#            print "Market Closed or No Data on date="+ datestr
#            return 0
#        
#        TCKRt = pd.read_csv('TCKRt.csv')
#    
#        arr_t = np.array( ['time', 'price', 'quantity', 'board','source','buyer', 'seller','initiator'] )
#        if not(np.array_equal(TCKRt.columns.values, arr_t)):
#            print 'Error: data columns do not match. Date=' + datestr
#            return 1
#        
#        TCKRt = TCKRt.drop_duplicates(cols = 'time')
#        for i in TCKRt.index:
#            TCKRt['time'][i] = pd.datetime.strptime(TCKRt['time'][i],'%Y%m%dT%H%M%S')
#        TCKRt['daysecs']= TCKRt['time'].map(lambda x: x.second+x.minute*60+x.hour*60*60)
#        TCKRt['date'] = TCKRt['time'].map(lambda x: dt.datetime(x.year,x.month,x.day))               
#        TCKRt.index = TCKRt['time']; del TCKRt['time']
#        os.remove('TCKRt.csv')
        
    
    if (extract=='all'):
        mktopen = TCKRt.groupby([TCKRt.index.year,TCKRt.index.month,TCKRt.index.day]).min()['daysecs']
        mktclose = TCKRt.groupby([TCKRt.index.year,TCKRt.index.month,TCKRt.index.day]).max()['daysecs']
        TCKRb = pd.concat([TCKRp, TCKRt]).sort_index()
        TCKRb['timeopen']= TCKRb['price'].map(lambda x: mktopen[0])
        TCKRb['timeclose']= TCKRb['price'].map(lambda x: mktclose[0])
        return (TCKRp,TCKRt,TCKRb)
    elif (extract=='position'):
        return TCKRp
    else:
        return TCKRt
        
    


    