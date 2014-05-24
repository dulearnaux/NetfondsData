# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 13:51:15 2014

@author: Dushan
"""


"""
change 'start' and 'end' to 'date' argumet to be: dates=1 as default. put if statement to set dates to range of ~15 business days
'dates' is checked whether its a range of dates and that min date is less than today. delete dates >= today.
"""

import pandas as pd  
import single_intraday_pull_write as sin
import datetime as dt


def multi_intraday_pull2(TCKR,start=None,end=None, maxiter=30,extract='all', folder='', directory=''):
    """
    pulls intraday data, for multiple days, from netfonds.com
    """

    
    if (start!=None):
        if (type(start)!=type(pd.datetime(1,1,1).date())):
            print "Error:start is not typedatetime.datetime"
            return 1      
    if (end!=None):
        if (type(end)!=type(pd.datetime(1,1,1).date())):
            print "Error:end is not typedatetime.datetime"
            return 1        
    if (start==None and end != None):
        print "Error: must enter 'start' if you have entered 'end'"
        return 1
    if (start==None and end==None):
        end = pd.datetime.now()
        start = pd.datetime.min
    if (start!=None and end==None):
        end = pd.datetime.now()
        print "set end="+end.strftime('%Y%m%d')
    
    
    date = end    
    retcode=0
    i=0

    #run the loops to pull data on each day 
    while (retcode !=1 and date>=start):
        i+=1
        temp = sin.single_intraday_pull_write(TCKR,date,extract, folder, directory)   
        
        #check that an error code was not returned
        if (type(temp) == int):
            retcode = temp
            print TCKR +": singl_intraday_pull failed for iter="+str(i)+" and date="+date.strftime('%Y%m%d')
            date = date - dt.timedelta(days=1) 
            i-=1
            continue
            
        print TCKR +": succesfully pulled iter=" +str(i) + " for date="+date.strftime('%Y-%m-%d')
    
        date = date - dt.timedelta(days=1)
        
    return str(i)
     