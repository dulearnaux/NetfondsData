# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 13:51:15 2014

@author: Dushan
"""


"""
change 'start' and 'end' to 'date' argumet to be: dates=1 as default. put if statement to set dates to range of ~15 business days
'dates' is checked whether its a range of dates and that min date is less than today. delete dates >= today.
"""

def multi_intraday_pull(TCKR,start=None,end=None, maxiter=30,extract='all'):
    """
    pulls intraday data, for multiple days, from netfonds.com
    """
    import pandas as pd  
    import single_intraday_pull as sin
    import datetime as dt
    
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
        collect = 'all'
    
    if (start!=None and end==None):
        end = pd.datetime.now()
        collect = 'setstart'
        print "set end="+end.strftime('%Y%m%d')
    
    if (start!=None and end!=None):
        collect = 'setwindow'
    
    date = end    
    retcode=0
    i=0

#run the loops to pull data on each day 
    data,datap,datat,datab=pd.DataFrame() ,pd.DataFrame() ,pd.DataFrame() ,pd.DataFrame()   
    if (extract == 'all'): #case for 'all' returns a triple tupple  
        while (retcode !=1 and date>=start):
            i+=1
            temp = sin.single_intraday_pull(TCKR,date,extract)   
            
            #check that an error code was not returned
            if (type(temp) == int):
                retcode = temp
                print TCKR +": singl_intraday_pull failed for iter="+str(i)+" and date="+date.strftime('%Y%m%d')
                date = date - dt.timedelta(days=1) 
                continue
            else:
                tempp,tempt,tempb=temp[0],temp[1],temp[2]
                
            print TCKR +": succesfully pulled iter=" +str(i) + " for date="+date.strftime('%Y-%m-%d')
        
            if i==1:
                datap,datat,datab=tempp,tempt,tempb
            else:
                datap,datat,datab = datap.append(tempp),datat.append(tempt),datab.append(tempb)
                
            date = date - dt.timedelta(days=1)
            
        return datap,datat,datab
     
    else: #case for not 'all' returns a singe dataframe
        while (retcode !=1 and date>=start):
            i+=1
            temp = sin.single_intraday_pull(TCKR,date,extract)   
                
            if (type(temp) == int):
                retcode = temp           
                print "singl_intraday_pull failed for iter="+str(i)+" and date="+date.strftime('%Y%m%d')
                date = date - dt.timedelta(days=1) 
                continue
            
            print TCKR +": succesfully pulled iter=" +str(i) + " for date="+date.strftime('%Y-%m-%d')
        
            if i==1:
                data=temp
            else:
                data = data.append(temp)
                
            date = date - dt.timedelta(days=1)
            
        return data