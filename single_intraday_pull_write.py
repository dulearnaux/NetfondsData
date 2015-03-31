# -*- coding: utf-8 -*-
"""
Created on Thu Mar 20 12:30:45 2014

@author: Dushan
"""
import pandas as pd
import urllib2
import socket
import time
import io
import httplib
import sys
    
def reporthook(a,b,c): 
    # ',' at the end of the line is important!
    print "% 3.1f%% of %d bytes\r" % (min(100, float(a * b) / c * 100), c),

def single_intraday_pull_write(TCKR,date,extract='all', folder='', directory=''):
    """
    pulls intraday data, for one day, from netfonds.com
    """
    
    if type(date)!= type(pd.datetime(1,1,1).date()):
        print "Error:date is not datetime.datetime type"
        return 1
    
    datestr = date.strftime('%Y%m%d')    
    #time0=time.time()
    
    urls = []
    results = []
    if(extract=='all' or extract=='position' or extract=='combined'):     
        urls.append('http://hopey.netfonds.no/posdump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')
    if (extract=='all' or extract=='trades' or extract=='combined'):
        urls.append('http://hopey.netfonds.no/tradedump.php?date=' 
                +datestr+'&paper=%s&csv_format=csv')             
                
    for url in urls:
        urlread=0
        while urlread==0:   
            start=time.time()
            try:     
                sys.stdout.flush()
                buff = urllib2.urlopen(url %TCKR) 
                csvstring = buff.read()
                if 'This stock does not exist' in csvstring:
                    print TCKR+': This stock no longer exists'
                    urlread=1
                    break
                if csvstring =='': #no position data, move on to next url
                    break 
                else:
                    df = pd.read_csv(io.BytesIO(csvstring))
                    head_pos='time,bid,bid_depth,bid_depth_total,offer,offer_depth,offer_depth_total'
                    head_trade='time,price,quantity,board,source,buyer,seller,initiator'
                    df_names=','.join(df.columns.values)
                    if not ((head_pos==df_names) or (head_trade==df_names)):
                        print "Error: problem pulling data from website on date="+ datestr
                        break
                    elif len(df)==0:
                        print "Market Closed or No Data on date="+ datestr
                        break
                    
                    df['time'] = pd.to_datetime(df['time'], format='%Y%m%dT%H%M%S')    
                    df = df.drop_duplicates()
                    df.index = df['time']; del df['time']
                    results.append(df)
                    urlread=1
            except urllib2.URLError, e:
                print TCKR + ' OOPS: timout error 1' + ' time='+str(time.time()-start)
                print e
            except socket.timeout, e:
                print TCKR + ' OOPS: timeout error 2' + ' time='+str(time.time()-start)
                print e
            except socket.error, e:
                print TCKR + ' OOPS: timeout error 3' + 'time='+str(time.time()-start)
                print e
            except httplib.IncompleteRead, e:
                print TCKR + ' OOPS: timeout error 4' + 'time='+str(time.time()-start)
                print e

    if len(results)==0: #return error 
        return 0
    elif len(results)==1:
        df = results[0].sort_index()
    elif len(results)>1:
        df = pd.concat(results).sort_index()        
    loc=directory+'\\Combined\\'+folder
    df.to_csv(loc+TCKR+'.'+datestr+'.csv')
        
        
#    if(extract=='all' or extract=='position' or extract=='combined'):     
#        url = ('http://hopey.netfonds.no/posdump.php?date=' 
#                +datestr+'&paper=%s&csv_format=csv')        
#
#        urlread=0
#        while urlread==0:   
#            start=time.time()
#            try:     
#                sys.stdout.flush()
#                buff = urllib2.urlopen(url %TCKR) 
#                csvstring = buff.read()
#                if 'This stock does not exist' in csvstring:
#                    print TCKR+': This stock no longer exists'
#                    urlread=1
#                    continue
#                if csvstring =='':
##                    print TCKR+': Market Closed or No Data on date='+ datestr
#                    break 
#                else:
#                    df = pd.read_csv(io.BytesIO(csvstring))
#                    head='time,bid,bid_depth,bid_depth_total,offer,offer_depth,offer_depth_total'.split(',')
#                    if not (head==df.columns.values).all():
#                        print "Error: problem pulling data from website on date="+ datestr
#                        return 1
#                    elif len(df)==0:
#                        print "Market Closed or No Data on date="+ datestr
#                        return 0
#                    TCKRp = df
#                    TCKRp['time'] = pd.to_datetime(TCKRp['time'], format='%Y%m%dT%H%M%S')    
#                    TCKRp = TCKRp.drop_duplicates()
#                    TCKRp.index = TCKRp['time']; del TCKRp['time']
#                    urlread=1
#            except urllib2.URLError, e:
#                print TCKR + ' OOPS: timout error 1' + ' time='+str(time.time()-start)
#                print e
#            except socket.timeout, e:
#                print TCKR + ' OOPS: timeout error 2' + ' time='+str(time.time()-start)
#                print e
#            except socket.error, e:
#                print TCKR + ' OOPS: timeout error 3' + 'time='+str(time.time()-start)
#                print e
#            except httplib.IncompleteRead, e:
#                print TCKR + ' OOPS: timeout error 4' + 'time='+str(time.time()-start)
#                print e
                
                
#    if (extract=='all' or extract=='trades' or extract=='combined'):
#        url = ('http://hopey.netfonds.no/tradedump.php?date=' 
#                +datestr+'&paper=%s&csv_format=csv')     
#        
#
#        urlread=0
#        while urlread==0:   
#            start=time.time()
#            try:
#                buff = urllib2.urlopen(url %TCKR) 
#                csvstring = buff.read()
#                if 'This stock does not exist' in csvstring:
#                    print TCKR+': This stock no longer exists'
#                    urlread=1
#                    continue
#                else:
#                    df = pd.read_csv(io.BytesIO(csvstring))
#                    urlread=1
#            except urllib2.URLError, e:
#                print TCKR + ' OOPS: timout error 1' + ' time='+str(time.time()-start)
#                print e
#            except socket.timeout, e:
#                print TCKR + ' OOPS: timeout error 2' + ' time='+str(time.time()-start)
#                print e
#            except socket.error, e:
#                print TCKR + ' OOPS: timeout error 3' + 'time='+str(time.time()-start)
#                print e
#            except httplib.IncompleteRead, e:
#                print TCKR + ' OOPS: timeout error 4' + 'time='+str(time.time()-start)
#                print e
#
#        head='time,price,quantity,board,source,buyer,seller,initiator'.split(',')
#        if not (head==df.columns.values).all():
#            print "Error: problem pulling data from website on date="+ datestr
#            return 1
#        elif len(df)==0:
#            print "Market Closed or No Data on date="+ datestr
#            return 0
#        
#        TCKRt = df
#        del TCKRt['board'] 
#        del TCKRt['buyer']
#        del TCKRt['initiator'] 
#        del TCKRt['seller'] 
#        del TCKRt['source']
#        TCKRt['time'] = pd.to_datetime(TCKRt['time'], format='%Y%m%dT%H%M%S')        
#        TCKRt = TCKRt.drop_duplicates()
#        TCKRt.index = TCKRt['time']; del TCKRt['time']
        

#    if (extract=='all' or extract=='combined'):
#        if ('TCKRp' in locals()):
#            TCKRb = pd.concat([TCKRp, TCKRt]).sort_index()        
#        else:
#            TCKRb = TCKRt.sort_index()
#        loc=directory+'\\Combined\\'+folder
#        TCKRb.to_csv(loc+TCKR+'.'+datestr+'.csv')

#    elif (extract=='position' or extract=='all'):
#        loc=directory+'\\Positions\\'+folder
#        TCKRp.to_csv(loc+TCKR+'.'+datestr+'.csv')
#        
#    elif (extract=='trade' or extract=='all'):
#        loc=directory+'\\Trades\\'+folder
#        TCKRt.to_csv(loc+TCKR+'.'+datestr+'.csv')
#    else:
#        print 'Unable to recognise mktdata type [all,trade,position, combined]'
#        return 1
        
    return 'ok'



