# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 18:21:54 2015

@author: Dushan
"""

import pandas as pd
#from urllib import urlretrieve as ul
#import numpy as np
import urllib2
import io


def ExchangeTickers2CSV(url, ind,datestr,filepath):

    if ind=='AMEX':
        code = 'A'
    if ind=='NASDAQ':
        code = 'O'
    if ind=='NYSE':
        code = 'N'
    
    urlread=0
    #get ticker names from netfonds website
    while urlread==0:
        try:                
            buff = urllib2.urlopen(url %code) 
            csvstring = buff.read()
            if len(csvstring)==0:
                print 'No '+ind +' data for this date'
                print 'Using '+ind +' tickers from the .csv file on disk'
                urlread=1
                continue
            else:
                df = pd.read_csv(io.BytesIO(csvstring), sep=';', header=None)
                df.to_csv(filepath+ind+'.'+code+'.txt', sep=';', header=False, index=False)
                flname = ind+'.'+code+'.'+datestr+'.txt'
                df.to_csv(filepath+flname, sep=';', header=False, index=False)
                urlread=1
        except urllib2.URLError,e:
            print 'OOPS: URLError on '+ind +' ticker list pull'
            print e
        except socket.timeout, e:
            print TCKR + ' OOPS: socket timeout on '+ind +' ticker list pull'
            print e
        except socket.error, e:
            print TCKR + ' OOPS: socket error on '+ind +' ticker list pull'
            print e
        except httplib.IncompleteRead, e:
            print TCKR + ' OOPS: httplib Imcomplete error on '+ind +' ticker list pull'
            print e
    return 
    
    
    