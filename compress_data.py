# -*- coding: utf-8 -*-


import tarfile
import gzip
import bz2
import os
import pandas as pd
import get_lists as getl


def compress_data_ticker(TCKR, listdir, directory, compression='bz2', complevel=9):
    start_dir = os.getcwd()
    os.chdir(directory)
    
    #get list of files for ticker
    files_for_ticker = getl.get_csv_file_list(TCKR, listdir, directory)
    if (files_for_ticker==0):
        return 0
    if (files_for_ticker=='no tickers'):
        print TCKR + ': no files to archive in directory:' +directory
        return None
    if len(files_for_ticker)==0:
        print TCKR + ': no files to archive in directory:' +directory
        return None

    #check if hdf file exists for this ticker    
    hdf = TCKR+'.combined.h5'
    if not(os.path.isfile(hdf)):
        print TCKR+': no hdf5 file in:'+directory
#        store = pd.HDFStore(hdf)
#        store['dataframe'] = pd.DataFrame()
        print 'could not archive any .csv files for:' +TCKR
        print 'create hdf5 file first with tick_data_combine'
        print 'program terminated'
        f = open('D:\\Financial Data\\Netfonds\\DailyTickDataPull\\errorlog.txt','a')
        f.write('-8%s: no hdf5 file in directory: '%TCKR+directory+'\n')
        f.close()
        return 0
        
        
    #get last saved date in hdf file. Everything <= this data gets archived    
    store = pd.HDFStore(hdf)
    maxdate = store['dataframe'].index.max().date()
    store.close()    
    
    #remove combined files and files not merged into h5 file
    for fl in files_for_ticker:
        if 'combined' in fl:
            files_for_ticker.remove(fl)
            continue
        fl_datestr = fl.replace(TCKR+'.','').replace('.csv','')
        fl_date = pd.datetime.strptime(fl_datestr, '%Y%m%d').date()
        if fl_date > maxdate:
            files_for_ticker.remove(fl)

    if len(files_for_ticker)==0:
        print TCKR+': no files exist where date < combined.h5 max'        
        return 0
        
    #open the tarball    
    tar = tarfile.open(TCKR+'.tar','a')
    already_archived_files = tar.getnames()    
    for fl in already_archived_files: #remove names already in the tarball
        if fl.replace('.'+compression,'') in files_for_ticker:
            files_for_ticker.remove(fl.replace('.'+compression,''))
            
        
    #loop through remainng files and compress 
    comp_files=[]    
    for fl in files_for_ticker:
        outf = fl+'.'+compression      
        inf = open(fl, 'rb')
        data = inf.read()
        inf.close()
        
        if compression =='gz':
            fcomp = gzip.open(outf, 'wb', compresslevel=complevel) 
        elif compression =='bz2':
            fcomp = bz2.BZ2File(outf, 'wb', compresslevel=complevel)
       
        fcomp.write(data)
        fcomp.close()  
        comp_files.append(outf) #create list of compressed files for adding to tarball         

    for fl in files_for_ticker:
        os.remove(fl)
        
    #loop through compressed files and add to tarball
    for fl in comp_files:
        if (fl in tar.getnames()): #check if its already in there
            print fl+': is already in tarball'           
            continue
        tar.add(fl)
    tar.close()
    #remove compressed files outside of archive
    for fl in comp_files:
        os.remove(fl)
        
    os.chdir(start_dir)
    return None

def compress_data_directory(directories, compression):
    #import get_lists as getl
    # get list of tickers.    
    import time    
    j=0    
    start = time.time()
    for directory in directories:
        j=j+1
        tickers_in_dir, listdir = getl.get_list_tickers_in_dir(directory)
        i=0        
        for ticker in tickers_in_dir:
            i=i+1
            compress_data_ticker(ticker, listdir, directory,compression) 
            print '%-8s:compressed, iter '%ticker +str(i)+' of '+str(len(tickers_in_dir))+ ', dir:'+str(j)+' of ' +str(len(directories)) + ' time=%5.2f' %((time.time()-start)/60)
    return
        
if __name__ == '__main__':
    os.chdir('D:\\Google Drive\\Python\\FinDataDownload')

    directories = ['D:\\Financial Data\\Netfonds\\DailyTickDataPull\\Combined\\ETF']
    compress_data_directory(directories, 'bz2')

