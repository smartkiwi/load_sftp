#!//home/sd/tools/dk/python2.5/bin/python

#!/sd/tools/python2.5/bin/python



'''

Script to load latest files via FTP protocol.
    Reads previously saved last_ts from the last_ts.txt file.
    Connects to the specified user:password@host:port
    Go to specified directory
    Get the list of files there
    Filters this list of get the list of file with mtime later then last_ts (mtime>last_ts+delay in seconds)
    Downloads matching files into the local output directory
    Updates last_ts file 

@author: Volodymyr Vladymyrov

Copyright: Telcordia 2009
'''

import re
import os
import sys
import getopt
import traceback
import logging
import time
import stat

import pprint


pprint = pprint.PrettyPrinter(4)


import pprint
from ftplib import FTP, error_perm

class FileHistory(object):
    """
    Class for handling the list of downloaded files, with timestamps
    Used to detect changed and new files 
    """
    _logger = None
    _file = None
    data = dict()
    age = dict()
    _purgeperiod = 12*30*24*3600
    _age_purgeperiod = 5*24*3600
    
    _save_every = 1000
    
    def __init__(self,file=None,purgeperiod=30*24*3600):
        """Constructor
        @param file: full path to the history file
        @param purgeperiod: number of seconds from now to detect item as outdated        
        """
        #init logger
        try:
            self._logger = logging.getLogger('FileHistory')
        except Exception, e:
            print "exception while initializing logger: %s" % str(e)
            traceback.print_exc()
            sys.exit(1)
        self._file = file
        self.data = dict()
        if (os.path.exists(self._file)):
            self.load()


    """ dict wrapping methods """
    def clear(self): self.data.clear()          
    def copy(self):                             
        if self.__class__ is FileHistory:          
            return FileHistory(self.data)         
        import copy                             
        return copy.copy(self)                 
    def keys(self): return self.data.keys()     
    def items(self): return self.data.items()  
    def values(self): return self.data.values()
    
    def has_key(self,key):
        return self.data.has_key(key)        
        

    def __getitem__(self, key):
        """return value for the key
        @param - key
        """ 
        return self.data[key]

    def __setitem__(self, key, item):         
        """ sets the value for the key
        @param key
        @param value
        """
        self.data[key] = item
        self.age[key] = time.time()
        
        
    def __len__(self):
        return len(self.data)
        
        
    def save(self):
        """saves file history into the file on disk"""
        if len(self.data.keys())%self._save_every==0:
            self.save_final()

                
    def save_final(self):
        try:
            f = open(self._file, 'w')
            for file in self.data.keys():
                ts = self.data[file]
                #print "ts to save %s" % ts
                age = self.age[file]
                tsread = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(ts)))
                f.write("%s|%s|%s|%s\n" % (file,ts,tsread,age))
            f.close()
        except Exception, e:
            self._logger.error("exception while saving file history - writing to file %s: %s" % (file,str(e)) )
            self._logger.debug(traceback.format_exc())        
    
    
    def load(self):
        """loads file history from the file on disk"""
        try:        
            f = open(self._file, 'r')
            totallines = 0
            founditems = 0
            while 1:
                lines = f.readline(100000)
                if not lines:
                    break
                line = lines.rstrip()
                totallines=totallines+1
                items = line.split('|')
                if len(items)>=2:
                    founditems=founditems+1
                    filename = items[0]
                    ts = float(items[1])
                    self.data[filename]=ts
                    #load age if exists or set is to current time
                    if len(items)>=3:
                        self.age[filename] = items[3]
                    else:
                        self.age[filename] = time.time()
                        
                        
            f.close()
        except Exception, e:
            self._logger.error("exception while loading file history - reading file %s: %s" % (self._file,str(e)) )
            self._logger.error(traceback.format_exc())
        self._logger.info("%s lines found in file %s, %s items loaded " % (totallines,self._file,founditems))

    def purge_by_ts(self):
        """deletes entries with ts older then NOw-purgeperiod seconds"""
        result = dict()
        ctime=time.time()
        purgedcount=0
        for filename,ts in self.data.items():
            if ts>=ctime-self._purgeperiod:
                result[filename] = ts
            else:
                purgedcount=purgedcount+1
        self.data = result
        self._logger.info("%s items were purged" % (purgedcount))


    def purge_by_age(self):
        """deletes entries with ts older then NOw-purgeperiod seconds"""
        result = dict()
        ctime=time.time()
        purgedcount=0
        for filename,ts in self.age.items():
            if ts>=ctime-self._age_purgeperiod:
                result[filename] = ts
            else:
                purgedcount=purgedcount+1
        self.data = result
        self._logger.info("%s items were purged" % (purgedcount))
        



class TransportFTP(object):
    '''
    FTP Transport Class
    '''


    def __init__(self,host=None,port=None,user=None,password=None,remote_dir='.',local_dir='.'):
        self.__init_logger()
        self.transport = None
        self.remote_dir = remote_dir
        self.host = host
        if port!=None:
            self.port = port
        else:
            self.port = 21
        self.user = user
        self.password = password
        self.local_dir = local_dir
        self.ftp = None
        
        
        self.__error_stat = dict()
        
    def __init_logger(self):
        self.lg = logging.getLogger("scp_transport")
        

    
    def connect(self):
        """connects to the remote FTP server"""
        try:
            self.transport = FTP()
            self.ftp = self.transport
            self.transport.connect(self.host, self.port)
#            if self.lg.getEffectiveLevel()!=logging.INFO:
#                paramiko.util.log_to_file(self.script_dir+"/ssh_session.log")
            self.ftp.login(self.user, self.password)
            #self.ftp = paramiko.SFTPClient.from_transport(self.transport)
            #check that directory exists           
        except Exception, e:
            self.lg.error("Failed to connect to the remote server %s:%s (%s)" % (self.host,self.port,str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))

        try:
            self.ftp.cwd(self.remote_dir)
        except Exception, e:
            self.lg.error("Failed to change directory %s" % str(e))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))            
            
    def disconnect(self):
        """disconnects from the remove FTP server"""
        try:
            self.transport.close()
        except Exception, e:
            self.lg.error("Failed to disconnect %s" % str(e))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))                    
    
    def get_remote_files(self,remote_dir=None):
        """retrieves the list of files in the directory and its attributes - 
        every file entry is in SFTPAttributes format
        @return: list of SFTPAttributes
        """
        if remote_dir is None:
            remote_dir = self.remote_dir
            
        fileattrs  = None
        try:
            fileattrs = self.ftp.nlst(remote_dir) 
        except Exception, e:
            self.lg.error("Failed to get list of files in remote directory %s : %s" % (remote_dir, str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))
        return self.__convert_file_attrs(fileattrs)
    
    
    def __convert_file_attrs(self,scpfileattrs):
        fileattrs = list()
        for file in scpfileattrs:
            onlyfilename=os.path.basename(file)
            newfile = {
                       #pretend that all etries returned by nlist are regular files (even directories)
                       'type':stat.S_IFREG,
                       'filename':onlyfilename,
                       #file modification time - is 0 unixtime because nlist doesn't provide file modification time
                       'mtime':0
                       }
            fileattrs.append(newfile)
        return fileattrs
    
    
    def get_file(self,filename,localname):
        ok = False
        try:
            f = open(localname, 'wb')
            try:
                self.ftp.retrbinary('RETR '+filename, f.write, 8*1024)
                ok = True
            except error_perm, e:
                self.lg.warn("5xx failed to download remote file: %s and save it as local: %s, reason %s " % (filename,localname,str(e)))
                f.close()
                #remove file we haven't downloaded
                os.remove(localname)
                return ok
            f.close()
            #self.ftp.get(filename, localname)
        except Exception, e:
            self.lg.error("failed to download remote file: %s and save it as local: %s, reason %s " % (filename,localname,str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))
            ok = False
        return ok
    
    
    def __count_error(self,errstr=''):
        if self.__error_stat.has_key(errstr):
            self.__error_stat[errstr]=self.__error_stat[errstr]+1
        else:
            self.__error_stat[errstr]=1
            
    def log_error_stats(self):
        self.lg.info("error stats:")
        for k,v in self.__error_stat.items():
            self.lg.info("\t %s: %s times"  % (k,v))
                




class FileLoader(object):
    """Class for loading changed and latest files from the (remove) directory"""
    
    def __init__(self):
        """Constructor"""
        #self.script_dir = self.get_script_dir()
        self.script_dir = os.getcwd()
        self.init_logger()
        self.remote_dir = "."
        self.host = None
        self.port = 22
        self.user = None
        self.password = None

        self.local_dir = "."
        self.history_file="%s/history" % (self.script_dir)
        self.history = None
        self.file_pattern = "^.*$"
        self.protocol = 'ftp'
        
        self.donotdownload = False
        
        self.__start_time = time.time()
        
        
        self.debug_level = "INFO"
        self._filteredfiles = []     
        self._files = []
        
        
    

    
    
    def main(self):
        #Step 1 reads command line arguments
        self.parse_options()
        self._check_local_dir()
        self.set_log_level() #update log level to the one specified in the --debug option (or default value)

        #Step 2inits file history object and loads items from the file history if it exists
        self.history = FileHistory(file=self.history_file)
        

        #init transport
        if (self.protocol=='ftp'):
            self.transport = TransportFTP(
                host=self.host,
                user=self.user,
                password=self.password,
                remote_dir=self.remote_dir
            )              
        else:
            print "ERROR: unsupported protocol %s " % self.protocol
            sys.exit(1) 
        
        #Step 3
        #connects to the SFTP server
        self.connect()
        
        #Step 4
        #get list of files and match with file pattern
        self._files = self.get_remote_files()

        
        #Step 5
        #Finds new/changed files using FileHistory and file name regex pattern
        if (self.file_pattern is not None):
            self._filteredfiles = self.filter_files(self._files,self.history,self.file_pattern)
        else:
            self._filteredfiles = self.filter_files(self._files,self.history)                
        
        #Step 6 download files/update history file
        self.download_files(self._filteredfiles)
        
        #Step 7 end: save history file/write log messages
#TODO: think on purge strategry - simply deleting files with TS later then 1 month don't work for old files
#TODO     solution would be to have purge history turnable off manually 
#TODO     have purge history enabled while downloading flow recent files updated every ~5min
#TODO     have purge history disabled while using manually
        self.history.purge_by_age()
        self.history.save_final()
        self.disconnect()        
        self.end()
    
    def init_logger(self):
        try:
            self.lg = logging.getLogger()
            self.lg.setLevel(logging.INFO)
            handler = logging.FileHandler("%s/load_sftp.log" % self.script_dir)
 
            #logformat = '%(asctime)s : %(name)s : %(levelname)s : %(message)s'
            logformat = '%(asctime)s : %(name)s : %(levelname)s : %(filename)s: %(lineno)d : %(module)s : %(funcName)s : %(message)s'
            handler.setFormatter(logging.Formatter(logformat))
            self.lg.addHandler(handler)
                       
            self.lg.info('Started')
        except Exception, e:
            print "Failed to init logger %s" % str(e)
            traceback.print_exc()
            
    def _check_local_dir(self):
        if not os.path.exists(self.local_dir):
            self.lg.warn("local directory %s not found, try to create it" % self.local_dir)
            try:
                os.mkdir(self.local_dir)
            except Exception, e:
                self.lg.error("Failed to create local directory %s" % str(e))
                self.lg.error(traceback.format_exc())
                
            
            
    def set_log_level(self,choise=None):
        if choise is None:
            if self.debug_level is not None and self.debug_level!="":
                choise = self.debug_level
            else:
                choise = "INFO"
        level = logging.INFO
        if choise == 'DEBUG':    
            level = logging.DEBUG
        elif choise == 'INFO':     
            level = logging.INFO
        elif choise == 'WARNING':  
            level = logging.WARNING
        elif choise == 'ERROR':    
            level = logging.ERROR
        elif choise == 'CRITICAL': 
            level = logging.CRITICAL        
        self.lg.setLevel(level)        
    
    def parse_options(self):
        usage = """load_sftp script. Usage:
load_sftp.py --help --host=<remote host> --user=<sftp user> --password=<sftp password> \
    --source_dir=<source_directory> --file_pattern=<file name pattern> \
    --local_dir=<local_dir>  --history_file=<full path to history file>
    --debug=<debug level> --protocol=ftp|scp
Optional parameters:
    history_file - by default <scriptdir>/history file used
    debug - default value - INFO
    file_pattern - by default - load all found files
    protocol - ftp by default
    port - used default protocol port
        """
        help = False
        required = 0
        try:
            opts, args = getopt.getopt(sys.argv[1:], "", ["host=", "user=", "password=", "remote_dir=","file_pattern=","local_dir=","history_file=","debug=","protocol=","help","fake"])
            for o, a in opts:
                if o.lower() == "--host":
                    self.host = a
                    required=required+1
                if o.lower() == "--user":
                    self.user = a
                    required=required+1
                if o.lower() == "--password":
                    self.password = a
                    required=required+1                                        
                if o.lower() == "--remote_dir":
                    self.remote_dir = a
                if o.lower() == "--file_pattern":
                    self.file_pattern = a
                if o.lower() == "--local_dir":
                    self.local_dir = a
                if o.lower() == "--history_file":
                    self.history_file = a
                if o.lower() == "--debug":
                    self.debug_level = a
                if o.lower() == "--protocol":
                    if a=='scp' or a=='ftp':
                        self.protocol = a                    
#TODO: have option to disable purge, by default - purge is on                    
                if o.lower() == "--help":
                    help=True
                    print usage
                if o.lower() == "--fake":
                    self.donotdownload=True

        except IOError, ioerr:
            print str(ioerr)
            print "Try calling with --help option to see the args list"
            sys.exit(-1)
            
        if required<3 or help==True:
            print "Try calling with --help option to see the args list"
            sys.exit(-1)            
            
    def connect(self):       
        """connects to the remote server using transport"""        
        try:
            self.transport.connect()
        except Exception, e:
            self.lg.error("Failed to connect to the remote server %s" % str(e))
            self.lg.debug(traceback.format_exc())
        
    def get_remote_files(self):
        """retrieves the list of files in the directory and its attributes - 
        every file entry is in SFTPAttributes format
        @return: list of SFTPAttributes
        """
        fileattrs  = []
        try:
            #print "starting getting file"
            fileattrs = self.transport.get_remote_files(self.remote_dir)
#            pprint.pprint(fileattrs)
        except Exception, e:
            self.lg.error("Failed to get list of files in remote directory %s : %s" % (self.remote_dir, str(e)))
            self.lg.debug(traceback.format_exc())
        return fileattrs

    
    def filter_files(self,files,history,regex='^.*$'):
        """filters the list of SFTPAttributes against saved FileHistory and files regex pattern
        @param files: list of  SFTPAttributes
        @param history: FileHistory object
        @param regex:  string with regex pattern to match the files
        @return:  the list of files names to be downloaded
        """
        matchingrx=0
        nonmatchingrx=0
        changedcount=0
        newcount=0
        oldcount=0
        filecount=0
        result = dict()
        for file in files:
            #look only for files (not directories, not symlinks etc)
            if stat.S_IFMT(file['type'])==stat.S_IFREG:
                filecount=filecount+1            
                if re.match(regex, file['filename'])!=None:
                    self.lg.debug(str(file))
                    matchingrx=matchingrx+1                
                    filename = "%s/%s" % (self.remote_dir,file['filename'])
                    if history.has_key(filename):
                        self.lg.debug("File exists in history: old file: %s, history ts: %s, file ts: %s" % (filename,history[filename],file['mtime']))
#TODO: make history time checking optional
                        if history[filename]<file['mtime']:
                            self.lg.debug("File has later ts then in history: old file: %s, history ts: %s, file ts: %s" % (filename,history[filename],file['mtime']))
                            #history[filename] = file.st_mtime
                            result[filename] = file['mtime']
                            changedcount=changedcount+1
                        else:
                            self.lg.debug("File same or older ts then in history: old file: %s, history ts: %s, file ts: %s" % (filename,history[filename],file['mtime']))
                            oldcount=oldcount+1
                    else:
                        self.lg.debug("new file: %s, file ts: %s" % (filename,file['mtime']))
                        #history[filename] = file.st_mtime
                        result[filename] = file['mtime']
                        newcount=newcount+1
                else:
                    nonmatchingrx=nonmatchingrx+1
        self.lg.info("filtering %s files found in %s remote directory against FileHistory with %s entries " % 
                     (filecount,self.remote_dir,len(history)))
        self.lg.info("\tmatching file_pattern '%s': %s (non matching: %s) " % (regex,matchingrx,nonmatchingrx))
        self.lg.info("\told files %s" % oldcount)
        self.lg.info("\tnew files %s" % newcount)
        self.lg.info("\tchanged files %s" % newcount)

        
        
        return result
                
                
    def download_files(self,filelist):
        """downloads files to the local directory
        @param fileslist: list of full filenames on remote server
        """
        download_count = 0
        for filepath in filelist:
            ok = False
            justfile = os.path.basename(filepath)
            try:
                if not self.donotdownload:                
                    ok = self.transport.get_file(filepath, self.local_dir+'/'+justfile)
                else:
                    ok = True
            except Exception, e:
                self.lg.error("Failed to download file: %s" % str(e))
                self.lg.debug(traceback.format_exc())
            else:
                #ok
                if ok:
                    download_count=download_count+1
                    self.lg.debug("downloaded file: %s to the %s" % (filepath,self.local_dir+'/'+justfile))
                    self.history[filepath] = self._filteredfiles[filepath]
                #save after every file - in case of crash 
                    self.history.save()
                else:
                    self.lg.warn("failed to download the file")
        self.lg.info("%s files were downloaded" % download_count)
            
    def end(self):
        runtime = time.time() - self.__start_time
        self.lg.info("Finished. Execution took %s" %runtime)             

    
    def disconnect(self):
        """disconnects from the remove SFTP server"""
        self.transport.disconnect()
        self.transport.log_error_stats()
        
    
if __name__=='__main__':
    loader = FileLoader()
    loader.main()