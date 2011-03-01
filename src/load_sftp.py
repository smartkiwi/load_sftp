'''

Script to load latest files via SFTP protocol.
    Reads previously saved last_ts from the last_ts.txt file.
    Connects to the specified user:password@host:port
    Go to specified directory
    Get the list of files there
    Filters this list of get the list of file with mtime later then last_ts (mtime>last_ts+delay in seconds)
    Downloads matching files into the local output directory
    Updates last_ts file 

@author: Volodymyr Vladymyrov

'''
from transport_scp import TransportSCP
from transport_ftp import TransportFTP

import re
from FileHistory import FileHistory
import os
import sys
import getopt
import traceback
import logging
import time
import stat

import pprint


pprint = pprint.PrettyPrinter(4)



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
        
        
        self.debug_level = "INFO"
        self._filteredfiles = []     
        self._files = []
        
        self.donotdownload = False
        
        self.ftpdebug = False
        
        self.pkey = False
        self.keysdir = None
        
        self.__start_time = time.time()
        
        self.gzip = False
        
        self.gzip_location = '/usr/contrib/bin/gzip'
        
    

    
    
    def main(self):
        #Step 1 reads command line arguments
        self.parse_options()
        self._check_local_dir()
        self.set_log_level() #update log level to the one specified in the --debug option (or default value)

        #Step 2inits file history object and loads items from the file history if it exists
        self.history = FileHistory(file=self.history_file)
        if self.donotdownload:
            self.history._save_every=10000
        

        #init transport
        if (self.protocol=='ftp'):
            self.transport = TransportFTP(
                host=self.host,
                user=self.user,
                password=self.password,
                remote_dir=self.remote_dir,
            )
            if self.ftpdebug:
                self.transport.ftpdebug=True
            
        elif (self.protocol=='scp'):
            self.transport = TransportSCP(
                host=self.host,
                user=self.user,
                password=self.password,
                remote_dir=self.remote_dir,
                pkey=self.pkey,
                keysdir=self.keysdir
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
        
        #pprint.pprint(self._files)        
        #print self.transport.get_remote_files_dir()
        

        
        #Step 5
        #Finds new/changed files using FileHistory and file name regex pattern
        if (self.file_pattern is not None):
            self._filteredfiles = self.filter_files(self._files,self.history,self.file_pattern)
        else:
            self._filteredfiles = self.filter_files(self._files,self.history)                
        
        #Step 6 download files/update history file
        self.download_files(self._filteredfiles)
        
        #Step 7 end: save history file/write log messages
#DONE: think on purge strategry - simply deleting files with TS later then 1 month don't work for old files
#DONE     solution would be to have purge history turnable off manually 
#DONE     have purge history enabled while downloading flow recent files updated every ~5min
#DONE     have purge history disabled while using manually
        self.history.purge_by_age()
        self.history.save_final()
        self.disconnect()        
        #self.lg.info("finished")
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
    --fake - do not download files - just create and populate history file (used to initiate history file with current ftp state)
    --ftpdebug - show FTP session log to STDOUT
    --gzip - after downloading file gzip it 
    --pkey [--keysdir=/home/user/.ssh] - for scp protocol use publickey authecation, optionally - specify keysdir=directory with known_hosts and id_rsa/id_dsa key files 
        """
        help = False
        required = 0
        try:
            opts, args = getopt.getopt(sys.argv[1:], "", ["host=", "user=", "password=", "remote_dir=","file_pattern=","local_dir=","history_file=","debug=","protocol=","keysdir=","help","fake","ftpdebug","pkey"])
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
                if o.lower() == "--pkey":
                    self.pkey = True
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
                if o.lower() == "--keysdir":
                    self.keysdir = a
                    
                    
#TODO: have option to disable purge, by default - purge is on                    
                if o.lower() == "--help":
                    help=True
                    print usage
                if o.lower() == "--fake":
                    self.donotdownload=True
                if o.lower() == "--ftpdebug":
                    self.ftpdebug = True

                if o.lower() == "--gzip":
                    self.gzip = True


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
                    self.compressfile(self.local_dir+'/'+justfile)
                else:
                    self.lg.warn("failed to download the file")
        self.lg.info("%s files were downloaded" % download_count)
        
    def compressfile(self,filepath):
        if self.gzip:
            #run gzip file
            pass
            
             

    
    def disconnect(self):
        """disconnects from the remove SFTP server"""
        self.transport.disconnect()
        self.transport.log_error_stats()
        
    def end(self):
        runtime = time.time() - self.__start_time
        self.lg.info("Finished. Execution took %s" %runtime) 
        
    
if __name__=='__main__':
    loader = FileLoader()
    loader.main()