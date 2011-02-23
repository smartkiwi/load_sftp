'''
@author: Volodymyr Vladymyrov

'''
import re
import stat
import pprint
import logging
import os
import traceback
from ftplib import FTP, error_perm


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
        self.ftpdebug = False
        
        
        
        self.__error_stat = dict()
        
    def __init_logger(self):
        self.lg = logging.getLogger("scp_transport")
        

    
    def connect(self):
        """connects to the remote FTP server"""
        try:
            self.transport = FTP()
            self.ftp = self.transport
            if self.ftpdebug:
                self.transport.set_debuglevel(2)
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
        return self.__convert_file_attrs_nlist(fileattrs)
    
    
    def __convert_file_attrs_nlist(self,scpfileattrs):
        fileattrs = list()
        for file in scpfileattrs:
            onlyfilename=os.path.basename(file)
            newfile = {
                       #pretend that all entries returned by nlist are regular files (even directories)
                       'type':stat.S_IFREG,
                       'filename':onlyfilename,
                       #file modification time - is 0 unixtime because nlist doesn't provide file modification time
                       'mtime':0
                       }
            fileattrs.append(newfile)
        return fileattrs
    def convert_file_attrs_dir(self,lines,pat_str=None):
        '''-rw-r--r--   1 vvlad    vvlad      109068 Jul 17  2010 dictd_www_freedict_de_eng-rus.prc'''
        '''drwxr-xr-x   2 vvlad    vvlad         512 Feb 23 00:10 scripts'''
        fileattrs = list()
        if pat_str is None:
            pat_str = "(\d+\s*\d*)\s([\s+])$"
        pattern = re.compile(pat_str)
        for file in lines:
            
            onlyfilename=os.path.basename(file)
            newfile = {
                       #pretend that all entries returned by nlist are regular files (even directories)
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
            
    def __sendcommand(self,cmd):
        lines = []
        self.transport.retrlines(cmd, lines.append)
        return lines
        
    def get_remote_files_dir(self,remote_dir=None):
        """retrieves the list of files in the directory and its attributes - 
        every file entry is in SFTPAttributes format
        @return: list of SFTPAttributes
        """
        if remote_dir is None:
            remote_dir = self.remote_dir
            
        fileattrs  = None
        try:
            fileattrs = self.__sendcommand("LIST -latr "+self.remote_dir) 
        except Exception, e:
            self.lg.error("Failed to get list of files in remote directory %s : %s" % (remote_dir, str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))
        return self.convert_file_attrs_dir(fileattrs)

    

                


if __name__ == '__main__':
#init pprint and default logger
    import pprint
    pprint = pprint.PrettyPrinter(4)    
    

    logformat = '%(asctime)s : %(name)s : %(levelname)s : %(filename)s: %(lineno)d : %(module)s : %(funcName)s : %(message)s'
    logging.basicConfig(level=logging.DEBUG,format=logformat)
    _logger = logging.getLogger()
#test main    
    ftp = TransportFTP(
            host='osprey.ise.telcordia.com',
            port=21,
            user="sd",
            password="sd123",
            remote_dir="."
                       )
    ftp.connect()
    files = ftp.get_remote_files()
    pprint.pprint(files)
    ftp.get_file("/sd/vpd.properties",'/sdtl/vpd.properties_downloaded')
    ftp.disconnect()
    ftp.log_error_stats()
    