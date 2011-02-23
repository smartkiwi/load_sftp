'''
@author: Volodymyr Vladymyrov

'''
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
    