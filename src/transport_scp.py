'''
@author: Volodymyr Vladymyrov

'''
import pprint
import logging
import os
import traceback


class TransportSCP(object):
    '''
    SCP Transport Class
    '''


    def __init__(self,host=None,port=None,user=None,password=None,remote_dir='.',local_dir='.'):
        import paramiko        
        self.__init_logger()
        self.transport = None
        self.remote_dir = remote_dir
        self.host = host
        if port!=None:
            self.port = port
        else:
            self.port = 22
        self.user = user
        self.password = password
        self.local_dir = local_dir
        self.ftp = None
        
        self.__error_stat = dict()
        
    def __init_logger(self):
        self.lg = logging.getLogger("scp_transport")
        

    
    def connect(self):
        """connects to the remote SFTP server"""
        try:
            self.transport = paramiko.Transport((self.host, self.port))
#            if self.lg.getEffectiveLevel()!=logging.INFO:
#                paramiko.util.log_to_file(self.script_dir+"/ssh_session.log")
            self.transport.connect(username = self.user, password = self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            #check that directory exists
            self.sftp.chdir(self.remote_dir)
        except IOError, e:
            self.lg.error("directory %s doesn't exists: %s" % (self.remote_dir,str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))            
        except Exception, e:
            self.lg.error("Failed to connect to the remote server %s" % str(e))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))
            
    def disconnect(self):
        """disconnects from the remove SFTP server"""
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
            fileattrs = self.sftp.listdir_attr(remote_dir) 
        except Exception, e:
            self.lg.error("Failed to get list of files in remote directory %s : %s" % (remote_dir, str(e)))
            self.lg.debug(traceback.format_exc())
            self.__count_error(str(e))
        return self.__convert_file_attrs(fileattrs)
    
    
    def __convert_file_attrs(self,scpfileattrs):
        fileattrs = list()
        for file in scpfileattrs:
            newfile = {
                       'type':file.st_mode,
                       'filename':file.filename,
                       'mtime':file.st_mtime
                       }
            fileattrs.append(newfile)
        return fileattrs
    
    
    def get_file(self,filename,localname):
        ok = True
        try:
            self.sftp.get(filename, localname)
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
    scp = TransportSCP(
            host='osprey.ise.telcordia.com',
            port=22,
            user="sd",
            password="sd123",
            remote_dir="."
                       )
    scp.connect()
    files = scp.get_remote_files()
    pprint.pprint(files)
    scp.get_file("/sd/vpd.properties1",'/sdtl/vpd.properties_downloaded')
    scp.disconnect()
    scp.log_error_stats()
    