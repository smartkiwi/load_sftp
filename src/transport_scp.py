'''
@author: Volodymyr Vladymyrov

'''
import pprint
import logging
import os
import traceback
import paramiko
import sys

pprint = pprint.PrettyPrinter(4)


class TransportSCP(object):
    '''
    SCP Transport Class
    '''


    def __init__(self,host=None,port=None,user=None,password=None,remote_dir='.',local_dir='.',pkey=False,keysdir=None):
        import paramiko
        paramiko.util.log_to_file('demo_sftp.log')        
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
        self.hostkeytype = None
        self.hostkey = None
        self.host_keys = {}
        
        
        
        self.key = None
        
        
        self.pkey = pkey
        if pkey:
            if keysdir is None:
                self.keysdir = os.path.expanduser('~/.ssh/')
            else:
                self.keysdir = keysdir
                
            if not os.path.exists(self.keysdir):
                print "*** Error: public keys directory do no exists %s" % self.keysdir                
                sys.exit(-1)
                        
        
        
    def __init_logger(self):
        self.lg = logging.getLogger("scp_transport")
        
        
    def load_keys(self):
        # get host key, if we know one
        self.hostkeytype = None
        self.hostkey = None
        self.host_keys = {}
        try:
            self.host_keys = paramiko.util.load_host_keys(self.keysdir+'/known_hosts')
        except IOError:
            try:
                # try ~/ssh/ too, because windows can't have a folder named ~/.ssh/
                self.host_keys = paramiko.util.load_host_keys(self.keysdir+'/known_hosts')
            except IOError:
                print '*** Unable to open host keys file'
                self.lg.error('*** Unable to open host keys file %s' %  self.keysdir+'/known_hosts')
                self.host_keys = {}
        
        if self.host_keys.has_key(self.host):
            self.hostkeytype = self.host_keys[self.host].keys()[0]
            self.hostkey = self.host_keys[self.host][self.hostkeytype]
            self.lg.info('Using host key of type %s' % self.hostkeytype)
            
        if self.hostkeytype=='ssh-dss':
            self._load_key_d()
        elif self.hostkeytype=='ssh-rsa':
            self._load_key_r()
        else:
            self.lg.error("don't know how to load key type %s" % self.hostkeytype)
            sys.exit(-1)
        
            
        #pprint.pprint(self.host_keys)
        self.lg.info(pprint.pformat(self.host_keys))
            
        # check server's host key -- this is important.
        key = self.transport.get_remote_server_key()
        
        if not self.host_keys.has_key(self.host):
            print '*** WARNING: Unknown host key! %s' % self.host
            self.lg.error('*** WARNING: Unknown host key! %s' % self.host)
        elif not self.host_keys[self.host].has_key(key.get_name()):
            print '*** WARNING: Unknown host key!'
            self.lg.error('*** WARNING: Unknown host key! ')
        elif self.host_keys[self.host][key.get_name()] != key:
            print '*** WARNING: Host key has changed!!!'
            self.lg.error('*** WARNING: Host key has changed!!!')
            sys.exit(1)
        else:
            self.lg.info('*** Host key OK.')
            #print '*** Host key OK.'
            
    def _load_key_d(self):
        default_path = self.keysdir + '/id_dsa'
        self.lg.info("will try to load dsa key from %s" % default_path)

        try:
            self.key = paramiko.DSSKey.from_private_key_file(default_path)                    
        except paramiko.PasswordRequiredException:
            password = 'test123'
            self.key = paramiko.DSSKey.from_private_key_file(default_path, password)

    def _load_key_r(self):
        default_path = self.keysdir + '/id_rsa'
        self.lg.info("will try to load rsa key from %s" % default_path)

        try:
            self.key = paramiko.RSAKey.from_private_key_file(default_path)                    
        except paramiko.PasswordRequiredException:
            password = 'test123'
            self.key = paramiko.DSSKey.from_private_key_file(default_path, password)
        
        

    def _connect_publickey(self):
        """connects to the remote SFTP server"""
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            
            #if self.lg.getEffectiveLevel()!=logging.INFO:
            #    paramiko.util.log_to_file(self.script_dir+"/ssh_session.log")
                
            self.transport.connect(username = self.user, password = None, hostkey=self.hostkey)
            if self.pkey:
                self.load_keys()
            
            self.transport.auth_publickey(self.user, self.key)                
                    
            if not self.transport.is_authenticated():
                print '*** Authentication failed. :('
                self.lg.error('*** Authentication failed. :(')
                self.transport.close()
                sys.exit(1)
                            
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


    def _connect_interactive(self):
        """connects to the remote SFTP server"""
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            
#            if self.lg.getEffectiveLevel()!=logging.INFO:
#                paramiko.util.log_to_file(self.script_dir+"/ssh_session.log")
            self.transport.connect(username = self.user, password = self.password, hostkey=None)
           
                            
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
    
    def connect(self):
        """connects to the remote SFTP server"""
        if not self.pkey:
            self._connect_interactive()
        else:
            self._connect_publickey()
            
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
    

    print "trying user/password login"
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
    scp.get_file("/sd/.profile",'out/.profile')
    scp.disconnect()
    scp.log_error_stats()
    
    
    print "trying user/publickey login"
    
    scp = TransportSCP(
            host='xp4r.com',
            port=22,
            user="vvlad",
            password="",
            remote_dir=".",
            pkey=True,
            keysdir="C:/My Documents/Work/workspace/load_sftp/pkeys" 
                       )
    scp.connect()
    files = scp.get_remote_files()
    pprint.pprint(files)
    scp.get_file("/home/vvlad/.profile",'out/.bashrc')
    scp.disconnect()
    scp.log_error_stats()

    
    