#init pprint and default logger
import pprint
from transport_scp import TransportSCP
pprint = pprint.PrettyPrinter(4)



import pprint
import logging
import os
import traceback
import paramiko
import sys



    


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
        keysdir="C:/My Documents/Work/workspace/load_sftp/pkeys",
        passphrase='test123' 
                   )
scp.connect()
files = scp.get_remote_files()
pprint.pprint(files)
scp.get_file("/home/vvlad/.profile",'out/.bashrc')
scp.disconnect()
scp.log_error_stats()
