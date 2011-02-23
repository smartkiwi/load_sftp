'''
Created on ${date}
Last update $Date: 2009/10/09 21:45:28 $
Re$Revision: 1.20 $

@author: Volodymyr Vladymyrov

'''
import time
import traceback
import sys
import logging
import os



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
    _age_purgeperiod = 2*24*3600
    
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
            skipolditems = 0
            while 1:
                lines = f.readline(100000)
                if not lines:
                    break
                line = lines.rstrip()
                totallines=totallines+1
                items = line.split('|')
                if len(items)>=2:
                    founditems=founditems+1
                    age = 0
                    #load age if exists or set is to current time
                    if len(items)>=3:
                        age = int(items[3])
                    else:
                        age = time.time()
                        
                    if time.time()-age<=self._age_purgeperiod:                        
                        filename = items[0]
                        ts = float(items[1])
                        self.data[filename]=ts
                        self.age[filename] = age                        
                    else:
                        self._logger.debug("skipping record is older then %s hours" % int(self._age_purgeperiod/3600))
                        skipolditems=skipolditems+1
                    
                                            
                        
            f.close()
        except Exception, e:
            self._logger.error("exception while loading file history - reading file %s: %s" % (self._file,str(e)) )
            self._logger.error(traceback.format_exc())
        self._logger.info("%s lines found in file %s, %s items loaded " % (totallines,self._file,founditems))
        self._logger.info("%s lines skipped because they are older then %s hours" % (skipolditems,int(self._age_purgeperiod/3600)))

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
        



if __name__ == '__main__':
    
    logger = logging.getLogger()
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(filename)s: %(lineno)d : %(module)s : %(funcName)s : %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    fh = FileHistory("test")
    ctime = time.time()
    fh["dir/file"] = ctime
    print fh.keys()
    print fh["dir/file"]
    print os.getcwd()
    if str(fh["dir/file"])!=str(ctime):
        print "nok"
    else:
        print "ok"
        
    fh["di1/fil1"] = ctime-32*24*3600
        
    fh.save_final()
    fh=None
    
    fh1 = FileHistory("test")
    #fh1.load()
    print fh1.keys()
    print fh1["dir/file"]
    if str(fh1["dir/file"])!=str(ctime):
        print "nok"
    else:
        print "ok"
        
        
        
    fh1.purge_by_age()
    
    if len(fh1.keys())>1:
        print "nok"
    fh1.save_final()
    
    fh1 = None
    
                 
    fh2 = FileHistory("test")
    #fh1.load()
    print fh2.keys()
    print fh2["dir/file"]
    if str(fh2["dir/file"])!=str(ctime):
        print "nok"
    else:
        print "ok"
        
