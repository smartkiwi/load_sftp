'''
Created on ${date}
Last update $Date: 2009/10/09 21:45:28 $
Re$Revision: 1.20 $

@author: Volodymyr Vladymyrov

Copyright: Telcordia 2009
'''
import logging
import re


class Multi_Grep:

    def __init__(self,regexes={},notfoundrex=None):
        self.__init_logger()
        self.cleanup()
        self.regexes = regexes
        for rx in self.regexes.keys():
            self.refound[rx] = False
            self.recontents[rx] = None
        self.debug = False
        self.notfountbreakflag = False
        self.renotfound = notfoundrex
        
        
    def append(self,buffer):
        """process item """
        self.match_all(buffer)
        self.buffers.append(buffer)
        
        
    def match_all(self,buffer):
        """check item againts predefined regexes, remembers result and groups"""
        for rx in self.regexes.keys():
            reg = self.regexes[rx]
            res = re.search(reg,buffer)
            self.dbg("checking %s regex against '%s' " % (rx,buffer))
            if res != None:
                self.dbg("\t match found")
                self.refound[rx] = True
                if len(res.groups())>0:
                    self.dbg("\t groups %s " % str(res.groups()))
                    self.recontents[rx] = res.group(1)
            else:
                self.dbg("\t no match")
        
        """check against break not found regex"""
        if self.renotfound is not None and re.search(self.renotfound,buffer) is not None:
            self.notfountbreakflag = True        
            
    
    def found_all(self):
        """check was all patterns found"""
        count = 0
        for rx in self.regexes.keys():
            if self.refound[rx]:
                count=count+1
        return count==len(self.regexes)
    
    def found_break(self):
        """return true if not-found regex trigerred"""
        return self.notfountbreakflag
            
    
    def has_item(self,itemkey):
        """check does file contain particular pattern"""
        self.dbg(str(self.refound))
        if self.refound.has_key(itemkey):
            return self.refound[itemkey] 
        return False
    
    def cleanup(self):
        """cleanup internal buffers"""
        self.buffers = []
        self.regexes = {}
        self.refound = {}
        self.recontents = {}
        
    
    def __init_logger(self):
        self.lg = logging.getLogger("mutli grep")
        
    def dbg(self,str):
        if self.debug:
            self.lg.debug(str)
    
        



if __name__ == '__main__':
#init pprint and default logger
    import pprint
    pprint = pprint.PrettyPrinter(4)    
    

    logformat = '%(asctime)s : %(name)s : %(levelname)s : %(filename)s: %(lineno)d : %(module)s : %(funcName)s : %(message)s'
    logging.basicConfig(level=logging.DEBUG,format=logformat)
    _logger = logging.getLogger()
#test main    

    tsre = re.compile('''.*PMSetup  startTime="([^\"]+)" ''')
    kpire = re.compile('''.*\<M1023C5\>''')
    
    mg = Multi_Grep({'ts':tsre,'kpi':kpire})
    mg.debug = True
    
    mg.append('''<?xml version="1.0"?>''')
    
    assert mg.found_all()==False
    
    mg.append('''<OMeS  xmlns="pm/cnf_rnc_nsn.6.0.xsd">''')
    
    assert mg.found_all()==False
    
    mg.append('''<PMSetup  startTime="2011-02-20T15:00:00.000-06:00:00" interval="60">''')
    
    assert mg.has_item('ts')==True    
    assert mg.found_all()==False
    
    mg.append('''<M1023C5>122.666668</M1023C5>''')
    
    assert mg.has_item('kpi')==True
    assert mg.found_all()==True
    
    
    mg.cleanup()
    
    
    mg = Multi_Grep({'ts':tsre,'kpi':kpire})
    assert mg.found_all()==False
    
    mg.append('''<?xml version="1.0"?>
<OMeS  xmlns="pm/cnf_rnc_nsn.6.0.xsd">
  <PMSetup  startTime="2011-02-20T15:00:00.000-06:00:00" interval="60">
    <PMMOResult>
      <MO>
        <DN><![CDATA[PLMN-PLMN/RNC-1902/WBTS-201/WCEL-2015]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/MCC-334/MNC-3]]></DN>
''')
    assert mg.has_item('ts')==True    
    assert mg.found_all()==False
    
    mg.append('''        <M1017C11>1</M1017C11>
        <M1017C12>13.439999</M1017C12>
        <M1017C13>564.47998</M1017C13>
        <M1023C5>12</M1023C5>
''')
    
    assert mg.has_item('kpi')==True
    assert mg.found_all()==True
    

    mg.cleanup()
    notfoundre = re.compile('''<\/PMMOResult>''')
    mg = Multi_Grep({'ts':tsre,'kpi':kpire},notfoundre)
    assert mg.found_all()==False    
        


    mg.append('''<?xml version="1.0"?>
<OMeS  xmlns="pm/cnf_rnc_nsn.6.0.xsd">
  <PMSetup  startTime="2011-02-20T15:00:00.000-06:00:00" interval="60">
    <PMMOResult>
      <MO>
        <DN><![CDATA[PLMN-PLMN/RNC-1902/WBTS-201/WCEL-2015]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/MCC-334/MNC-3]]></DN>
''')
    assert mg.has_item('ts')==True    
    assert mg.found_all()==False
    
    mg.append('''        <M1017C11>1</M1017C11>
        <M1017C12>13.439999</M1017C12>
        <M1017C13>564.47998</M1017C13>
        <M1023C>12</M1023C>
        </PMMOResult>
''')
    
    assert mg.has_item('kpi')==False
    assert mg.found_break()==True
    
#TODO - support for string broken into parts - use StringIO instead of buffer
#TODO - add debug messages for not found pattern


"""
<?xml version="1.0"?>
<OMeS  xmlns="pm/cnf_rnc_nsn.6.0.xsd">
  <PMSetup  startTime="2011-02-20T15:00:00.000-06:00:00" interval="60">
    <PMMOResult>
      <MO>
        <DN><![CDATA[PLMN-PLMN/RNC-1902/WBTS-201/WCEL-2015]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/MCC-334/MNC-3]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/TR_CLASS-background/TR_SUBCLASS-PS_NRT_data/THP-not_used]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/RAB_max_dl-2048000/RAB_max_ul-1024000]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/RB_dl-FACH/RB_ul-RACH]]></DN>
      </MO>
      <MO>
        <DN><![CDATA[PLMN-PLMN/error_ratio-SDU_1e4]]></DN>
      </MO>
      <PMTarget  measurementType="RCPM_RLC">
        <M1017C0>122.666668</M1017C0>
        <M1017C1>0</M1017C1>
        <M1017C10>0</M1017C10>
        <M1017C11>1</M1017C11>
        <M1017C12>13.439999</M1017C12>
        <M1017C13>564.47998</M1017C13>
        <M1017C14>12</M1017C14>
        <M1017C15>13.439999</M1017C15>
        <M1017C16>564.47998</M1017C16>
        <M1017C17>0.001814</M1017C17>
        <M1017C18>1</M1017C18>
        <M1017C19>0</M1017C19>
        <M1017C2>0.2704</M1017C2>
        <M1017C20>2</M1017C20>
        <M1017C21>6</M1017C21>
        <M1017C22>0</M1017C22>
        <M1017C23>0</M1017C23>
        <M1017C24>144</M1017C24>
        <M1017C25>144</M1017C25>
        <M1017C26>0</M1017C26>
        <M1017C27>20736</M1017C27>
        <M1017C28>1</M1017C28>
        <M1017C29>150</M1017C29>
        <M1017C3>0</M1017C3>
        <M1017C30>261</M1017C30>
        <M1017C31>52</M1017C31>
        <M1017C32>9</M1017C32>
        <M1017C33>180</M1017C33>
        <M1017C34>2</M1017C34>
        <M1017C35>0</M1017C35>

"""