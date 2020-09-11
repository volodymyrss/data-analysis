

import re
import sys
import os
import threading
import time
from datetime import datetime

from dataanalysis.bcolors import render

global_suppress_output=False
global_fancy_output=False
global_catch_main_output=True
global_output_levels=['top','cache','net']
#global_permissive_output=True
global_permissive_output=False
global_all_output=True
global_log_enabled=True
global_debug_enabled=False

if 'DDA_OUTPUT_LEVELS' in os.environ:
    global_output_levels=os.environ.get("DDA_OUTPUT_LEVELS").split(",")

def get_local_log(local_level):
    def local_log(*args, **kwargs):
        if 'level' not in kwargs:
            kwargs['level'] = local_level
        log(*args, **kwargs)
    return local_log


def setup_graylog():
    try:
        import graypy
    except ImportError:
        return

    import os
    import logging
    from logstash_formatter import LogstashFormatterV1

    my_logger = logging.getLogger('dda_logger')
    my_logger.setLevel(logging.DEBUG)

    handler = graypy.GELFHandler(os.environ.get('GELF_HOST','localhost'), 12201)
    formatter = LogstashFormatterV1()
    handler.setFormatter(formatter)
    my_logger.addHandler(handler)

    return my_logger

graylog_logger=None
#graylog_logger=setup_graylog()

import base64
import json
import os
import re

def extract_logstash_levels():
    default_logstash_level = os.environ.get('LOGSTASH_OUTPUT_LEVEL', 'ERROR')

    if "LOGSTASH_LEVELS" in os.environ:
        logstash_levels=os.environ.get("LOGSTASH_LEVELS").split(",")
        logstash_levels_patterns=list(map(re.compile,logstash_levels))
        return logstash_levels, logstash_levels_patterns, default_logstash_level
    return None, [], default_logstash_level

def setup_logstash(default_logstash_level):
    import os
    import sys
    import logging


    my_logger = logging.getLogger('logstash_logger')
    my_logger.setLevel(default_logstash_level)
    
    try:
        from logstash_formatter import LogstashFormatterV1
        formatter = LogstashFormatterV1()
        handler.setFormatter(formatter)
    except Exception as e:
        my_logger.warning("unable to setup logstash formatter: %s", e)

    handler = logging.StreamHandler(stream=sys.stdout)
    my_logger.addHandler(handler)

    my_logger.info("starting logstash logger",extra=dict(main=__file__,origin="dda",levels=logstash_levels))
    return my_logger


logstash_levels, logstash_levels_patterns, default_logstash_level = extract_logstash_levels()

logstash_logger=setup_logstash(default_logstash_level = default_logstash_level)

def log(*args,**kwargs):
    if global_suppress_output:
        return
    else:
        my_pid = os.getpid()
        my_thread=threading.current_thread().ident

        level = kwargs.get("level","debug")

        if graylog_logger is not None and ("net" in level or "top" in level):
            graylog_logger.debug(args)

        if global_permissive_output:
            print(time.time(),"DEBUG",my_pid,"/",my_thread,level, *args)

        if level in global_output_levels:
            print(time.time(),render("{BLUE}%s{/}"%level),str(my_pid)+"/"+str(my_thread), *args)


def log_hook(level,obj,**aa):
    for l in logstash_levels_patterns:
        if l.match(level):
            return log_in_context(level,obj,**aa)
    if global_permissive_output:
        print("suppressed log, \""+level+"\" while",logstash_levels,",:",end="")
        log_in_context(level,obj,**aa)

def log_in_context(level,obj,**aa):
    if obj._da_locally_complete is not None:
        aa['graph']=obj._da_locally_complete
    else:
        aa['graph']={}

    aa['graph']=base64.b64encode(json.dumps(aa['graph']).encode('utf-8'))
    if 'hashe' in aa:
        aa.pop("hashe")

    aa['target']=obj.get_signature()
    aa['target_full']=obj.get_version()
    log_logstash(level,**aa)

def log_logstash(a,**aa):
    if logstash_levels is None:
        log(a,level="logstash")
    else:
        if logstash_logger is not None:
            if 'message' in aa:
                aa['note']=aa.pop('message')
            aa['action']=a

            if 'origin' not in aa:
                aa['origin']="dda"

            for k,v in list(os.environ.items()):
                if len(v)<20:
                    aa['env_'+k]=v

            logstash_logger.info(aa.get('note',''),extra=aa)


def debug_print(text):
    if global_debug_enabled:
        open("debug.txt","a").write(text+"\n")

class PrintHook:
    """
    this class gets all output directed to stdout(e.g by print statements)
    and stderr and redirects it to a user defined function

    out = 1 means stdout will be hooked
    out = 0 means stderr will be hooked
    """

    def __init__(self,out=1,n=""):
        self.n=n
        self.func = None # self.func is userdefined function
        self.origOut = None
        self.out = out

    def __repr__(self):
        return "["+self.__class__.__name__+" for "+self.n+"]"

    def Start(self,func):
        if self.out:
            self.origOut = sys.stdout
            sys.stdout = self
        else:
            self.origOut = sys.stderr
            sys.stderr= self
            
        self.func = func

    def Stop(self):
        if hasattr(self,'text') and self.text!="":
            self.write(self.text,last=True)

        #open("file.txt","a").write(repr(self)+"::::stopping\n")

        self.get_origOut().flush()
        if self.out:
            sys.stdout =  self.origOut
        else:
            sys.stderr =  self.origErr
        #open("file.txt","a").write(repr(self)+"::::stopping to %s\n"%sys.stdout)
        self.func = None

    #override write of stdout        
    def write(self,text,last=False):
        try:
            raise Exception("Dummy")
        except:
            lineText =  str(sys.exc_info()[2].tb_frame.f_back.f_lineno)
            codeObject = sys.exc_info()[2].tb_frame.f_back.f_code
            fileName = codeObject.co_filename
            funcName = codeObject.co_name


        if not hasattr(self,'text'):
            self.text=""

        self.text+=text

        lines=self.text.split("\n")
        linestoprint=lines if last else lines[:-1]

        self.text=lines[-1]



        for l in linestoprint:
            r=self.func(l.strip(),fileName,lineText,funcName)
            if r.strip()!="":
                self.get_origOut().write(r+"\n")




    def get_origOut(self):
        try:
            return self.origOut.get_origOut()
        except:
            return self.origOut
                
    def __getattr__(self, name):
        return getattr(self.get_origOut(),name)


def decorate_method_log(f):
    try:
        if f.__name__=="__repr__":
            return f
    except:
        return f

    def nf(s,*a,**b):

        def HookedOutputLine(text,fileName,lineText,funcName):
            if hasattr(s,'default_log_level') and s.default_log_level is not None:
                text+='{log:%s}'%s.default_log_level

            ct=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-5]

            processed_text=render('{CYAN}'+ct+'{/} ')+'[%10s'%render("{BLUE}"+fileName[-10:].strip()+":%4s"%lineText+"{/}")+ \
                                 render("{YEL}%20s{/}"%repr(s)[:20])+ \
                                 '; '+render("{CYAN}%10s"%funcName[:10]+"{/}")+': '+\
                                 text

            passing_output=""
            for l in LogStreams:
                o=l.process(processed_text)
                if isinstance(o,str):
                    passing_output+=o

            return passing_output

        
        def MyHookErr(text):
            return ""

        phOut = PrintHook(n=repr(f))
        phOut.Start(HookedOutputLine)
    
        try:
            function_return=f(s,*a,**b)
        except Exception as e:
            phOut.Stop()
            raise
    
        phOut.Stop()

        return function_return

    return nf


class LogStream(object):
    def __init__(self,target,levels,name=None):
        self.target=target
        self.levels=levels
        self.name=name

    def register(self):
        for i,l in enumerate(LogStreams):
            if self.target==l.target:
                LogStreams[i]=self
                return
        LogStreams.append(self)

    def forget(self):
        if self not in LogStreams:
            log("unable to forget this:",self,"while available streams:",LogStreams,level="WARNING")
        else:
            LogStreams.remove(self)

    def check_levels(self,inlevels):
        if self.levels is None:
            return True

        if isinstance(self.levels,list):
            # exclusive levels
            raise Exception("not implememtned")
        if callable(self.levels):
            return self.levels(inlevels)



    def process(self,text):

        levels=re.findall("{log:(.*?)}",text)
        text=re.sub("{log:(.*?)}","",text)

        debug_print(repr(self)+": "+text)

        if self.check_levels(levels) or global_permissive_output:
            return self.output(text)


    def output(self,text):
        if self.target is None: # or global_all_output:
        #if self.target is None or global_all_output:
            return text

        if hasattr(self.target,'write'):
            self.target.write(text+"\n")
            return

        if isinstance(self.target,str):
            self.targetfn=self.target
            self.target=open(self.target,"a")
            return self.output(text)

        raise Exception("unknown target in logstream:"+repr(self.target))

    def __repr__(self):
        r=super(LogStream,self).__repr__()
        if self.name is not None:
            r+=": "+self.name
        r += "; target: "+repr(self.target)
        if hasattr(self.target,'name'):
            r+=": "+self.target.name
        return "["+r+"]"

LogStreams=[]

def reset(comment="at init"):
    global LogStreams
    LogStreams=[LogStream(sys.stdout, levels=None, name="original stdout "+comment)]

reset()

