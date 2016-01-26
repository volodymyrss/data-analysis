from __future__ import print_function

import sys
from bcolors import render
from datetime import datetime
import re

global_suppress_output=False
global_fancy_output=True
global_output_levels=['top','cache']
global_permissive_output=False
global_all_output=False

if not hasattr(print,'replaced'):
    sprint=print
    def print(*a,**aa):
        if global_suppress_output:
            return
        else:
            level=aa['level'] if 'level' in aa else None 
            #if level in global_output_levels:
            if ((level is None) and global_permissive_output) or level in global_output_levels:
                return sprint(level,*a)
    print.replaced=True

#this class gets all output directed to stdout(e.g by print statements)
#and stderr and redirects it to a user defined function
class PrintHook:
    #out = 1 means stdout will be hooked
    #out = 0 means stderr will be hooked
    def __init__(self,out=1,n=""):
        self.n=n
        self.func = None##self.func is userdefined function
        self.origOut = None
        self.out = out

    def __repr__(self):
        return "for "+self.n

    def Start(self,func):
        if self.out:
            #open("file.txt","a").write(repr(self)+"::::starting: from %s\n"%(sys.stdout))
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
            raise "Dummy"
        except:
            lineText =  str(sys.exc_info()[2].tb_frame.f_back.f_lineno)
            codeObject = sys.exc_info()[2].tb_frame.f_back.f_code
            fileName = codeObject.co_filename
            funcName = codeObject.co_name


        if not hasattr(self,'text'):
            self.text=""

        self.text+=text
        

        #open("file.txt","a").write(repr(self)+"::::input: %s\n"%repr(text))
        #open("file.txt","a").write(repr(self)+":::: last:"+repr(last)+"\n")
        #open("file.txt","a").write(repr(self)+"::::stored: %s\n"%repr(self.text))

        lines=self.text.split("\n")
        linestoprint=lines if last else lines[:-1]

        #open("file.txt","a").write(repr(self)+"::::: all lines " +repr(lines)+"\n")
        #open("file.txt","a").write(repr(self)+"::::: flushing lines " +repr(linestoprint)+"\n")
        
        self.text=lines[-1]
        #open("file.txt","a").write(repr(self)+"::::stored last: %s\n"%repr(self.text))


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
        #open("file.txt","a").write("decorate method of"+repr(s)+repr(f))

        def MyHookOut(text,fileName,lineText,funcName):
            # find 
            if hasattr(s,'default_log_level') and s.default_log_level is not None:
                text+='{log:%s}'%s.default_log_level

            ct=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-5]

            processed_text=render('{CYAN}'+ct+'{/} ')+'[%10s'%render("{BLUE}"+fileName[-10:].strip()+":%4s"%lineText+"{/}")+ \
                                 render("{YEL}%20s{/}"%repr(s)[:20])+ \
                                 '; %20s'%render("{CYAN}"+funcName[:20]+"{/}")+': '+\
                                 text
            
            r=""
            for l in LogStreams:
                o=l.process(processed_text)
                if isinstance(o,str):
                    r+=o

            return r

        
        def MyHookErr(text):
            return ""

        if global_fancy_output:
            phOut = PrintHook(n=repr(f))
            phOut.Start(MyHookOut)
        
            try:
                r=f(s,*a,**b)
            except Exception as e:
                phOut.Stop()
                raise
        
            phOut.Stop()
        else:
            r=f(s,*a,**b)

        return r

    return nf


class LogStream:
    def __init__(self,target,levels):
        self.target=target
        self.levels=levels

        for i,l in enumerate(LogStreams):
            if target==l.target:
                LogStreams[i]=self
                return
        LogStreams.append(self)

    def forget(self):
        LogStreams.remove(self)

    def check_levels(self,inlevels):
        if isinstance(self.levels,list):
            # exclusive levels
            raise Exception("not implememtned")
        if callable(self.levels):
            return self.levels(inlevels)

    def process(self,text):
        levels=re.findall("{log:(.*?)}",text)
        text=re.sub("{log:(.*?)}","",text)

        if self.check_levels(levels):
        #if any([l in levels for l in self.levels]):
            return self.output(text)

    def output(self,text):
        if self.target is None or global_all_output:
            return text
        if hasattr(self.target,'write'): # callable?
        #if isinstance(self.target,file) or:
            self.target.write(text+"\n")
            return
        if isinstance(self.target,str):
            self.targetfn=self.target
            self.target=open(self.target,"a")
            return self.output(text)
        raise Exception("unknown target in logstream:"+repr(self.target))

LogStreams=[]

cprint=print
