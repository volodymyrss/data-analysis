import sys
from bcolors import render

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
