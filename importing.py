import imp
import os
import shutil
import sys

from caches import core
from dataanalysis import DataAnalysis, DataFile
from hashtools import *
from printhook import cprint


class find_module_standard(DataAnalysis):
    cached=False # never    
    input_module_name=None

    def main(self):
        cprint("find module",self.input_module_name)

        module_name=self.input_module_name.handle
        self.found=imp.find_module(module_name,["."]+sys.path)
        fp, pathname, description=self.found
        
        cprint("will search in",["."]+sys.path)

        cprint("found as",pathname)

        self.module_path=pathname

cm= core.CacheModule()
#cmi=caches.CacheModuleIRODS()
#cm.parent=cmi

class find_module_cached(DataAnalysis):
    cached=True
    cache=cm

    allow_timespent_adjustment=False
    hard_timespent_checks=False

    input_module_name=None
    input_module_version=None

    def main(self):
        cprint("find module as ",self.input_module_name)

        pathname=self.input_module_name.handle+".py"

        if not os.path.exists(pathname):
            pathname=imp.find_module(self.input_module_name.handle)[1]
            print(pathname)

        hashedfn=self.input_module_name.handle+"."+self.get_version()+"."+hash_for_file(open(pathname))[:8]+".py"


        shutil.copyfile(pathname,hashedfn)

        cprint("found as",pathname)
        cprint("will store as",hashedfn)

        self.module_path=hashedfn
        self.module_file=DataFile(hashedfn)


class load_module(DataAnalysis):
    cached=False # never    
    input_module_path=None

    def main(self):
        cprint("load module",self.input_module_path.input_module_name.handle)
        cprint("load as",self.input_module_path.module_path)

#        self.module = __import__(self.input_module_path.module_path)
        if not os.path.exists(self.input_module_path.module_path):
            raise Exception("can not open: "+self.input_module_path.module_path)

        self.module=imp.load_source(self.input_module_path.input_module_name.handle,self.input_module_path.module_path)
        #self.module=imp.load_module(,*self.input_module_path.found)

def import_git_module(name,version):
    gitroot=os.environ["GIT_ROOT"] if "GIT_ROOT" in os.environ else "git@github.com:volodymyrss"
    netgit=os.environ["GIT_COMMAND"] if "GIT_COMMAND" in os.environ else "git"
    
    os.system(netgit+" clone "+gitroot+"/dda-"+name+".git")
    os.system("cd dda-"+name+"; "+netgit+" pull; git checkout "+version)
    print name,os.getcwd()+"/dda-"+name+"/"+name+".py"
    return imp.load_source(name,os.getcwd()+"/dda-"+name+"/"+name+".py")

def load_by_name(m):
    if m.startswith("/"):
        cprint("will import modul from cache")
        ms=m[1:].split("/",1)
        if len(ms)==2:
            m0,m1=ms
        else:
            m0=ms[0]
            m1="master"

        cprint("as",m0,m1)
        result=import_analysis_module(m0,m1),m0
        result[0].__dda_module_global_name__=(m0,m1)
        return result
    elif m.startswith("git://"):
        cprint("will import modul from cache")
        ms=m[len("git://"):].split("/")

        if len(ms)==2:
            m0,m1=ms
        else:
            m0=ms[0]
            m1="master"

        cprint("as",m0,m1)
        result=import_git_module(m0,m1),m0
        result[0].__dda_module_global_name__=(m0,m1)
        return result
    else:
        fp, pathname, description=imp.find_module(m,["."]+sys.path)
        cprint("found as",  fp, pathname, description)
        return imp.load_module(m,fp,pathname,description), m

    return load_module(input_module_path=find_module_standard(input_module_name=name)).get().module

def import_analysis_module(name,version):
    return load_module(input_module_path=find_module_cached(input_module_name=name,input_module_version=version)).get().module


