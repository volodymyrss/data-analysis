import imp
import os
import shutil
import sys

from dataanalysis.caches import cache_core
from dataanalysis.core import DataAnalysis, DataFile
from dataanalysis.hashtools import *
from printhook import log
import analysisfactory

def _import_git_module(name,version,local_gitroot=None,remote_git_root=None,preserve_found=False):
    if remote_git_root == "any":
        return _import_git_module(name, version, local_gitroot, ["volodymyrss-private","volodymyrss-public"])


    if isinstance(remote_git_root,list):
        exceptions=[]
        for try_remote_git_root in remote_git_root:
            try:
                log("try to import with",remote_git_root)
                return _import_git_module(name, version, local_gitroot, try_remote_git_root)
            except Exception as e:
                log("failed to import",e)
                exceptions.append(e)

        raise Exception("failed to import from git",exceptions)


    if local_gitroot is None:
        local_gitroot=os.getcwd()

    gitroot=os.environ["GIT_ROOT"] if "GIT_ROOT" in os.environ else "git@github.com:volodymyrss"
    if remote_git_root is not None:
        if remote_git_root=="volodymyrss-public":
            gitroot="https://github.com/volodymyrss"
        elif remote_git_root == "volodymyrss-private":
            gitroot="git@github.com:volodymyrss"

    netgit=os.environ["GIT_COMMAND"] if "GIT_COMMAND" in os.environ else "git"

    local_module_dir=local_gitroot+"/dda-"+name

    print("local git clone:",local_module_dir)


    local_module_tag=local_module_dir + "/valid_version"
    module_file=local_module_dir + "/" + name + ".py"
    if preserve_found and os.path.exists(local_module_tag) and open(local_module_tag).read().strip()==version and os.path.exists(module_file):
        print("module already found!")
    else:
        cmd=netgit+" clone "+gitroot+"/dda-"+name+".git "+local_module_dir
        print(cmd)
        os.system(cmd)
        cmd="cd " + local_module_dir + "; " + netgit + " pull; git checkout " + version
        print(cmd)
        os.system(cmd)
        open(local_module_dir+"/valid_version","w").write(version)

    print(name,module_file)
    m=imp.load_source(name,module_file)
    return m


def load_by_name(m, local_gitroot=None,remote_git_root='any'):
    log("requested to load by name:",m)
    if isinstance(m,list):
        if m[0]=="filesystem":
            if m[2] is not None:
                name=m[1]

                fullpath=m[2].replace(".pyc",".py")

                module=imp.load_source(name, fullpath)
                #analysisfactory.AnalysisFactory.assume_module_used(module)
                return module, name
            else:
                m=m[1]
                log("using generic load from filesystem:",m[1])
        else:
            m=m[0]+"://"+m[1]
            log("loading with provider:",m)

    if m.startswith("git://"):
        log("will import modul from cache")
        ms=m[len("git://"):].split("/") # or @

        if len(ms)==2:
            m0,m1=ms
        else:
            m0=ms[0]
            m1="master"

        log("as",m0,m1)
        result=_import_git_module(m0,m1,local_gitroot=local_gitroot,remote_git_root=remote_git_root),m0
        result[0].__dda_module_global_name__= m
        result[0].__dda_module_origin__ = "git"
        #analysisfactory.AnalysisFactory.assume_module_used(result[0])
        return result
    else:
        fp, pathname, description=imp.find_module(m,["."]+sys.path)
        log("found as",  fp, pathname, description)
        module=imp.load_module(m, fp, pathname, description)
       # analysisfactory.AnalysisFactory.assume_module_used(module)
        return module,m

#    return load_module(input_module_path=find_module_standard(input_module_name=name)).get().module
