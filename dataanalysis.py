from __future__ import print_function
from collections import Mapping, Set, Sequence 
from bcolors import render as render
from hashlib import sha224
import hashlib
import pprint
import time,gzip,cPickle,socket,sys,os,shutil,re,copy
from subprocess import check_call
import subprocess
import collections
import StringIO
from datetime import datetime


# TODO:

# optional input
# cache migration


# delegation simple, in scripts

# data intergity: prevent corruption during copying to/from hashe

# transient cache no work as expected? call same analysis?

# cache-only analysis
# cache migration
# hashe operations: hashe spearation

# need more flexible system to interpret object:
#    interpret aliases

#  exceptions handle better
#  output hash in never-cached objects
#  timestamps 
#  more nested caches!

# there are several ways to construct new analysis
#  - inherit class from dataanalysis. identified by class name  
#  - create an object with arguments. by class name and arguments (_da_attributes)

# translation of subgraphs: rewrite rules

# add advanced verification while setting arguments: do not allow objects of DA (or treat them?) and big data

# an option for "testing" state object, forces all requesters to recompute



global_log_enabled=True
global_fancy_output=True
global_suppress_output=False
global_all_output=False
global_readonly_caches=False
global_output_levels=('top')


sprint=print
def print(*a,**aa):
    if global_suppress_output:
        return
    else:
        level=aa['level'] if 'level' in aa else None 
        if level is None or level in global_output_levels:
            return sprint(*a)

#

#AnalysisDict={}

def shhash(x):
    return sha224(str(hash(x))).hexdigest()

# dual python 2/3 compatability, inspired by the "six" library
string_types = (str, unicode) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()


class DataHandle:
    pass

class DataFile:
    pass

            
def isdataanalysis(obj,alsofile=False):
    if isinstance(obj,DataFile) and not alsofile:
        return False
    if isinstance(obj,DataAnalysis):
        return True
    if isinstance(obj,type) and issubclass(obj,DataAnalysis):
        return True
    return False

def objwalk(obj, path=(), memo=None, sel=lambda x:True):
    #if global_log_enabled: print("walking in",obj,path)
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator:
        #if global_log_enabled: print("walking interate")
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
         #       if global_log_enabled: print("walking",path,path_component,value)
                for result in objwalk(value, path + (path_component,), memo, sel):
                    yield result
            memo.remove(id(obj))
    else:
        #if global_log_enabled: print("walk end",path,obj)
        if sel(obj):
         #   if global_log_enabled: print("walk selected",path,obj)
            yield obj

#/objwalk

def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate

AnalysisFactory=None

class DataAnalysis:
    pass

class decorate_all_methods(type):
    def __new__(cls, name, bases, local):
        # also store in the dict

        # decorate
        if global_fancy_output:
            for attr in local:
                value = local[attr]
                if callable(value) and not isinstance(value,type):
                    local[attr] = decorate_method(value)

        c=type.__new__(cls, name, bases, local)

        # and we want to registed analysis
        if AnalysisFactory is not None and issubclass(c,DataAnalysis) and c.infactory:
            if global_log_enabled: print("declaring analysis class",name,) #,"version",c.version,"as",c)
            if global_log_enabled: print("   constructing object...")
            o=c(update=True) # test
            if global_log_enabled: print("   registered",o)
            #AnalysisFactory.put(c,name)
        return c

from printhook import PrintHook

def decorate_method(f):

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
                                 render("{YEL}%30s{/}"%repr(s))+ \
                                 '; %30s'%render("{CYAN}"+funcName+"{/}")+': '+\
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

class AnalysisFactoryClass: # how to unify this with caches?..
# handles all object equivalence
    __metaclass__ = decorate_all_methods
# dictionary of current object names, aliases?..
    cache={} 
    dda_modules_used=[]

    def __repr__(self):
        return "[AnalysisFactory: %i]"%len(self.cache)

    def put(self,obj,sig=None): 
        if global_log_enabled: print("requested to put in factory:",obj,sig)  
        if global_log_enabled: print("factory assumptions:",self.cache_assumptions)  


        if not obj.infactory:
            if global_log_enabled: print("object is not in-factory, not putting")
            return obj

        if obj.assumptions!=[]:
            print("object has assumptions:",obj,obj.assumptions)
            raise Exception("can not store in cache object with assumptions")

        module_record=sys.modules[obj.__module__]
        if self.dda_modules_used==[] or self.dda_modules_used[-1]!=module_record:
            self.dda_modules_used.append(module_record)

        if isinstance(obj,type):
            if global_log_enabled: print("requested to put class, it will be constructed")
            obj=obj()

        sig=obj.get_signature() if sig is None else sig # brutal
        if global_log_enabled: print("put object:",obj,"signature",sig)
        saved=None
        if sig in self.cache:
            saved=self.cache[sig]
        self.cache[sig]=obj
        return saved

    def get(self,item,update=False): 
        """
        generates and instance of DataAnalysis from something
   
        """
        if global_log_enabled: print("interpreting",item)
        if global_log_enabled: print("factory knows",self.cache) #.keys())

        
        if item is None:
            if global_log_enabled: print("item is None: is it a virtual class? should not be in the analysis!")
            raise Exception("virtual class, class with None inputs, is not allowed directly in the analysis")

        
        if isinstance(item,type) and issubclass(item,DataAnalysis):
            if global_log_enabled: print("is subclass of DataAnalysis, probably need to construct it")
            name=item.__name__
            if global_log_enabled: print("class name:",name)
            if name in self.cache:
                c=self.cache[name]
                if global_log_enabled: print("have cache for this name:",c)
                if isinstance(item,type):
                    if global_log_enabled: print("it is class, constructing")
                    c=c()
                    if global_log_enabled: print("constructed",c)
                    self.put(c)
                    #if global_log_enabled: print("will store",c)
                    return c
                if isinstance(item,DataAnalysis):
                    if global_log_enabled: print("cache returns object, will use it:",c)
                    return c
            else:
                if global_log_enabled: print("there is no such class registered!",name)
                raise Exception("there is no such class registered: "+name+" !")
        
        if isinstance(item,DataAnalysis): 
            if global_log_enabled: print("is instance of DataAnalysis, signature",item.get_signature())
        
            if isinstance(item,DataHandle) or isinstance(item,DataFile):  # make as object call
                if global_log_enabled: print("is datahandle or file, returning",item)
                return item

            if not item.infactory:  # make as object call
                if global_log_enabled: print("is not in-factory, returning")
                return item
            
            s=item.get_signature()
            if s in self.cache:
                storeditem=self.cache[item.get_signature()]
    
                if isinstance(storeditem,type):
                    raise Exception("no classes can be stored!")

                #if item!=storeditem: # object was constructed during the declaration of the class or interpreted earlier, either way the ___new__ call had to guarantee it is the same. it has to be the same
                #    raise Exception("critical violation of sanity check! object was constructed twice "+str(item)+" vs "+str(storeditem))

                if global_log_enabled: print("so, offered object:",item)
                if global_log_enabled: print("     stored object:",storeditem)

                if not item.is_virtual(): # careful!
                    if global_log_enabled: print("     offered object is non-virtual, simply returning") 
                    if global_log_enabled: print("     offered object complettion:",item._da_locally_complete) 
                    if global_log_enabled: print("     offered virtual reason:",item._da_virtual_reason if hasattr(item,'_da_virtual_reason') else "") 
                    #if global_log_enabled: print("     offered object is non-virtual, forcing it") 
                    #update=True #!!!!
## think about this! update??
                    return item
                
                if len(item.assumptions)!=0:
                    if global_log_enabled: print("     offered object has assumptions",item.assumptions) 
                    if global_log_enabled: print("     offered object complettion:",item._da_locally_complete) 
                    if global_log_enabled: print("     will copy and assign assumptions") 

                    copied_storeditem=storeditem.__class__(dynamic=False)
                    copied_storeditem.assumptions=item.assumptions

                    return copied_storeditem

                if update:
                    if global_log_enabled: print("recommendation is to force update")
                    self.put(item)
                    return item
                else:
                    if global_log_enabled: print("attention! offered object is discarded:",item ,item.export_data())
                    if global_log_enabled: print("stored in factory (this will be the valid instance):",storeditem,storeditem.export_data())
                    return storeditem

            if global_log_enabled: print("no such object registered! registering") # attention!
            self.put(item)
            return item

        if isinstance(item,str):
            if global_log_enabled: print("considering string data handle")
            return DataHandle(item)
         
        if global_log_enabled: print("unable to interpret item: "+repr(item))
        return None

    cache_stack=[]
    cache_assumptions=[]
    comment=""

    def WhatIfCopy(self,description,new_assumptions):
        if global_log_enabled: print(render("{RED}go deeper! what if?{/} stack of size %i"%len(self.cache_stack)))
        if self.comment==description:
            if global_log_enabled: print("will not push copy, already in the assumption",description)
            return

        if global_log_enabled: print("pushing into stack current version, using copy",description)
        self.comment=description
        self.cache_stack.append(self.cache)
        self.cache_assumptions.append(new_assumptions)
        self.cache={} # this makes assumptions reset
        if global_log_enabled: print("cache stack of size",len(self.cache_stack))
        if global_log_enabled: print("cache stack last entry:",self.cache_stack[-1])

        for i,o in self.cache_stack[-1].items():
            if global_log_enabled: print("promoting",i,'assumptions',o.assumptions)
            #if o.is_virtual():
            #    if global_log_enabled: print("virtual object, constructing empty copy")
            o.__class__(dynamic=False).promote() # assume??

        for assumptions in self.cache_assumptions:
            print("assumption group:",assumptions)
            for a in assumptions:
                print("assumption",a)
                a.promote()

         #   else:
         #       if global_log_enabled: print("non-virtual object! promoting object itself")
         #       o.promote() # assume??
        #self.cache=copy.deepcopy(self.cache_stack[-1]) # dangerous copy: copying call __new__, takes them from current cache
        if global_log_enabled: print("current cache copied:",self.cache)

    def WhatIfNot(self):
        if global_log_enabled: print("factory knows",self.cache) #.keys())
        if self.cache_stack==[]:
            if global_log_enabled: print("empty stack!")       
            return
            #raise Exception("empty stack!")       

        if global_log_enabled: print("poping from stack last version")
        self.cache=self.cache_stack.pop()
        assumptions=self.cache_assumptions.pop()
        if global_log_enabled: print("discarding last stacked",self.comment)
        if global_log_enabled: print("discarding:",assumptions)
        if global_log_enabled: print("current assumptions level %i:"%len(self.cache_assumptions),self.cache_assumptions[-1] if self.cache_assumptions!=[] else "none")
        self.comment=""
        if global_log_enabled: print("factory knows",self.cache) #.keys())


    def byname(self,name):
        if name not in self.cache:
            raise Exception("name is not known, can not get this: "+name)
        return self.cache[name]

    def __getitem__(self,name):
        return self.byname(name)
    
    def __iter__(self):
        for i in self.cache.keys():
            yield i

    aliases=[]

    def register_alias(self,h1,h2):
        if self.aliases is None: self.aliases=[]
        self.aliases.append((h1,h2))

    def list_relevant_aliases(self,obj):
        h0=obj.get_version()

        def contains(graph,key):
            if graph==key: return True
            if isinstance(graph,tuple):
                if graph[0]=='analysis':
                    if graph[2]==key: return True
                    return contains(graph[1],key)
                if graph[0]=='list':
                    return any([contains(k,key) for k in graph[1:]])

        print('aliases:',self.aliases)
        return self.aliases   

     #   return [[a,b] for a,b in self.aliases if contains(h0,a)]

    def get_module_description(self):
        module_description=[]
        for m in AnalysisFactory.dda_modules_used:
            if global_log_enabled: print("module",m)
            if hasattr(m,"__dda_module_global_name__"):
                if global_log_enabled: print("dda module global name", m.__dda_module_global_name__)
                module_description.append(['cache',m.__name__,m.__dda_module_global_name__])
            else:
                module_description.append(['filesystem',m.__name__,m.__file__])

        return module_description

AnalysisFactory=AnalysisFactoryClass()



def hashe_replace_object(hashe,what,witha):
    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return ('analysis',hashe_replace_object(hashe[1],what,witha),hashe[2])
        if hashe[0]=='list':
            return ('list',)+tuple([hashe_replace_object(h,what,witha) for h in hashe[1:]])
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve tpule!")
    if hashe==what: return witha
    return hashe

def hashe_list_objects(hashe):
    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return [hashe[2]]+hashe_list_objects(hashe[1])
        if hashe[0]=='list':
            l=[]
            for h in hashe[1:]:
                l+=hashe_list_objects(h)
            return l
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve tpule!")
    return []

#LogStream(None,lambda x:True)                                                                                               
#LogStream("alllog.txt",lambda x:True)

class FileBackend:
    def exists(self,fn):
        return os.path.exists(fn)

    def getsize(self,origin):
        return os.path.getsize(origin)/1024./1024.
    
    def get(self,orig,dest,gz=False):
        print(orig,"to",dest)
        if gz:
            open(dest,"w").write(gzip.open(orig).read())
        else:
            shutil.copy(orig,dest)
    
    def symlink(self,orig,dest):
        os.symlink(orig,dest)
    
    def put(self,orig,dest):
        shutil.copy(orig,dest)
    
    def makedirs(self,dirs):
        os.makedirs(dirs)

    def open(self,fn,mode="r",gz=False):
        if gz:
            return gzip.open(fn,mode)
        return open(fn,mode)

    def flush(self):
        pass

class MemCache: #d
    # currently store each cache in a file; this is not neccesary 
    __metaclass__ = decorate_all_methods
    cache={}
    filecacheroot="./filecache"

    parent=None

    readonly_cache=False

    filebackend=FileBackend()

    def statistics(self):
        pass

    def hashe2signature(self,hashe_raw):
        hashe=hashe_replace_object(hashe_raw,None,"None")
        if global_log_enabled: print("hashe:",hashe)
        if isinstance(hashe,tuple):
            if hashe[0]=="analysis":
                return hashe[2]+":"+shhash(hashe)[:8]
        sig=shhash(hashe)[:8]
        if global_log_enabled: print("signature hashe:",sig)
        return sig

    def __init__(self,rootdir=None):
        if rootdir is not None:
            self.filecacheroot=rootdir

    def __repr__(self):
        return "["+self.__class__.__name__+" of size %i]"%len(self.cache.keys())

    def find(self,hashe):
        #for c in self.cache.keys():
            #if global_log_enabled: print("cache:",c)

        self.load()

        if hashe in self.cache:
            fi=self.cache[hashe]
            if global_log_enabled: print("{log:cache}","cache found!",fi)
            return fi
            
        if global_log_enabled: print("found no cache for",hashe)

        return None
    
    def runtime_update(self,obj,c):
        pass
        #if self.parent is not None:
        #    self.parent.runtime_unregister(obj)
    
    def restore_from_parent(self,hashe,obj,rc=None):
        if self.parent is None:
            if global_log_enabled: print("no parent available to call for")
            return None
        
        if global_log_enabled: print("there is a parent available to call for:",self.parent)
        return self.parent.restore(hashe,obj,rc)

    def store_to_parent(self,hashe,obj):
        if self.parent is None:
            if global_log_enabled: print("no parent available to call for")
            return None
        
        if global_log_enabled: print("there is a parent available to call for:",self.parent)
        return self.parent.store(hashe,obj)


    def load_content(self,hashe,c):
        cached_path=self.construct_cached_file_path(hashe,c)
                    
        if global_log_enabled: print("restoring from",cached_path+"/cache.pickle.gz")

        try:
            if global_log_enabled: print("loading from pickle")
            content=cPickle.load(self.filebackend.open(cached_path+"/cache.pickle.gz",gz=True))
            if global_log_enabled: print("done loading from pickle")
            return content
        except IOError,cPickle.UnpicklingError:
            if global_log_enabled: print("problem loading cache! corrupt cache!")
            raise

    def restore_file(self,origin,dest,obj,hashe):
       # statistics 
        if global_log_enabled: print("restore file:")
        if global_log_enabled: print("< ",origin,'{log:top}')
        if global_log_enabled: print("> ",dest,'{log:top}')

        dest_unique=dest+"."+self.hashe2signature(hashe)
        
        if global_log_enabled: print("as",dest_unique)

        fsize=self.filebackend.getsize(origin)/1024./1024.
        if global_log_enabled: print("restoring file of",fsize,'{log:resources}','{log:cache}')

        t0=time.time()
        #check_call("gunzip -c "+origin+" > "+dest_unique,shell=True) # just by name? # gzip optional
        self.filebackend.get(origin,dest_unique,gz=True)

        #shutil.copyfile(origin,dest_unique) # just by name? # gzip optional
        #shutil.copyfile(origin,dest) # just by name? # gzip optional
        tspent=time.time()-t0

        if global_log_enabled: print("restoring took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        #t0=time.time()
        #check_call(['gunzip','-f',dest_unique])
        tspentc=0
        #if global_log_enabled: print("compressing took",tspentc,"seconds, speed",fsize/tspentc,'MB/s','{log:resources}','{log:cache}')

        if global_log_enabled: print("here should verify integrity")
        if global_log_enabled: print("successfully restored:",dest_unique)

        if os.path.exists(dest):
            if global_log_enabled: print("destination exists:",dest)
            savedname=dest+"."+time.strftime("%s")
            if global_log_enabled: print("saving as",savedname)
            shutil.move(dest,savedname)

        shutil.copyfile(dest_unique,dest)
        #check_call(['ln',dest_unique,dest])
        
        if global_log_enabled: print("successfully copied to",dest)

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc}


    def store_file(self,origin,dest):
        if global_log_enabled: print("store file:")
        if global_log_enabled: print("< ",origin,'{log:top}')
        if global_log_enabled: print("> ",dest,'{log:top}')

        fsize=self.filebackend.getsize(origin)/1024./1024.
        if global_log_enabled: print("storing file of",fsize,'{log:resources}','{log:cache}')

# note that this leaves files!!

        t0=time.time()
        os.system("gzip -c %s > %s.gz"%(origin,origin))
        #check_call(['gzip','-f',origin])
        tspentc=time.time()-t0
        if global_log_enabled: print("compressing took",tspentc,"seconds, speed",fsize/tspentc,'MB/s','{log:resources}','{log:cache}')

        t0=time.time()
        self.filebackend.put(origin+".gz",dest+".gz") # just by name? # gzip optional
        #shutil.copyfile(origin+".gz",dest+".gz") # just by name? # gzip optional
        tspent=time.time()-t0

        if global_log_enabled: print("storing took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc}

    def restore(self,hashe,obj,restore_config=None):
        # check if updated
        if obj.run_for_hashe or obj.mutating:
            return 

        if not any([isinstance(self,c) for c in obj.read_caches]):
            if global_log_enabled: print("cache "+repr(self)+" should not be read by this analysis, allowed: "+repr(obj.read_caches))
            return self.restore_from_parent(hashe,obj)

        if restore_config is None:
            restore_config={}
        restore_config_default=dict(datafile_restore_mode="copy",datafile_target_dir=None) # no string
        restore_config=dict(restore_config_default.items()+(restore_config.items() if restore_config is not None else []))

        if obj.datafile_restore_mode is not None:
            restore_config['datafile_restore_mode']=obj.datafile_restore_mode

        c=self.find(hashe)
        if c is None:
            if global_log_enabled: print("normal restore failed.")
            return self.restore_from_parent(hashe,obj)

        if global_log_enabled: print("requested to restore cache")
        cached_path=self.construct_cached_file_path(hashe,obj)
        if global_log_enabled: print("cached path:",cached_path)

        obj._da_cache_path_root=cached_path

        try:
            c=self.load_content(hashe,c)
        except Exception as e:
            if global_log_enabled: print("can not laod content from cache, while cache record exists! inconsistent cache!") #???
            #raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!") # ???
            return self.restore_from_parent(hashe,obj)

        if isinstance(c,dict):
            for a,b in c.items(): 
                if isinstance(b,DataFile):
                    if global_log_enabled: print("requested to restore DataFile",b,"mode",restore_config['datafile_restore_mode'],'{log:top}')

                    prefix=restore_config['datafile_target_dir']
                    if prefix is not None:
                        prefix=prefix+"/"
                        try:
                            os.makedirs(prefix)
                        except:
                            pass
                    else:
                        prefix=""

                    stored_filename=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional
                    if not self.filebackend.exists(stored_filename): # and
                        if global_log_enabled: print("file from cache does not exist, while cache record exists! inconsistent cache!",stored_filename)
                        return None

                    # other way
                    if restore_config['datafile_restore_mode']=="copy":
                        try:
 #                           stored_filename=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional

                            if not self.filebackend.exists(stored_filename):
                                if global_log_enabled: print("can not copy from from cache, while cache record exists! inconsistent cache!",stored_filename)
                                #raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                                # just reproduce?
                                return None

                            if global_log_enabled: print("stored file:",stored_filename,"will save as",prefix+b.path+".gz") 

                            b.restore_stats=self.restore_file(stored_filename,prefix+b.path,obj,hashe)
                            obj.note_resource_stats({'resource_type':'cache','resource_source':repr(self),'filename':b.path,'stats':b.restore_stats,'operation':'restore'})

                        except IOError:
                            if IOError.errno==20:
                                if global_log_enabled: print("can not copy from from cache, while cache record exists! inconsistent cache!")
                      #          raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                                # just reproduce?
                                return None
                        except subprocess.CalledProcessError:
                            if global_log_enabled: print("can not copy from from cache, while cache record exists! inconsistent cache!")
                 #           raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                            # just reproduce?
                            return None
                    elif restore_config['datafile_restore_mode']=="symlink":
                        self.filebackend.symlink(cached_path+os.path.basename(b.path)+".gz",prefix+b.path+".gz") # just by name? # gzip optional
                    elif restore_config['datafile_restore_mode']=="urlfile":
                        open(prefix+b.path+".url.txt","w").write(cached_path+os.path.basename(b.path)+".gz"+"\n") # just by name? # gzip optional
                    elif restore_config['datafile_restore_mode']=="urlfileappend":
                        open(prefix+b.path+".urls.txt","a").write(cached_path+os.path.basename(b.path)+".gz"+"\n") # just by name? # gzip optional
                    elif restore_config['datafile_restore_mode']=="url_in_object":
                        b.cached_path=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional
                    else:
                        raise Exception("datafile restore mode not understood!")

                    b.cached_path=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional

            for k,i in c.items():
                setattr(obj,k,i)

            if global_log_enabled: print("restored")
            return True
        raise Exception("content from cache is not dict! "+str(c))


    def store(self,hashe,obj):
        if obj.run_for_hashe or obj.mutating:
            return 
        
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        if global_readonly_caches:
            raise Exception("all caches are readonly!")

        if global_log_enabled: print("requested to store:",hashe)
        if not obj.cached:
            if global_log_enabled: print("the object is declared as non-cached, not storing")
            return
        else:
            if global_log_enabled: print("object",obj,"is cached, storing")
        
        if any([isinstance(self,c) for c in obj.write_caches]):

            if global_log_enabled: print("storing:",hashe)

            if not self.filebackend.exists(self.filecacheroot):
                self.filebackend.makedirs(self.filecacheroot)

            obj._da_stamp=obj.get_stamp() # or in the object?
            
            if not hasattr(self,'cache'):
                self.cache={}

            content=obj.export_data()

            if global_log_enabled: print("content:",content)
                        
            cached_path=self.construct_cached_file_path(hashe,obj)

            obj._da_cached_path=cached_path
            if global_log_enabled: print("storing in",cached_path)
                        
            dn=os.path.dirname(cached_path)
            if not self.filebackend.exists(dn):
                self.filebackend.makedirs(dn)


            cPickle.dump(content,self.filebackend.open(cached_path+"cache.pickle.gz","w",gz=True))
            cPickle.dump(hashe,self.filebackend.open(cached_path+"hash.pickle.gz","w",gz=True))
            self.filebackend.open(cached_path+"hash.txt","w").write(pprint.pformat(hashe)+"\n")
            self.filebackend.open(cached_path+"log.txt.gz","w",gz=True).write(obj._da_main_log_content)
            
     #       aliases=AnalysisFactory.list_relevant_aliases(obj)
    #        if aliases!=[]:
                #open(cached_path+"aliases.txt","w").write("\n".join([("="*80)+"\n"+pprint.pformat(a)+"\n"+("-"*80)+"\n"+pprint.pformat(b)+"\n" for a,b in aliases]))

            self.filebackend.open(cached_path+"modules.txt","w").write(pprint.pformat(AnalysisFactory.get_module_description())+"\n")

            self.filebackend.flush()

            if hasattr(obj,'alias'):
                if global_log_enabled: print('object has alias:',obj.alias)
                open(cached_path+"alias.txt","w").write(pprint.pformat(obj.alias)+"\n")
            else:
                if global_log_enabled: print('object has no alias')


            if isinstance(content,dict):
                for a,b in content.items(): 
                    if isinstance(b,DataFile):
                        if global_log_enabled: print("requested to store DataFile",b)

                        p=cached_path+os.path.basename(b.path)
                        b.cached_path=p+".gz"
                        b.store_stats=self.store_file(b.path,p)
                        obj.note_resource_stats({'resource_type':'cache','resource_source':repr(self),'filename':b.path,'stats':b.store_stats,'operation':'store'})

            import socket

            if global_log_enabled: print("will check if record exists",'{log:top}')
            found=self.find(hashe)
            if found is None:
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content})
            else:
                if global_log_enabled: print("record already found:",found,'{log:reflections}')
                if global_log_enabled: print("these results will be ignored! (why would we do this?..)","{log:reflections}") # current behavior is to replace
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content}) # twice same!
            
        else:
            if global_log_enabled: print("cache "+repr(self)+" should not be written by this analysis, allowed: "+repr(obj.write_caches))

        return self.store_to_parent(hashe,obj)
        
        # and save

    def make_record(self,hashe,content):
        if global_log_enabled: print("make record",hashe,content)
        self.cache[hashe]=content
        self.save()
        if global_log_enabled: print("now entries",len(self.cache))

    def runtime_update(self,hashe,content):
        pass
        #self.make_record(self,hashe,content)

    def construct_cached_file_path(self,hashe,obj):
        if global_log_enabled: print("requested default cached file path")

        def hash_to_path(hashe):
            if isinstance(hashe,tuple):
                if hashe[0]=="analysis": # more universaly
                    return hash_to_path(hashe[2])+"/"+hash_to_path(hashe[1])
                if hashe[0]=="list": # more universaly
                    return "..".join(map(hash_to_path,hashe[1:]))
                raise Exception("unknown tuple in the hash:"+str(hashe))
            if isinstance(hashe,str):
                return hashe
            raise Exception("unknown class in the hash:"+str(hashe))

        def hash_to_path2(hashe):
            #by32=lambda x:x[:8]+"/"+by8(x[8:]) if x[8:]!="" else x
            return hashe[2]+"/"+shhash(repr(hashe[1]))

        return self.filecacheroot+"/"+hash_to_path2(hashe)+"/" # choose to avoid overlapp

        #return self.filecacheroot+"/"+hashe+"/"+os.path.basename(datafile.path)

    def save(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"
        cPickle.dump(self.cache,gzip.open(target,"w"))

    def load(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"

        if os.path.exists(target):
            self.cache=cPickle.load(gzip.open(target))
        else:
            if global_log_enabled: print("file to load does not exist:",target)

    def make_delegation_record(self,hashe,module_description,dependencies):
        return self.parent.make_delegation_record(hashe,module_description,dependencies)

    def register_delegation(self,obj,hashe):
        if global_log_enabled: print("requested to register delegation of",obj)
        if global_log_enabled: print("hashe:",obj)

        if global_log_enabled: print("modules used in dda factory:")
        
        module_description=AnalysisFactory.get_module_description()

        dependencies=obj._da_delegated_input

        print ("will store hashe       ",hashe)
        print ("will store modules     ",module_description,dependencies)
        print ("will store dependencies",dependencies)

        return self.make_delegation_record(hashe,module_description,dependencies)

        # check
        #for m in sys.modules:
        #    if global_log_enabled: print("all modules:",m,sys.modules[m],sys.modules[m].__file__ if hasattr(sys.modules[m],'__file__') else "no file??")


        
import sqlite3 as lite
import sys

class MemCacheSqlite(MemCache):
    cache={}

    def statistics(self):
        if self.con is None:
            if global_log_enabled: print("NOT connected")
        else:
            if global_log_enabled: print("connected to",self.con)

    def connect(self):
        if self.con is None:
            if global_log_enabled: print("connecting to",self.filecacheroot+'/index.db')
            self.con = lite.connect(self.filecacheroot+'/index.db',1000)
        return self.con

    def __init__(self,*a,**aa):
        if global_log_enabled: print(a,aa)
        super(MemCacheSqlite,self).__init__(*a,**aa)
        self.con=None
        #self.connect()

    def list(self,select=None,nlast=None):

        con=self.connect()
        if global_log_enabled: print("listing cache")

        selection_string=""
        if select is not None:
            selection_string=" WHERE "+select # must be string

        nlast_string=""
        if nlast is not None:
            nlast_string=" ORDER BY rowid DESC LIMIT %i"%nlast # must be int

        with con:    
            cur = con.cursor()    

            if global_log_enabled: print("SELECT * FROM cacheindex"+selection_string+nlast_string)

            t0=time.time()
            self.retry_execute(cur,"SELECT * FROM cacheindex"+selection_string+nlast_string)
            rows = cur.fetchall()
            if global_log_enabled: print("mysql request took",time.time()-t0,"{log:top}")


            if global_log_enabled: print("found rows",len(rows))
            for h,c in rows:
                try:
                    c=cPickle.loads(str(c))
                    if global_log_enabled: print(str(h),str(c))
                except Exception as e:
                    if global_log_enabled: print("exception while loading:",e)
                    raise

        return len(rows)

    def retry_execute(self,cur,*a,**aa):
        timeout=a['timeout'] if 'timeout' in aa else 10
        for x in range(timeout):
            try:
                return cur.execute(*a)
            except Exception as e:
                if global_log_enabled: print(render("{RED}sqlite execute failed, try again{/}: "+repr(e)),x)
                time.sleep(1)
        raise e

    def find(self,hashe):
        import sqlite3 as lite
        import sys

        con=self.connect()

        if global_log_enabled: print("requested to find",hashe)

        with con:    
            cur = con.cursor()    
            if global_log_enabled: print("now rows",cur.rowcount)

            try:
                self.retry_execute(cur,"SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            except Exception as e:
                if global_log_enabled: print("failed:",e)
                return None
            #cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            try:
                rows = cur.fetchall()
            except Exception as e:
                if global_log_enabled: print("exception while fetching",e)
                return None

        if len(rows)==0:
            if global_log_enabled: print("found no cache")
            return None
        
        if len(rows)>1:
            if global_log_enabled: print("multiple entries for same cache!")
            #raise Exception("confused cache! mupltile entries! : "+str(rows))
            print ("confused cache! mupltile entries! : "+str(rows),"{log:reflections}")
            print ("confused cache will run it again","{log:reflections}")
            return None

        return cPickle.loads(str(rows[0][0]))


    def make_record(self,hashe,content):
        import sqlite3 as lite
        import sys

        if global_log_enabled: print("will store",hashe,content)

        #con = lite.connect(self.filecacheroot+'/index.db')
        con=self.connect()

        c=cPickle.dumps(content)
        if global_log_enabled: print("content as",c)

        with con:
            cur = con.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, content TEXT)")
            self.retry_execute(cur,"INSERT INTO cacheindex VALUES(?,?)",(self.hashe2signature(hashe),c))

            if global_log_enabled: print("now rows",cur.rowcount)

    def load_content(self,hashe,c):
        if global_log_enabled: print("restoring from sqlite")
        if global_log_enabled: print("content",c['content'])
        return c['content']

#import MySQLdb

class MemCacheMySQL(MemCacheSqlite):
    cache={}
                
# also to object
    total_attempts=0
    failed_attempts=0

    def statistics(self):
        if self.con is None:
            if global_log_enabled: print("NOT connected")
        else:
            if global_log_enabled: print("connected to",self.con)
        if global_log_enabled: print("operations total/failed",self.total_attempts,self.failed_attempts)

    def connect(self):
        raise Exception("mysql disabled")
        if self.db is None:
            if global_log_enabled: print("connecting to mysql")
            self.db = MySQLdb.connect(host="apcclwn12", # your host, usually localhost
                      user="root", # your username
                      port=42512,
                      #unix_socket="/workdir/savchenk/mysql/var/mysql.socket",
                      passwd=open(os.environ['HOME']+"/.secret_mysql_password").read().strip(), # your password
                      db="ddacache") # name of the data base

        return self.db

    def __init__(self,*a,**aa):
        if global_log_enabled: print(a,aa)
        super(MemCacheMySQL,self).__init__(*a,**aa)
        self.db=None
        #self.connect()

    def list(self,select=None,nlast=None):

        con=self.connect()
        if global_log_enabled: print("listing cache")

        selection_string=""
        if select is not None:
            selection_string=" WHERE "+select # must be string

        nlast_string=""
        if nlast is not None:
            nlast_string=" ORDER BY rowid DESC LIMIT %i"%nlast # must be int

        with con:    
            cur = db.cursor()    

            if global_log_enabled: print("SELECT * FROM cacheindex"+selection_string+nlast_string)

            self.retry_execute(cur,"SELECT * FROM cacheindex"+selection_string+nlast_string)
            rows = cur.fetchall()

            if global_log_enabled: print("found rows",len(rows))
            for h,fh,c in rows:
                try:
                    c=cPickle.loads(str(c))
                    if global_log_enabled: print(str(h),str(c))
                except Exception as e:
                    if global_log_enabled: print("exception while loading:",e)
                    raise

        return len(rows)

    def retry_execute(self,cur,*a,**aa):
        timeout=a['timeout'] if 'timeout' in aa else 10
        for x in range(timeout):
            try:
                if global_log_enabled: print(a)
                self.total_attempts+=1
                return cur.execute(*a)
            except Exception as e:
                self.failed_attempts+=1
                if global_log_enabled: print(render("{RED}mysql execute failed, try again{/}: "+repr(e)),x)
                time.sleep(1)
        raise e

    def find(self,hashe):
        import sys
        
        if global_log_enabled: print("requested to find",hashe)
        if global_log_enabled: print("hashed",hashe,"as",self.hashe2signature(hashe))

        db=self.connect()

        if True:    
            cur = db.cursor()    
            if global_log_enabled: print("now rows",cur.rowcount)

            try:
                t0=time.time()
                self.retry_execute(cur,"SELECT content FROM cacheindex WHERE hashe=%s",(self.hashe2signature(hashe),))
                if global_log_enabled: print("mysql request took",time.time()-t0,"{log:top}")
            except Exception as e:
                if global_log_enabled: print("failed:",e)
                return None
            #cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            rows = cur.fetchall()

        if len(rows)==0:
            if global_log_enabled: print("found no cache")
            return None
        
        if len(rows)>1:
            if global_log_enabled: print("multiple entries for same cache!")
            if global_log_enabled: print(rows)
            return None
            #raise Exception("confused cache! mupltile entries!")

        return cPickle.loads(str(rows[0][0]))


    def make_record(self,hashe,content):
        import sys,json

        if global_log_enabled: print("will store",hashe,content)

        #con = lite.connect(self.filecacheroot+'/index.db')
        db=self.connect()

        c=cPickle.dumps(content)
        if global_log_enabled: print("content as",c)

        if "_da_cached_path" in content:
            aux1=content['_da_cached_path']
        else:
            aux1=""

        with db:
            cur = db.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, fullhashe TEXT, content TEXT)")
            self.retry_execute(cur,"INSERT INTO cacheindex (hashe,fullhashe,content,timestamp,refdir) VALUES(%s,%s,%s,%s,%s)",(self.hashe2signature(hashe),json.dumps(hashe),c,time.time(),aux1))

            if global_log_enabled: print("now rows",cur.rowcount)

    def load_content(self,hashe,c):
        if global_log_enabled: print("restoring from sqlite")
        if global_log_enabled: print("content",c['content'])
        return c['content']
    
    def make_delegation_record(self,hashe,module_description,dependencies):
        import sys,json

        if global_log_enabled: print("will store",hashe,module_description)

        #con = lite.connect(self.filecacheroot+'/index.db')
        db=self.connect()

        shorthashe=self.hashe2signature(hashe)

        if dependencies is not None and dependencies!=[]: # two??..
            status="waiting for:"+",".join(dependencies) # comas?
        else: 
            status="ready to run"

        with db:
            cur = db.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS delegationindex(id MEDIUMINT NOT NULL AUTO_INCREMENT, timestamp DOUBLE, hashe TEXT, fullhashe TEXT, modules TEXT, status TEXT, PRIMARY KEY (id))")
            self.retry_execute(cur,"INSERT INTO delegationindex (timestamp,hashe,fullhashe,modules,status) VALUES(%s,%s,%s,%s,%s)",(time.time(),shorthashe,json.dumps(hashe),json.dumps(module_description),status))

            if global_log_enabled: print("now rows",cur.rowcount)

        return shorthashe

Cache=MemCache


class TransientCache(MemCache): #d
    # currently store each cache in a file; this is not neccesary 
    __metaclass__ = decorate_all_methods
    cache={}

    parent=None

    def load(self):
        pass
    
    def save(self):
        pass

    def __repr__(self):
        return "[TransientCache of size %i at %s]"%(len(self.cache.keys()),str(id(self)))

    def list(self):
        for a,b in self.cache.items():
            if global_log_enabled: print(a,":",b)

    def restore(self,hashe,obj,rc=None):
        if obj.run_for_hashe or obj.mutating:
            return 
        # check if updated

        self.list()

        c=self.find(hashe)
        if c is None:
            if not obj.cached:
                if global_log_enabled: print("object is not cached, i.e. only transient level cache; not leading to parent")
                return
            return self.restore_from_parent(hashe,obj,rc)

        if global_log_enabled: print("transient cache stores results in the memory, found:",c)

        for k,i in c.items():
            if global_log_enabled: print("restoring",k,i)
            setattr(obj,k,i)
        
        if global_log_enabled: print("also files restores are ignored")

        if global_log_enabled: print("restored")
        return True

    def store_to_parent(self,hashe,obj):
        return
        if not obj.cached:
            if global_log_enabled: print("object is not cached, i.e. only transient level cache; not leading to parent")
            return

        #if self.parent is None:
            if global_log_enabled: print("no parent to push up to")
         #   return

        #if global_log_enabled: print("parent to push up to:",self.parent)
        #self.parent.store(hashe,obj)

    def store(self,hashe,obj):
        if global_log_enabled: print("storing in memory cache:",hashe)
        if obj.run_for_hashe or obj.mutating:
            return 
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        obj._da_stamp=obj.get_stamp() # or in the object?
        
        if not hasattr(self,'cache'):
            self.cache={}

        content=obj.export_data()

        self.cache[hashe]=content

        if global_log_enabled: print("stored")
        self.list()

        #self.store_to_parent(hashe,obj)

class MemCacheIndex(MemCache):
    def __init__(self,*a,**aa):
        if global_log_enabled: print(a,aa)
        super(MemCacheNoIndex,self).__init__(*a,**aa)

    def find(self,hashe):
        import sqlite3 as lite
        import sys

        if global_log_enabled: print("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)

        if os.path.exists(cached_path+"/cache.pickle.gz"):
            if global_log_enabled: print("found cache file:",cached_path+"/cache.pickle.gz")
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                if global_log_enabled: print("failed to load content! :"+repr(e))
                return None

        if global_log_enabled: print("no file found in",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return


class IRODSFileBackend:
    def exists(self,fn):
        try:
            subprocess.check_call(["ils",fn])
            return True
        except subprocess.CalledProcessError:
            return False

    def getsize(self,origin):
        return 0 
        #return os.path.getsize(origin)/1024./1024.
    
    def get(self,orig,dest,gz=False):
           # shutil.copy(orig,dest)
        subprocess.check_call(["iget","-f",orig])
        if gz:
            open(dest,"w").write(gzip.open(os.path.basename(orig)).read())
    
    def symlink(self,orig,dest):
        os.symlink(orig,dest)
    
    def put(self,orig,dest):
        subprocess.check_call(["iput","-f",orig,dest])
    
    def makedirs(self,dirs):
        check_call(["imkdir","-p",dirs])

    def open(self,fn,mode="r",gz=False):
        local_fn=os.path.basename(fn) # !!

        if "w"==mode:
            if global_log_enabled: print("will later put file to irods")
            self.register_pending_put(local_fn,fn)
        elif "r"==mode:
            if global_log_enabled: print("will get file from irods:",fn,local_fn)
            self.get(fn,local_fn)
        else:
            raise Exception("do not understand this mode: "+mode)

        if gz:
            return gzip.open(local_fn,mode)
        return open(local_fn,mode)

    def register_pending_put(self,local_fn,remote_fn):
        if self.pending_put is None:
            self.pending_put=[]
        self.pending_put.append([local_fn,remote_fn])

    pending_put=None

    def flush(self):
        while self.pending_put is not None and len(self.pending_put)>0:
            local,remote=self.pending_put.pop()
            self.put(local,remote)



class MemCacheNoIndex(MemCache):
    def __init__(self,*a,**aa):
        if global_log_enabled: print(a,aa)
        super(MemCacheNoIndex,self).__init__(*a,**aa)

    def find(self,hashe):
        import sqlite3 as lite
        import sys

        if global_log_enabled: print("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)
        if self.filebackend.exists(cached_path+"/cache.pickle.gz"):
            if global_log_enabled: print("found cache file:",cached_path+"/cache.pickle.gz")
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                if global_log_enabled: print("faild to load content! :"+repr(e))
                return None

        if global_log_enabled: print("no file found",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return

class MemCacheIRODS(MemCacheNoIndex):
    filebackend=IRODSFileBackend()

def update_dict(a,b):
    return dict(a.items()+b.items())

TransientCacheInstance=TransientCache()

#@for_all_methods(decorate_method)
class DataAnalysis:
    __metaclass__ = decorate_all_methods

    infactory=True

    cache=Cache()

    cached=False

    abstract=False

    run_for_hashe=False
    mutating=False
    copy_cached_input=True
    datafile_restore_mode=None

    schema_hidden=False

    assumptions=[]

    essential_deep_inputs=()

    default_log_level=""

    explicit_output=None

    min_timespent_tocache=5.
    min_timespent_tocache_hard=0.5
    max_timespent_tocache=5.
    allow_timespent_adjustment=False
    hard_timespent_checks=False

    allow_alias=False

    virtual=True

    def is_virtual(self):
        #if self.run_for_hashe: return False
        return self.virtual

    _da_restored=None
    _da_locally_complete=None
    _da_main_delegated=None
    _da_delegated_input=None

    write_caches=[MemCache]
    read_caches=[MemCache]

    #def get_dynamic_input(self):
     #   if hasattr(self,'input_dynamic'):
      #      return self.input_dynamic
      #  return []

    def __new__(self,*a,**args): # no need to split this in new and factory, all togather
        self=object.__new__(self)

        # otherwise construct object, test if already there

        self._da_attributes=dict([(a,b) for a,b in args.items() if a!="assume" and not a.startswith("input") and a!="update" and a!="dynamic" and not a.startswith("use_")]) # exclude registered
        
        
        
        update=False
        #update=True
        if 'update' in args:
            if global_log_enabled: print("update in the args:",update)
            update=args['update']
        
        for a,b in args.items():
            if a.startswith("input"):
                if global_log_enabled: print("input in the constructor:",a,b)
                setattr(self,a,b)
                if global_log_enabled: print("explicite input require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit input:'+repr(a)+" = "+repr(b)
            
            if a.startswith("use"):
                if global_log_enabled: print("use in the constructor:",a,b)
                setattr(self,a.replace("use_",""),b)
                if global_log_enabled: print("explicite use require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit use:'+repr(a)+" = "+repr(b)

        name=self.get_signature()
        if global_log_enabled: print("requested object",name,"attributes",self._da_attributes)

        if 'dynamic' in args and not args['dynamic']:
            print("not dynamic object:",self,level='dynamic')
            r=self
        else:
            print("dynamic object, from",self)
            r=AnalysisFactory.get(self,update=update)
            print("dynamic object, to",r)
        
        if 'assume' in args and args['assume']!=[]:
            print("replacing with a copy: < ",r)
            r=r.__class__(dynamic=False) # replace with a copy
            print("replacing with a copy: > ",r)
            r.assumptions=args['assume']
            if not isinstance(r.assumptions,list): # iteratable?
                r.assumptions=[r.assumptions]
            print("assuming with a copy: > ",r)
            if global_log_enabled: print("requested assumptions:",self.assumptions)
            if global_log_enabled: print("explicite assumptions require non-virtual") # differently!
            #self.virtual=False
            r._da_virtual_reason='assumptions:'+repr(self.assumptions)

        return r

    def promote(self):
        if global_log_enabled: print("promoting to the factory",self)
        return AnalysisFactory.put(self)


#   implements:
#       caching
#       
#       dependencies
#   
#   input hashes + version => output hash

    #input= no inpuit!

    version="v0"

    def main(self):
        pass

    def get_stamp(self):
        return {'host':socket.gethostname(),
                  'time':time.time(),
                  'htime':time.ctime(),
                  'proc':sys.argv,
                  'pid':os.getpid()}


    def interpret_item(self,item):
        return AnalysisFactory.get(item,update=False)
    
    def import_data(self,data):
        if global_log_enabled: print("updating analysis with data")
        for a,b in data.items():
            setattr(self,a,b)


    def export_data(self):
        empty_copy=self.__class__
        if global_log_enabled: print("my class is",self.__class__)
        updates=set(self.__dict__.keys())-set(empty_copy.__dict__.keys())
        if global_log_enabled: print("new keys:",updates)

        if self.explicit_output is not None:
            if global_log_enabled: print("explicit output requested",self.explicit_output)
            r=dict([[a,getattr(self,a)] for a in self.explicit_output if hasattr(self,a)])
        else:
            r=dict([[a,getattr(self,a)] for a in updates if not a.startswith("_da_") and not a.startswith("use_") and not a.startswith("input") and not a.startswith('assumptions')])

        if global_log_enabled: print("resulting output:",r)
        return r

    # the difference between signature and version is that version can be avoided in definition and substituted later
    # it can be done differently
    # e.g. versions can be completely avoided


    def get_formatted_attributes(self):
        if hasattr(self,'_da_attributes'): #not like that
            return "_".join(["%s.%s"%(str(a),repr(b).replace("'","")) for a,b in self._da_attributes.items()])
        else:
            return ""

    def get_version(self):
        return self.get_signature()+"."+self.version

    def get_signature(self):
        a=self.get_formatted_attributes()
        if a!="": a="."+a
    
        state=self.get_state()
        if state is not None:
            a=a+"."+state

        return self.__class__.__name__+a

    def get_state(self):
        if not hasattr(self,'_da_state'):
            self._da_state=self.compute_state()
        return self._da_state

    def compute_state(self):
        return None

    def store_cache(self,fih):
        """
    store output with
        """

        if global_log_enabled: print(render("{MAGENTA}storing in cache{/}"))
        if global_log_enabled: print("hashe:",fih)

     #   c=MemCacheLocal.store(fih,self.export_data())
        #if global_log_enabled: print(render("{MAGENTA}this is non-cached analysis, reduced caching: only transient{/}"))
        TransientCacheInstance.store(fih,self)
        self.cache.store(fih,self)
        #c=MemCacheLocal.store(oh,self.export_data())
    
    def retrieve_cache(self,fih,rc=None):
        if global_log_enabled: print("requested cache for",fih)

        if self._da_locally_complete is not None:
            if global_log_enabled: print("this object has been already restored and complete",self)
            if self._da_locally_complete == fih:
                if global_log_enabled: print("this object has been completed with the neccessary hash: no need to recover state",self)
                return True
            else:
                if global_log_enabled: print("state of this object isincompatible with the requested!")
                if global_log_enabled: print(" was: ",self._da_locally_complete)
                if global_log_enabled: print(" now: ",fih)
                #raise Exception("hm, changing analysis dictionary?")
                if global_log_enabled: print("hm, changing analysis dictionary?","{log:thoughts}")
    
                if self.run_for_hashe:
                    if global_log_enabled: print("object is run_for_hashe, this is probably the reason")
        
                return None

        if rc is None:
            rc={}
            
        r=TransientCacheInstance.restore(fih,self,rc)
        
        if r and r is not None:
            if global_log_enabled: print("restored from transient: this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=True # info save
            return r

        if not self.cached:
            if global_log_enabled: print(render("{MAGENTA}not cached restore only from transient{/}"))
            return None # only transient! 
        # discover through different caches
        #c=MemCacheLocal.find(fih)

        r=self.cache.restore(fih,self,rc)

        if r and r is not None:
            if global_log_enabled: print("this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=fih # info save
            if global_log_enabled: print("locally complete:",fih,'{log:top}')
            return r
        return r # twice

       # if global_log_enabled: print(c)

       # if c is not None:
        #    self.import_data(c)
        #    return True

    def plot_schema(self,fn="schema.png"):
        self.get_schema().write_png(fn)

    def get_schema(self,graph=None):
        import pydot

        graph = pydot.Dot(graph_type='digraph')

        def make_schema(i1,i2):
            
            if not isinstance(i2,DataAnalysis):
                return

            if i2.schema_hidden:
                return
            
            node = pydot.Node(repr(i2), style="filled", fillcolor="green")
            node = pydot.Node(repr(i1), style="filled", fillcolor="green")

            edge = pydot.Edge(repr(i2), repr(i1))
            graph.add_edge(edge)

        self.process_input(obj=None,process_function=make_schema,explicit_input_required=False)

        # do it all from hash, no need to construct again
        return graph

    def get(self):
        return self.process(output_required=True)[1]
        

    def process_checkin_assumptions(self):
        if self.assumptions!=[]:
            if global_log_enabled: print("cache assumptions:",AnalysisFactory.cache_assumptions)
            if global_log_enabled: print("assumptions:",self.assumptions)
            if global_log_enabled: print("non-trivial assumptions require copy of the analysis tree")
            AnalysisFactory.WhatIfCopy("requested by "+repr(self),self.assumptions)
            for a in self.assumptions:
                a.promote()
        else:
            if global_log_enabled: print("no special assumptions") 

    def process_checkout_assumptions(self):
        if global_log_enabled: print("assumptions checkout")
        if self.assumptions!=[]:
            AnalysisFactory.WhatIfNot()

    def process_restore_rules(self,restore_rules,extra):
        if global_log_enabled: print("suggested restore rules:",restore_rules)
        restore_rules_default=dict(
                    output_required=False,
                    substitute_output_required=False,
                    explicit_input_required=False,
                    restore_complete=False,
                    restore_noncached=False,
                    run_if_haveto=True,
                    input_runs_if_haveto=False,
                    can_delegate=False,
                    can_delegate_input=False,
                    run_if_can_not_delegate=True)
        restore_rules=dict(restore_rules_default.items()+(restore_rules.items() if restore_rules is not None else []))
        # to simplify input
        for k in extra.keys():
            if k in restore_rules:
                restore_rules[k]=extra[k]
        
        # always run to process
        restore_rules['substitute_output_required']=restore_rules['output_required']
        if self.run_for_hashe:
            if global_log_enabled: print(render("{BLUE}this analysis has to run for hashe! this will be treated later, requiring output{/}"))
            if global_log_enabled: print(render("{BLUE}original object output required?{/}"+str(restore_rules['output_required'])))
            restore_rules['output_required']=True
            restore_rules['explicit_input_required']=True
            restore_rules['input_runs_if_haveto']=True
            restore_rules['run_if_haveto']=True

        if global_log_enabled: print("will use restore_rules:",restore_rules)
        return restore_rules

    def process_restore_config(self,restore_config):
        rc=restore_config # this is for data restore modes, passed to cache
        if restore_config is None:
            rc={'datafile_restore_mode':'copy'}
        
        if global_log_enabled: print('restore_config:',rc)
        return restore_config


    def process_timespent_interpret(self):
        tspent=self.time_spent_in_main
        if tspent<self.min_timespent_tocache and self.cached:
            if global_log_enabled: print(render("{RED}requested to cache fast analysis!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache)))
            if self.allow_timespent_adjustment:
                if global_log_enabled: print(render("{MAGENTA}temporarily disabling caching for this analysis{/}"))
                self.cached=False
            else:
                if global_log_enabled: print("being ignorant about it")
        
            if tspent<self.min_timespent_tocache_hard and self.cached:
                if self.hard_timespent_checks:
                    estr=render("{RED}requested to cache fast analysis, hard limit reached!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache_hard))
                    raise Exception(estr)
                else:
                    if global_log_enabled: print("ignoring hard limit on request")

        if tspent>self.max_timespent_tocache and not self.cached:
            if global_log_enabled: print(render("{BLUE}analysis takes a lot of time but not cached, recommendation is to cache!{/}"),"{log:advice}")

    def process_run_main(self):
        #self.runtime_update('running')
        if self.abstract:
            raise Exception("attempting run abstract!")

        dll=self.default_log_level
        self.default_log_level="main"

        if global_log_enabled: print(render("{RED}running main{/}"),'{log:top}')
        t0=time.time()
        main_log=StringIO.StringIO()
        main_logstream=LogStream(main_log,lambda x:True)
        if global_log_enabled: print("starting main log stream",main_log,main_logstream)

        mr=self.main() # main!

        main_logstream.forget()
        self._da_main_log_content=main_log.getvalue()
        main_log.close()
        if global_log_enabled: print("closing main log stream",main_log,main_logstream)

        tspent=time.time()-t0
        self.time_spent_in_main=tspent
        if global_log_enabled: print(render("{RED}finished main{/} in {MAGENTA}%.5lg seconds{/}"%tspent),'{log:resources}')
        self.note_resource_stats({'resource_type':'runtime','seconds':tspent})

        self.default_log_level=dll
        #self.runtime_update("storing")

        if mr is not None:
            if global_log_enabled: print("main returns",mr,"attaching to the object as list")

            if isinstance(mr, collections.Iterable):
                mr=list(mr)
            else:
                mr=[mr]
            for r in mr:
                if isinstance(r,DataAnalysis):
                    if global_log_enabled: print("returned dataanalysis:",r,"assumptions:",r.assumptions)
            setattr(self,'output',mr)

    def process_find_output_objects(self):
        da=list(objwalk(self.export_data(),sel=lambda y:isdataanalysis(y)))
        if da!=[]:
            if global_log_enabled: print(render("{BLUE}resulting object exports dataanalysis, should be considered:{/}"),da)
            if global_log_enabled: print(render("{RED}carefull!{/} it will substitute the object!"),da)
    
            if len(da)==1:
                da=da[0]
        return da

    def get_delegation(self):
    #def list_delegated(self):
        #if self._da_delegated_input is not None:
        #   if self._da_main_delegated is not None:
        #       raise Exception("core design error! main is delegated as well as input! ")
        #   return self._da_delagated_input
        if self._da_main_delegated is not None:
            return self._da_main_delegated
    
    def process_list_delegated_inputs(self,input):
        return [] # disabled

        # walk input recursively
        if isinstance(input,list) or isinstance(input,tuple):
            delegated_inputs=[]
            for input_item in input:
                delegated_inputs+=self.process_list_delegated_inputs(input_item)
            return delegated_inputs
            
        if isinstance(input,DataAnalysis):
            d=input.get_delegation()
            if d is not None:
                if global_log_enabled: print("input delegated:",input,d)
                return [d]
            return []

        raise Exception("can not understand input: "+repr(input))

    def process_verify_inputs(self,input):
        # walk input recursively
        if isinstance(input,list) or isinstance(input,tuple):
            for input_item in input:
                self.process_verify_inputs(input_item)
            return

        if isinstance(input,DataAnalysis):
            if global_log_enabled: print("will verify:",input)
            if not input._da_locally_complete:
                if global_log_enabled: print("input is not completed! this should not happen!",input)
                raise("input is not completed! this should not happen!")
            return

        if input is None:
            if global_log_enabled: print("input is None, it is fine")
            return

        raise Exception("can not understand input: "+repr(input))

    def process(self,process_function=None,restore_rules=None,restore_config=None,requested_by=None,**extra):
        if global_log_enabled: print(render("{BLUE}PROCESS{/}"))

        if requested_by is None:
            requested_by=['direct']

        if global_log_enabled: print('cache assumptions:',AnalysisFactory.cache_assumptions,'{log:top}')
        if global_log_enabled: print('object assumptions:',self.assumptions,'{log:top}')



        restore_config=self.process_restore_config(restore_config)
        restore_rules=self.process_restore_rules(restore_rules,extra)
        
        if global_log_enabled: print(render("{BLUE}requested "+("OUTPUT" if restore_rules['output_required'] else "")+" by{/} "+" ".join(requested_by)),'{log:top}')
        requested_by=[("+" if restore_rules['output_required'] else "-")+self.get_version()]+requested_by

        #self.cache.statistics()
        self.process_checkin_assumptions()

        rr= dict(restore_rules.items() + dict(output_required=False).items()) # no need to request input results unless see below

        #### process input
        input_hash,input=self.process_input(obj=None,
                                            process_function=process_function,
                                            restore_rules=update_dict(restore_rules,dict(
                                                                                            output_required=False,
                                                                                            can_delegate=restore_rules['can_delegate_input'])), 
                                            requested_by=["input_of"]+requested_by) 
        #### /process input
        
        process_t0=time.time()

        if global_log_enabled: print("input hash:",input_hash)
        if global_log_enabled: print("input objects:",input)

                
        
        fih=('analysis',input_hash,self.get_version()) # construct hash
        if global_log_enabled: print("full hash:",fih)

        substitute_object=None

        if restore_rules['output_required']: # 
            if global_log_enabled: print("output required, try to GET from cache")
            if self.retrieve_cache(fih,restore_config): # will not happen with self.run_for_hashe
                if global_log_enabled: print("cache found and retrieved",'{log:top}')
                if global_log_enabled: print(fih,'{log:top}')
            else:
                if global_log_enabled: print("no cache",'{log:top}')
                if global_log_enabled: print(fih,'{log:top}')

                if restore_rules['explicit_input_required']:
                    if global_log_enabled: print("exclicite input is available")
                else:
                    if global_log_enabled: print("need to guarantee that explicit input is available")

                    ## if output has to be generated, but explicite input was not prepared, do it
                    ## process
                    return self.process(process_function=process_function,
                                        restore_rules=update_dict(restore_rules,dict(explicit_input_required=True)),requested_by=['output_required_by_parent']+requested_by )
                                        #restore_rules=update_dict(restore_rules,dict(output_required=True,explicit_input_required=True)) )
                    ##  /process

                delegated_inputs=self.process_list_delegated_inputs(input)
                if delegated_inputs!=[]:
                    if global_log_enabled: print("some input was delegated:",delegated_inputs)
                    if global_log_enabled: print(render("{RED}waiting for delegated input!{/}"))
                    self._da_delegated_input=delegated_inputs

                if restore_rules['can_delegate'] and self.cached:
                    if global_log_enabled: print("will delegate this analysis") 
                    hashekey=self.cache.register_delegation(self,fih)
                    self._da_main_delegated=hashekey
                    return fih,self # RETURN!

             # check if can and inpust  relaxe

                if delegated_inputs!=[]:
                    if global_log_enabled: print("analysis design problem! input was delegated but the analysis can not be. wait until the input is done!")
                    raise

                self.process_verify_inputs(input)
                
                if restore_rules['run_if_can_not_delegate']:
                    if global_log_enabled: print("no way was able to delegate, but all ready to run and allowed. will run")
                else:
                    if global_log_enabled: print("not allowed to run here. hopefully will run as part of higher-level delegation") 
                    raise Exception("not allowed to run but has to (delegation)!")
                    return fih,self # RETURN!

                if restore_rules['run_if_haveto'] or self.run_for_hashe:
                    mr=self.process_run_main() # MAIN!
                    self.process_timespent_interpret()
                else:
                    raise Exception("not allowed to run but has to! at "+repr(self))

                if global_log_enabled: print("new output:",self.export_data())

                self.store_cache(fih)
                #self.runtime_update("done")

            da=self.process_find_output_objects()
            if da!=[]:
                if self.cached:
                    if global_log_enabled: print(render("{RED}can not be cached - can not save non-virtual objects! (at the moment){/}"),da)
                    self.cached=False
                    
                #restore_rules_for_substitute=update_dict(restore_rules,dict(explicit_input_required=False))
                restore_rules_for_substitute=update_dict(restore_rules,dict(explicit_input_required=restore_rules['substitute_output_required']))
                if global_log_enabled: print(render("{RED}will process substitute object as input with the following rules:{/}"),restore_rules_for_substitute)

                rh,ro=self.process_input(da,restore_rules=restore_rules_for_substitute,requested_by=['output_of']+requested_by)
                if global_log_enabled: print(render("substitute the object with dynamic input:"),rh,ro)

                if global_log_enabled: print("--- old input hash:",fih)
                if self.allow_alias:
                    self.register_alias(fih,rh)
                else:
                    if global_log_enabled: print("alias is not allowed: using full input hash!")
                    fih=rh
                    substitute_object=ro
                    if global_log_enabled: print("+++ new input hash:",fih)
            
            if global_log_enabled: print("processing finished, main, object is locally complete")
            if global_log_enabled: print("locally complete:",id(self))
            if global_log_enabled: print("locally complete:",fih,'{log:top}')
            self._da_locally_complete=fih
        else:
            if global_log_enabled: print("NO output is strictly required, will not attempt to get")
            if restore_rules['restore_complete']: 
                if global_log_enabled: print("however, diagnostic complete restore is requested, trying to restore")
                if self.retrieve_cache(fih,rc):
                    if global_log_enabled: print("cache found and retrieved",'{log:top}')
                    if global_log_enabled: print("processing finished, object is locally complete")
                    self._da_locally_complete=fih
                    if global_log_enabled: print("locally complete:",fih,'{log:top}')
                else:
                    if global_log_enabled: print("NO cache found",'{log:top}')
        
        self.process_checkout_assumptions()

        process_tspent=time.time()-process_t0
        if global_log_enabled: print(render("{MAGENTA}process took in total{/}"),process_tspent)
        self.note_resource_stats({'resource_type':'usertime','seconds':process_tspent})
        self.summarize_resource_stats()

        return_object=self
        if substitute_object is not None:
            if global_log_enabled: print("returning substituted object")
            return_object=substitute_object

        return fih,return_object

    def register_alias(self,hash1,hash2):
        if global_log_enabled: print("alias:",hash1)
        if global_log_enabled: print("stands for",hash2)
        self.alias=hash2
        AnalysisFactory.register_alias(hash1,hash2)

    def geta(self,fih): # run or recover
        raise Exception("this is not used!")
                       # guarantee globally up-to-date state: either run or get from cache
        if global_log_enabled: print("GET")
        if self.retrieve_cache(fih):
            if global_log_enabled: print("cache found and retrieved",'{log:top}')
        else:
            if global_log_enabled: print("no cache")
            if global_log_enabled: print(render("{RED}running main{/}"),'{log:top}')
            self.main()
            if global_log_enabled: print(render("{RED}finished main{/}"))
            if global_log_enabled: print("new output:",self.export_data())

            self.store_cache(fih)


        
    def process_input(self,obj=None,process_function=None,restore_rules=None,restore_config=None,requested_by=None,**extra):
        """
        walk over all input; apply process_function and implement if neccessary
        """

        if global_log_enabled: print("PROCESS INPUT")

        restore_rules_default=dict(explicit_input_required=False,restore_complete=False)
        restore_rules=dict(restore_rules_default.items()+restore_rules.items() if restore_rules is not None else [])
        # to simplify input
        for k in extra.keys():
            if k in restore_rules:
                restore_rules[k]=extra[k]
        #/

        if restore_config is None:
            restore_config={}
        if self.copy_cached_input:
            restore_config['datafile_restore_mode']="copy"
        else:
            restore_config['datafile_restore_mode']="url_in_object"
        
        if global_log_enabled: print("input restore_rules:",restore_rules)
        if global_log_enabled: print("input restore_config:",restore_config)
        
        if obj is None:
            # start from the object dir, look for input
            inputhashes=[]
            inputs=[]
            for a in dir(self):
                if a.startswith("input"):
                    o=getattr(self,a)
                    if global_log_enabled: print("input item",a,o)
                    if o is None:
                        raise Exception("input is None: vortual class: "+repr(self)+" input "+a)
                    h,l=self.process_input(obj=o,process_function=process_function,restore_rules=restore_rules,restore_config=restore_config,requested_by=requested_by)

                    if l is not None: 
                        if global_log_enabled: print("implemented input item",a,l)
                        if global_log_enabled: print("implemented input item",h)
                        setattr(self,a,l)
                    else:
                        if global_log_enabled: print("input item None!",a,l)
                        raise Exception("?")

                    inputhashes.append(h)
                    inputs.append(l)

            if len(inputhashes)>1:
                return ('list',)+tuple(inputhashes),inputs
            if len(inputhashes)==1:
                return inputhashes[0],inputs[0]

            if global_log_enabled: print("this class has no input! origin class")
            # does this ever happen?
            return None,None
        else:
            # process given input structure
            if global_log_enabled: print("parse and get",obj,obj.__class__)

            # list or tuple
            if isinstance(obj,list) or isinstance(obj,tuple):
                if global_log_enabled: print("parse list")
                hashes=[]
                nitems=[]
                for i in obj:
                    if global_log_enabled: print("item:",i)
                    hi,ni=self.process_input(obj=i,process_function=process_function,restore_rules=restore_rules,restore_config=restore_config,requested_by=requested_by)
                    hashes.append(hi)
                    nitems.append(ni)
                if all([i is None for i in nitems]):
                    return tuple(['list']+hashes),None

                if any([i is None for i in nitems]):
                    raise Exception("process input returned None for a fraction of a structured input! this should not happen")
                    
                return tuple(['list']+hashes),nitems

            # we are down to the input item finally
        
        item=self.interpret_item(obj)  # this makes DataAnalysis object
        rr=dict(restore_rules.items()+dict(explicit_input_required=False,output_required=restore_rules['explicit_input_required']).items())
        if self.run_for_hashe:
            if global_log_enabled: print("for run_for_hashe, input need a right to run")
            restore_rules['run_if_haveto']=True

        
        if global_log_enabled: print("proceeding to run",item,"rules",restore_rules)
        if global_log_enabled: print("item:",item._da_locally_complete)
        input_hash,newitem=item.process(process_function=process_function,restore_rules=rr,restore_config=restore_config,requested_by=requested_by) # recursively for all inputs process input
        if global_log_enabled: print("process_input finishing at the end",input_hash,newitem)
        
        if process_function is not None:
            process_function(self,newitem) # then run something to each input item

        return input_hash,newitem # return path to the item (hash) and the item

    #def get_output_hash(self):
    #    return shhash(tuple(sorted(self.export_data().items())))

    _da_resource_stats=None

    def note_resource_stats(self,info):
        if self._da_resource_stats is None:
            self._da_resource_stats=[]
        if global_log_enabled: print('note resource stats:',info['resource_type'],'{log:resources}')
        self._da_resource_stats.append(info)
    
    def summarize_resource_stats(self):
        total_usertime=sum([a['seconds'] for a in self._da_resource_stats if a['resource_type']=='usertime'])
        if global_log_enabled: print(render("collected resource stats, total {MAGENTA}usertime{/}"),total_usertime,'{log:resources}')
        
        total_runtime=sum([a['seconds'] for a in self._da_resource_stats if a['resource_type']=='runtime'])
        if global_log_enabled: print(render("collected resource stats, total {MAGENTA}run time{/}"),total_runtime,'{log:resources}')
        
        total_cachetime=sum([a['stats']['copytime'] for a in self._da_resource_stats if a['resource_type']=='cache'])
        if global_log_enabled: print(render("collected resource stats, total {MAGENTA}cache copy time{/}"),total_cachetime,'{log:resources}')


    def __call__(self):
        return self

    def __repr__(self):
        return "[%s%s]"%(self.get_version(),";Virtual" if self.virtual else "")

        #if hasattr(self,'_da_attributes'):
        #    return "[%s: %s: %s]"%(self.__class__.__name__,repr(self._da_attributes),self.version)
        #return "[%s: %s]"%(self.__class__.__name__,self.version)
        #return "[%s: %s %s]"%(self.__class__.__name__,self.version,("%.12x"%id(self))[-6:]) # note that instances are different; take same instance by name from a dict

class Data(DataAnalysis):
    """
    data runs trivilarily: only returns hash of its content
    """

    def get(self,path):
        pass

class DataFile(DataAnalysis):
    infactory=False

    def __init__(self,fn=None):
        self.path=fn

    def get_cached_path(self): # not work properly if many run!
        return self.cached_path if hasattr(self,'cached_path') else self.path

    def open(self):
        return gzip.open(self.cached_path) if hasattr(self,'cached_path') else open(self.path)

    def __repr__(self):
        return "[DataFile:%s]"%(self.path if hasattr(self,'path') else 'undefined')

def hash_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return shhash(md5.digest())

class FileHashed(DataAnalysis):
    input_filename=None

    cached=False # never
    infactory=False
    run_for_hashe=True

    def main(self): # pointless unless fine has known hashe!
        self.md5=hash_for_file(open(self.input_filename.handle))
        return DataHandle(self.input_filename.handle+":md5:"+self.md5[:8])

class DataHandle(DataAnalysis):
    infactory=False

    def __new__(self,*a,**args): # not only kw
        return object.__new__(self)

    def __init__(self,h=None):
        self.handle=h

    def process(self,**args):
        if global_log_enabled: print("datahandle is hash",self)
        self._da_locally_complete=True
        return self.handle,self

    def __repr__(self):
        return '[%s]'%self.handle

    
class DataAnalysisGroup(DataAnalysis): # make it 
    def process(self):
        pass

# imported

import imp

class CacheModule(MemCache):
    filecacheroot=os.environ['DDA_MODULE_CACHE']

    def construct_cached_file_path(self,hashe,obj):
        print("requested path for",hashe,obj)

        def hash_to_path(hashe):
            if isinstance(hashe,tuple):
                if hashe[0]=="analysis": # more universaly
                    return hash_to_path(hashe[2])+"/"+hash_to_path(hashe[1])
                if hashe[0]=="list": # more universaly
                    return "..".join(map(hash_to_path,hashe[1:]))
                raise Exception("unknown tuple in the hash:"+str(hashe))
            if isinstance(hashe,str):
                return hashe
            raise Exception("unknown class in the hash:"+str(hashe))

        def hash_to_path2(hashe):
            #by32=lambda x:x[:8]+"/"+by8(x[8:]) if x[8:]!="" else x
            return hashe[2]+"/"+shhash(repr(hashe[1]))

        return self.filecacheroot+"/"+hashe[1][1]+"/"+hashe[1][2]+"/" # choose to avoid overlapp
    
class find_module_standard(DataAnalysis):
    cached=False # never    
    input_module_name=None

    def main(self):
        if global_log_enabled: print("find module",self.input_module_name)

        module_name=self.input_module_name.handle
        self.found=imp.find_module(module_name,["."]+sys.path)
        fp, pathname, description=self.found
        
        if global_log_enabled: print("will search in",["."]+sys.path)

        if global_log_enabled: print("found as",pathname)

        self.module_path=pathname

cm=CacheModule()

class find_module_cached(DataAnalysis):
    cached=True
    cache=cm

    allow_timespent_adjustment=False
    hard_timespent_checks=False

    input_module_name=None
    input_module_version=None

    def main(self):
        if global_log_enabled: print("find module as ",self.input_module_name)

        pathname=self.input_module_name.handle+".py"
        hashedfn=self.input_module_name.handle+"."+self.get_version()+"."+hash_for_file(open(pathname))[:8]+".py"

        shutil.copyfile(pathname,hashedfn)

        if global_log_enabled: print("found as",pathname)
        if global_log_enabled: print("will store as",hashedfn)

        self.module_path=hashedfn
        self.module_file=DataFile(hashedfn)


class load_module(DataAnalysis):
    cached=False # never    
    input_module_path=None

    def main(self):
        if global_log_enabled: print("load module",self.input_module_path.input_module_name.handle)
        if global_log_enabled: print("load as",self.input_module_path.module_path)

#        self.module = __import__(self.input_module_path.module_path)
        if not os.path.exists(self.input_module_path.module_path):
            raise Exception("can not open: "+self.input_module_path.module_path)

        self.module=imp.load_source(self.input_module_path.input_module_name.handle,self.input_module_path.module_path)
        #self.module=imp.load_module(,*self.input_module_path.found)


def load_by_name(m):
    if m.startswith("/"):
        if global_log_enabled: print("will import modul from cache")
        ms=m[1:].split("/",1)
        if len(ms)==2:
            m0,m1=ms
        else:
            m0=ms[0]
            m1="master"

        if global_log_enabled: print("as",m0,m1)
        result=import_analysis_module(m0,m1),m0
        result[0].__dda_module_global_name__=(m0,m1)
        return result
    else:
        fp, pathname, description=imp.find_module(m,["."]+sys.path)
        if global_log_enabled: print("found as",  fp, pathname, description)
        return imp.load_module(m,fp,pathname,description), m

    return load_module(input_module_path=find_module_standard(input_module_name=name)).get().module

def import_analysis_module(name,version):
    return load_module(input_module_path=find_module_cached(input_module_name=name,input_module_version=version)).get().module

# abstract

class AnyAnalysis(DataAnalysis):
    def main(self):
        raise Exception("requested to run abstract any analysis!")

