from __future__ import print_function
from collections import Mapping, Set, Sequence 
from bcolors import render as render
import pprint
import time,gzip,cPickle,socket,sys,os,shutil,re,copy
from subprocess import check_call
import subprocess
import tempfile
import collections
import StringIO
import json
from datetime import datetime

from printhook import PrintHook,decorate_method_log,LogStream
import printhook

import caches

# compatibitlity with ddosa!

from caches import *
from hashtools import *

Cache=caches.MemCacheNoIndex()
TransientCacheInstance=caches.TransientCache()

import analysisfactory

# for name
class DataFile:
    pass

# TODO:


# reporting classes
# optional input
# cache migration
# track all exceptions!!!
# hashe to picture
# deleting files: cleanup after analysis
# cache module not irods but git

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



#global_log_enabled=True
#global_fancy_output=True
#global_suppress_output=False
#global_all_output=False
#global_readonly_caches=False
#global_output_levels=('top')

from  printhook import cprint

#

#AnalysisDict={}

from hashtools import shhash

# dual python 2/3 compatability, inspired by the "six" library
string_types = (str, unicode) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()

def get_cwd():
    tf=tempfile.NamedTemporaryFile()
    ppw=subprocess.Popen(["pwd"],stdout=tf)
    ppw.wait()

    try:
        ppw.terminate()
    except OSError:
        pass
    tf.seek(0)
    owd=tf.read()[:-1]
    cprint("old pwd gives me",owd)
    tf.close()
    del tf
    return owd


def athostdir(f):
    def wrapped(self,*a,**aa):
        owd=get_cwd()
        try:
            if not os.path.exists(self.hostdir):
                os.makedirs(self.hostdir)
            os.chdir(self.hostdir)
            r=f(self,*a,**aa)
        except Exception as e:
            cprint("exception in wrapped:",e)
            os.chdir(owd)
            raise
        os.chdir(owd)
        return r
    return wrapped

class DataHandle:
    pass

class DataFile:
    pass

class NoAnalysis:
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
    #cprint("walking in",obj,path)
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator:
        #cprint("walking interate")
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
         #       cprint("walking",path,path_component,value)
                for result in objwalk(value, path + (path_component,), memo, sel):
                    yield result
            memo.remove(id(obj))
    else:
        #cprint("walk end",path,obj)
        if sel(obj):
         #   cprint("walk selected",path,obj)
            yield obj

#/objwalk

def update_dict(a,b):
    return dict(a.items()+b.items())

AnalysisFactory=None

class AnalysisException(Exception):
    pass
    

class DataAnalysis:
    pass


class decorate_all_methods(type):
    def __new__(cls, name, bases, local):
        # also store in the dict

        # decorate
        if printhook.global_fancy_output:
            for attr in local:
                value = local[attr]
                if callable(value) and not isinstance(value,type):
                    local[attr] = decorate_method_log(value)

        c=type.__new__(cls, name, bases, local)

        # and we want to registed analysis
        if AnalysisFactory is not None and issubclass(c,DataAnalysis) and c.infactory:
            cprint("declaring analysis class",name,) #,"version",c.version,"as",c)
            cprint("   constructing object...")
            o=c(update=True) # test
            cprint("   registered",o)
            #AnalysisFactory.put(c,name)
        return c

class AnalysisFactoryClass: # how to unify this with caches?..
# handles all object equivalence
    __metaclass__ = decorate_all_methods
# dictionary of current object names, aliases?..
    cache={} 
    dda_modules_used=[]

    def __repr__(self):
        return "[AnalysisFactory: %i]"%len(self.cache)

    def reset(self):
        self.cache={}

    def put(self,obj,sig=None): 
        cprint("requested to put in factory:",obj,sig)  
        cprint("factory assumptions:",self.cache_assumptions)  


        if not obj.infactory:
            cprint("object is not in-factory, not putting")
            return obj

        if obj.assumptions!=[]:
            cprint("object has assumptions:",obj,obj.assumptions)
            raise Exception("can not store in cache object with assumptions")

        module_record=sys.modules[obj.__module__]
        if self.dda_modules_used==[] or self.dda_modules_used[-1]!=module_record:
            self.dda_modules_used.append(module_record)

        if isinstance(obj,type):
            cprint("requested to put class, it will be constructed")
            obj=obj()

        sig=obj.get_signature() if sig is None else sig # brutal
        cprint("put object:",obj,"signature",sig)
        saved=None
        if sig in self.cache:
            saved=self.cache[sig]
        self.cache[sig]=obj
        return saved

    def get(self,item,update=False): 
        """
        generates and instance of DataAnalysis from something
   
        """
        cprint("interpreting",item)
        cprint("factory knows",self.cache) #.keys())

        
        if item is None:
            cprint("item is None: is it a virtual class? should not be in the analysis!")
            raise Exception("virtual class, class with None inputs, is not allowed directly in the analysis")

        
        if isinstance(item,type) and issubclass(item,DataAnalysis):
            cprint("is subclass of DataAnalysis, probably need to construct it")
            name=item.__name__
            cprint("class name:",name)
            if name in self.cache:
                c=self.cache[name]
                cprint("have cache for this name:",c)
                if isinstance(item,type):
                    cprint("it is class, constructing")
                    c=c()
                    cprint("constructed",c)
                    self.put(c)
                    #cprint("will store",c)
                    return c
                if isinstance(item,DataAnalysis):
                    cprint("cache returns object, will use it:",c)
                    return c
            else:
                cprint("there is no such class registered!",name)
                raise Exception("there is no such class registered: "+name+" !")
        
        if isinstance(item,DataAnalysis): 
            cprint("is instance of DataAnalysis, signature",item.get_signature())
        
            if isinstance(item,DataHandle) or isinstance(item,DataFile):  # make as object call
                cprint("is datahandle or file, returning",item)
                return item

            if not item.infactory:  # make as object call
                cprint("is not in-factory, returning")
                return item
            
            s=item.get_signature()
            if s in self.cache:
                storeditem=self.cache[item.get_signature()]
    
                if isinstance(storeditem,type):
                    raise Exception("no classes can be stored!")

                #if item!=storeditem: # object was constructed during the declaration of the class or interpreted earlier, either way the ___new__ call had to guarantee it is the same. it has to be the same
                #    raise Exception("critical violation of sanity check! object was constructed twice "+str(item)+" vs "+str(storeditem))

                cprint("so, offered object:",item)
                cprint("     stored object:",storeditem)

                if not item.is_virtual(): # careful!
                    cprint("     offered object is non-virtual, simply returning") 
                    cprint("     offered object complettion:",item._da_locally_complete) 
                    cprint("     offered virtual reason:",item._da_virtual_reason if hasattr(item,'_da_virtual_reason') else "") 
                    #cprint("     offered object is non-virtual, forcing it") 
                    #update=True #!!!!
## think about this! update??
                    return item
                
                if len(item.assumptions)!=0:
                    cprint("     offered object has assumptions",item.assumptions) 
                    cprint("     offered object complettion:",item._da_locally_complete) 
                    cprint("     will copy and assign assumptions") 

                    copied_storeditem=storeditem.__class__(dynamic=False)
                    copied_storeditem.assumptions=item.assumptions

                    return copied_storeditem

                if update:
                    cprint("recommendation is to force update")
                    self.put(item)
                    return item
                else:
                    cprint("attention! offered object is discarded:",item) #,item.export_data())
                    cprint("stored in factory (this will be the valid instance):",storeditem) #,storeditem.export_data())
                    return storeditem

            cprint("no such object registered! registering") # attention!
            self.put(item)
            return item

        if isinstance(item,str):
            cprint("considering string data handle")
            return DataHandle(item)
         
        
        cprint("unable to interpret item: "+repr(item))
        raise Exception("unable to interpret item: "+repr(item))
        #return None

    cache_stack=[]
    cache_assumptions=[]
    comment=""

    def WhatIfCopy(self,description,new_assumptions):
        if isinstance(new_assumptions,tuple):
            new_assumptions=list(new_assumptions)
        if not isinstance(new_assumptions,list):
            new_assumptions=[new_assumptions]


        cprint(render("{RED}go deeper! what if?{/} stack of size %i"%len(self.cache_stack)))
        if self.comment==description:
            cprint("will not push copy, already in the assumption",description)
            return

        cprint("pushing into stack current version, using copy",description)
        self.comment=description
        self.cache_stack.append(self.cache)
        self.cache_assumptions.append(new_assumptions)
        self.cache={} # this makes assumptions reset
        cprint("cache stack of size",len(self.cache_stack))
        cprint("cache stack last entry:",self.cache_stack[-1])

        for i,o in self.cache_stack[-1].items():
            cprint("promoting",i,'assumptions',o.assumptions)
            #if o.is_virtual():
            #    cprint("virtual object, constructing empty copy")
            o.__class__(dynamic=False).promote() # assume??

        for assumptions in self.cache_assumptions:
            cprint("assumption group:",assumptions)
            for a in assumptions:
                cprint("assumption",a)
                a.promote()

         #   else:
         #       cprint("non-virtual object! promoting object itself")
         #       o.promote() # assume??
        #self.cache=copy.deepcopy(self.cache_stack[-1]) # dangerous copy: copying call __new__, takes them from current cache
        cprint("current cache copied:",self.cache)

    def WhatIfNot(self):
        cprint("factory knows",self.cache) #.keys())
        if self.cache_stack==[]:
            cprint("empty stack!")       
            return
            #raise Exception("empty stack!")       

        cprint("poping from stack last version")
        self.cache=self.cache_stack.pop()
        assumptions=self.cache_assumptions.pop()
        cprint("discarding last stacked",self.comment)
        cprint("discarding:",assumptions)
        cprint("current assumptions level %i:"%len(self.cache_assumptions),self.cache_assumptions[-1] if self.cache_assumptions!=[] else "none")
        self.comment=""
        cprint("factory knows",self.cache) #.keys())


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

    definitions=[]

    def register_definition(self,c,h):
        self.definitions.append([c,h])

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

        #cprint('aliases:',self.aliases)
        return self.aliases   

    def get_definitions(self):
        return self.definitions

     #   return [[a,b] for a,b in self.aliases if contains(h0,a)]

    def get_module_description(self):
        module_description=[]
        for m in AnalysisFactory.dda_modules_used:
            cprint("module",m)
            if hasattr(m,"__dda_module_global_name__"):
                cprint("dda module global name", m.__dda_module_global_name__)
                module_description.append(['cache',m.__name__,m.__dda_module_global_name__])
            else:
                module_description.append(['filesystem',m.__name__,m.__file__])

        return module_description

AnalysisFactory=AnalysisFactoryClass()
analysisfactory.AnalysisFactory=AnalysisFactory

byname=lambda x:AnalysisFactory.byname(x)

def get_object(a):
    return AnalysisFactory[a]




class DataAnalysis(object):
    __metaclass__ = decorate_all_methods

    infactory=True

    cache=Cache

    cached=False

    abstract=False

    run_for_hashe=False
    mutating=False
    copy_cached_input=True
    datafile_restore_mode=None

    incomplete=False

    only_verify_cache_record=False

    schema_hidden=False

    test_files=True

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

    noanalysis=False
    
    rename_output_unique=False

    force_complete_input=True

    def is_virtual(self):
        #if self.run_for_hashe: return False
        return self.virtual

    _da_restored=None
    _da_locally_complete=None
    _da_main_delegated=None
    _da_delegated_input=None

    _da_main_log_content=""

    write_caches=[caches.MemCache]
    read_caches=[caches.MemCache]

    #def get_dynamic_input(self):
     #   if hasattr(self,'input_dynamic'):
      #      return self.input_dynamic
      #  return []

    _da_settings=None

    def str(self):
        if hasattr(self,'handle'): return self.handle
        return repr(self)

    def __new__(self,*a,**args): # no need to split this in new and factory, all togather
        self=object.__new__(self)

        # otherwise construct object, test if already there

        self._da_attributes=dict([(a,b) for a,b in args.items() if a!="assume" and not a.startswith("input") and a!="update" and a!="dynamic" and not a.startswith("use_") and not a.startswith("set_")]) # exclude registered
        
        
        update=False
        #update=True
        if 'update' in args:
            cprint("update in the args:",update)
            update=args['update']
        
        for a,b in args.items():
            if a.startswith("input"):
                cprint("input in the constructor:",a,b)
                setattr(self,a,b)
                cprint("explicite input require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit input:'+repr(a)+" = "+repr(b)
            
            if a.startswith("use"):
                cprint("use in the constructor:",a,b)
                setattr(self,a.replace("use_",""),b)
                cprint("explicite use require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit use:'+repr(a)+" = "+repr(b)
            
            if a.startswith("set"):
                cprint("set in the constructor:",a,b)
                setting_name=a.replace("set_","")
                setattr(self,setting_name,b)
                if self._da_settings is None:
                    self._da_settings=[]
                self._da_settings.append(setting_name)
                cprint('settings:',self,self._da_settings,level='top')
                #cprint("explicite use require non-virtual") # differently!
                #self.virtual=False
                #self._da_virtual_reason='explicit use:'+repr(a)+" = "+repr(b)

        name=self.get_signature()
        cprint("requested object",name,"attributes",self._da_attributes)

        if 'dynamic' in args and not args['dynamic']:
            cprint("not dynamic object:",self,level='dynamic')
            r=self
        else:
            cprint("dynamic object, from",self,level='dynamic')
            r=AnalysisFactory.get(self,update=update)
            cprint("dynamic object, to",r,level='dynamic')
        
        if 'assume' in args and args['assume']!=[]:
            cprint("replacing with a copy: < ",r,level='dynamic')
            r=r.__class__(dynamic=False) # replace with a copy
            cprint("replacing with a copy: > ",r,level='dynamic')
            r.assumptions=args['assume']
            if not isinstance(r.assumptions,list): # iteratable?
                r.assumptions=[r.assumptions]
            cprint("assuming with a copy: > ",r,level='dynamic')
            cprint("requested assumptions:",self.assumptions)
            cprint("explicite assumptions require non-virtual") # differently!
            #self.virtual=False
            r._da_virtual_reason='assumptions:'+repr(self.assumptions)

        return r

    def promote(self):
        cprint("promoting to the factory",self)
        return AnalysisFactory.put(self)

    def verify_content(self):
        return True

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
        cprint("updating analysis with data")
        for a,b in data.items():
            setattr(self,a,b)

    def export_data(self,embed_datafiles=False):
        empty_copy=self.__class__
        cprint("my class is",self.__class__)
        updates=set(self.__dict__.keys())-set(empty_copy.__dict__.keys())
        cprint("new keys:",updates)

        if self.explicit_output is not None:
            cprint("explicit output requested",self.explicit_output)
            r=dict([[a,getattr(self,a)] for a in self.explicit_output if hasattr(self,a)])
        else:
            r=dict([[a,getattr(self,a)] for a in updates if not a.startswith("_da_") and not a.startswith("set_") and not a.startswith("use_") and not a.startswith("input") and not a.startswith('assumptions')])
            if embed_datafiles:
                res=[]
                for a,b in r.items():
                    if isinstance(b,DataFile):
                        if b.size<100e3:
                            try:
                                content=json.load(b.open())
                            except:    
                                content=b.open().read()
                            
                            try:
                                json.dumps(content)
                            except:
                                content=b.open().read()
                                content=str(b)+" can not encode "+str(b.size)

                            res.append([a,content]) 
                        else:
                            res.append([a,str(b)+" too big "+str(b.size)]) 
                    else:
                        res.append([a,b])
                r=dict(res)


        cprint("resulting output:",r)
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
        ss="_".join(["%s:%s"%(a,repr(getattr(self,a))) for a in self._da_settings]) if self._da_settings is not None else ""
        return self.get_signature()+"."+self.version+("."+ss if ss!="" else "")

    def get_signature(self):
        a=self.get_formatted_attributes()
        if a!="": a="."+a
    
        state=self.get_state()
        if state is not None:
            a=a+"."+state
        
        if hasattr(self,'name'):
            name=self.name
        else:
            name=self.__class__.__name__
        return name+a

    def get_state(self):
        if not hasattr(self,'_da_state'):
            self._da_state=self.compute_state()
        return self._da_state

    def compute_state(self):
        return None

    def process_output_files(self,hashe):
        """
        rename output files to unique filenames
        """
        content=self.export_data()
        if isinstance(content,dict):
            for a,b in content.items(): 
                if isinstance(b,DataFile):
                    dest_unique=b.path+"."+self.cache.hashe2signature(hashe) # will it always?
                    b._da_unique_local_path=dest_unique
                    shutil.copyfile(b.path,dest_unique)
                    cprint("post-processing DataFile",b,"as",b._da_unique_local_path,log='datafile')

    def store_cache(self,fih):
        """
    store output with
        """

        cprint(render("{MAGENTA}storing in cache{/}"))
        cprint("hashe:",fih)

     #   c=MemCacheLocal.store(fih,self.export_data())
        #cprint(render("{MAGENTA}this is non-cached analysis, reduced caching: only transient{/}"))
        TransientCacheInstance.store(fih,self)
        self.cache.store(fih,self)
        #c=MemCacheLocal.store(oh,self.export_data())
    
    def retrieve_cache(self,fih,rc=None):
        cprint("requested cache for",fih)

        if self._da_locally_complete is not None:
            cprint("this object has been already restored and complete",self)
            if self._da_locally_complete == fih:
                cprint("this object has been completed with the neccessary hash: no need to recover state",self)
                if not hasattr(self,'_da_recovered_restore_config'):
                    cprint("the object has not record of restore config",level='top')
                    return True
                if rc==self._da_recovered_restore_config:
                    cprint("the object was recovered with the same restore config:",rc,self._da_recovered_restore_config,level='top')
                    return True
                cprint("the object was recovered with a different restore config:",self._da_recovered_restore_config,'became',rc,level='top')
            else:
                cprint("state of this object isincompatible with the requested!")
                cprint(" was: ",self._da_locally_complete)
                cprint(" now: ",fih)
                #raise Exception("hm, changing analysis dictionary?")
                cprint("hm, changing analysis dictionary?","{log:thoughts}")
    
                if self.run_for_hashe:
                    cprint("object is run_for_hashe, this is probably the reason")
        
                return None

        if rc is None:
            rc={}
            
        r=TransientCacheInstance.restore(fih,self,rc)
        
        if r and r is not None:
            cprint("restored from transient: this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=True # info save
            return r

        if not self.cached:
            cprint(render("{MAGENTA}not cached restore only from transient{/}"))
            return None # only transient! 
        # discover through different caches
        #c=MemCacheLocal.find(fih)

        r=self.cache.restore(fih,self,rc)

        if r and r is not None:
            cprint("this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=fih # info save
            cprint("locally complete:",fih)
            return r
        return r # twice


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

    def get(self,**aa):
        return self.process(output_required=True,**aa)[1]
        
    def process_checkin_assumptions(self):
        if self.assumptions!=[]:
            cprint("cache assumptions:",AnalysisFactory.cache_assumptions)
            cprint("assumptions:",self.assumptions)
            cprint("non-trivial assumptions require copy of the analysis tree")
            AnalysisFactory.WhatIfCopy("requested by "+repr(self),self.assumptions)
            for a in self.assumptions:
                a.promote()
        else:
            cprint("no special assumptions") 

    def process_checkout_assumptions(self):
        cprint("assumptions checkout")
        if self.assumptions!=[]:
            AnalysisFactory.WhatIfNot()

    def process_restore_rules(self,restore_rules,extra):
        cprint("suggested restore rules:",restore_rules)
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
                    run_if_can_not_delegate=True,
                    force_complete=True,
                    force_complete_input=True)
        restore_rules=dict(restore_rules_default.items()+(restore_rules.items() if restore_rules is not None else []))
        # to simplify input
        for k in extra.keys():
            if k in restore_rules:
                restore_rules[k]=extra[k]
        
        # always run to process
        restore_rules['substitute_output_required']=restore_rules['output_required']
        if self.run_for_hashe:
            cprint(render("{BLUE}this analysis has to run for hashe! this will be treated later, requiring output{/}"))
            cprint(render("{BLUE}original object output required?{/}"+str(restore_rules['output_required'])))
            restore_rules['output_required']=True
            restore_rules['explicit_input_required']=True
            restore_rules['input_runs_if_haveto']=True
            restore_rules['run_if_haveto']=True

        #restore_rules['force_complete_input']=self.force_complete_input

        cprint("will use restore_rules:",restore_rules)
        return restore_rules

    def process_restore_config(self,restore_config):
        rc=restore_config # this is for data restore modes, passed to cache
        if restore_config is None:
            rc={'datafile_restore_mode':'copy'}
        
        cprint('restore_config:',rc)
        return restore_config


    def process_timespent_interpret(self):
        tspent=self.time_spent_in_main
        if tspent<self.min_timespent_tocache and self.cached:
            cprint(render("{RED}requested to cache fast analysis!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache)))
            if self.allow_timespent_adjustment:
                cprint(render("{MAGENTA}temporarily disabling caching for this analysis{/}"))
                self.cached=False
            else:
                cprint("being ignorant about it")
        
            if tspent<self.min_timespent_tocache_hard and self.cached:
                if self.hard_timespent_checks:
                    estr=render("{RED}requested to cache fast analysis, hard limit reached!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache_hard))
                    raise Exception(estr)
                else:
                    cprint("ignoring hard limit on request")

        if tspent>self.max_timespent_tocache and not self.cached:
            cprint(render("{BLUE}analysis takes a lot of time but not cached, recommendation is to cache!{/}"),"{log:advice}")

    def treat_input_analysis_exceptions(self,analysis_exceptions):
        return False

    def get_exceptions(self):
        if not hasattr(self,'analysis_exceptions'):
            return []
        return self.analysis_exceptions

    def note_analysis_exception(self,ae):
        if not hasattr(self,'analysis_exceptions'):
            self.analysis_exceptions=[]
        self.analysis_exceptions.append((self.get_signature(),ae))

    watched_analysis=False
        
    def start_main_watchdog(self):
        cprint("main watchdog")
        self.report_runtime("starting")
        if not self.watched_analysis: return
        # do it?
        self.cache.report_analysis_state(self,"running")
        
    
    def stop_main_watchdog(self):
        self.report_runtime("done")
        if not self.watched_analysis: return
        self.cache.report_analysis_state(self,"done")

    def process_run_main(self):
        #self.runtime_update('running')
        if self.abstract:
            raise Exception("attempting to run abstract! :"+repr(self)+" requested by "+(" ".join(self._da_requested_by)))

        dll=self.default_log_level
        self.default_log_level="main"

        cprint(render("{RED}running main{/}"),'{log:top}')
        t0=time.time()
        main_log=StringIO.StringIO()
        main_logstream=printhook.LogStream(main_log,lambda x:True)
        cprint("starting main log stream",main_log,main_logstream,level='logstreams')

        self.start_main_watchdog()

        try:
            mr=self.main() # main!
        except AnalysisException as ae:
            self.note_analysis_exception(ae)
            mr=None
        except Exception as e:
            #os.system("ls -ltor")
            self.stop_main_watchdog()
            os.system("echo current dir;pwd")
            self.cache.report_exception(self,e)
            self.report_runtime("failed "+repr(e))
            raise
        self.stop_main_watchdog()

        main_logstream.forget()
        self._da_main_log_content=main_log.getvalue()
        main_log.close()
        cprint("closing main log stream",main_log,main_logstream,level="logstreams")

        tspent=time.time()-t0
        self.time_spent_in_main=tspent
        cprint(render("{RED}finished main{/} in {MAGENTA}%.5lg seconds{/}"%tspent),'{log:resources}')
        self.report_runtime("done in %g seconds"%tspent)
        self.note_resource_stats({'resource_type':'runtime','seconds':tspent})

        self.default_log_level=dll
        #self.runtime_update("storing")

        if mr is not None:
            cprint("main returns",mr,"attaching to the object as list")

            if isinstance(mr, collections.Iterable):
                mr=list(mr)
            else:
                mr=[mr]
            for r in mr:
                if isinstance(r,DataAnalysis):
                    cprint("returned dataanalysis:",r,"assumptions:",r.assumptions)
            setattr(self,'output',mr)

    def process_find_output_objects(self):
        da=list(objwalk(self.export_data(),sel=lambda y:isdataanalysis(y)))
        if da!=[]:
            cprint(render("{BLUE}resulting object exports dataanalysis, should be considered:{/}"),da)
            cprint(render("{RED}carefull!{/} it will substitute the object!"),da)
    
            if len(da)==1:
                da=da[0]
        return da
    
    def process_implement_output_objects(self,output_objects,implemented_objects):
        cprint("was",output_objects,level='output_objects')
        cprint("has",implemented_objects,level='output_objects')


        try:
            for newobj,obj in zip(implemented_objects,output_objects):
                cprint("replace",obj,"with",newobj,level='top')
        except TypeError:
            implemented_objects=[implemented_objects]
            output_objects=[output_objects]

        for newobj,obj in zip(implemented_objects,output_objects):
            cprint("replace",obj,"with",newobj,level='top')

            #for key in newobj.))dir: 
            for key in newobj.export_data().keys(): # or all??
                setattr(obj,key,getattr(newobj,key))

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
                cprint("input delegated:",input,d)
                return [d]
            return []

        raise Exception("can not understand input: "+repr(input))
    
    def process_list_analysis_exceptions(self,input):
        # walk input recursively
        if isinstance(input,list) or isinstance(input,tuple):
            l=[]
            for input_item in input:
                l+=self.process_list_analysis_exceptions(input_item)
            return l
            
        if isinstance(input,DataAnalysis):
            if hasattr(input,'analysis_exceptions'):
                return input.analysis_exceptions
            return []
        
        if input is None:
            cprint("input is None, it is fine")
            return []

        raise Exception("can not understand input: "+repr(input))

    def process_verify_inputs(self,input):
        # walk input recursively
        if not self.force_complete_input:
            cprint("loose input evaluation - not forced")
            return

        if isinstance(input,list) or isinstance(input,tuple):
            for input_item in input:
                self.process_verify_inputs(input_item)
            return

        if isinstance(input,DataAnalysis):
            cprint("will verify:",input)
            if not input._da_locally_complete:
                cprint("input is not completed! this should not happen!",input)
                raise("input is not completed! this should not happen!")
            return

        if input is None:
            cprint("input is None, it is fine")
            return

        raise Exception("can not understand input: "+repr(input))

    use_hashe=None

    def process_substitute_hashe(self,fih):
        if self.use_hashe is not None:
            substitute_hashe=self.use_hashe[0]
            hashe_mappings=self.use_hashe[1:]

            for a,b in hashe_mappings:
                cprint("mapping",a,b,getattr(self,b)._da_expected_full_hashe)
                substitute_hashe=hashe_replace_object(substitute_hashe,a,getattr(self,b)._da_expected_full_hashe)

            cprint("using substitute hashe:",substitute_hashe)
            cprint("instead of:",fih)
            return substitute_hashe
        else:
            return fih

    report_runtime_destination=None
    runtime_id=None

    def report_runtime(self,message): # separet
        if self.report_runtime_destination is None: return
        try:
            if not self.report_runtime_destination.startswith("mysql://"): return
            dbname,table=self.report_runtime_destination[8:].split(".")
            print("state goes to",dbname,table)

            import MySQLdb
            db = MySQLdb.connect(host="apcclwn12",
                      user="root",
                      port=42512,
                      #unix_socket="/workdir/savchenk/mysql/var/mysql.socket",
                      passwd=open(os.environ['HOME']+"/.secret_mysql_password").read().strip(), # your password
                      db=dbname)

            import socket

            if self.runtime_id is None:
                import random
                self.runtime_id=random.randint(0,10000000)

            cur=db.cursor()
            cur.execute("INSERT INTO "+table+" (analysis,host,date,message,id) VALUES (%s,%s,NOW(),%s,%s)",(self.get_version(),socket.gethostname(),message,self.runtime_id))

            db.commit()
            db.close()

        except Exception as e:
            print("failed:",e)


    def process(self,process_function=None,restore_rules=None,restore_config=None,requested_by=None,**extra):
        cprint(render("{BLUE}PROCESS{/}"))

        if requested_by is None:
            requested_by=['direct']

        cprint('cache assumptions:',AnalysisFactory.cache_assumptions,'{log:top}')
        cprint('object assumptions:',self.assumptions,'{log:top}')


        restore_config=self.process_restore_config(restore_config)
        restore_rules=self.process_restore_rules(restore_rules,extra)
        
        cprint(render("{BLUE}requested "+("OUTPUT" if restore_rules['output_required'] else "")+" by{/} "+" ".join(requested_by)),'{log:top}')
        requested_by=[("+" if restore_rules['output_required'] else "-")+self.get_version()]+requested_by

        self._da_requested_by=requested_by

        self.process_checkin_assumptions()

        rr= dict(restore_rules.items() + dict(output_required=False).items()) # no need to request input results unless see below

        #### process input
        input_hash,input=self.process_input(obj=None,
                                            process_function=process_function,
                                            restore_rules=update_dict(restore_rules,dict(
                                                                                            output_required=False,
                                                                                            can_delegate=restore_rules['can_delegate_input'])), 
                                            requested_by=["input_of"]+requested_by) 
        # TODO: defaults, alternatives
        #### /process input
        
        process_t0=time.time()

        cprint("input hash:",input_hash)
        cprint("input objects:",input)

        fih=('analysis',input_hash,self.get_version()) # construct hash
        cprint("full hash:",fih)

        fih=self.process_substitute_hashe(fih)

        self._da_expected_full_hashe=fih

        substitute_object=None

        if restore_rules['output_required']: # 
            cprint("output required, try to GET from cache")
            if self.retrieve_cache(fih,restore_config): # will not happen with self.run_for_hashe
                cprint("cache found and retrieved",'{log:top}')
                cprint(fih,'{log:top}')
            else:
                cprint("no cache",'{log:top}')
                cprint(fih,'{log:top}')
                        
                if hasattr(self,'produce_disabled') and self.produce_disabled:
                    if restore_rules['force_complete']:
                        raise Exception("not allowed to produce but has to! at "+repr(self)+"; hashe: "+repr(fih))
                    else:
                        self.incomplete=True
                        return fih,self


                if restore_rules['explicit_input_required']:
                    cprint("exclicite input is available")
                else:
                    cprint("need to guarantee that explicit input is available")

                    ## if output has to be generated, but explicite input was not prepared, do it
                    ## process
                    return self.process(process_function=process_function,
                                        restore_rules=update_dict(restore_rules,dict(explicit_input_required=True)),requested_by=['output_required_by_parent']+requested_by )
                                        #restore_rules=update_dict(restore_rules,dict(output_required=True,explicit_input_required=True)) )
                    ##  /process

                delegated_inputs=self.process_list_delegated_inputs(input)
                if delegated_inputs!=[]:
                    cprint("some input was delegated:",delegated_inputs)
                    cprint(render("{RED}waiting for delegated input!{/}"))
                    self._da_delegated_input=delegated_inputs

                if restore_rules['can_delegate'] and self.cached:
                    cprint("will delegate this analysis") 
                    hashekey=self.cache.register_delegation(self,fih)
                    self._da_main_delegated=hashekey
                    return fih,self # RETURN!

             # check if can and inpust  relaxe

                if delegated_inputs!=[]:
                    cprint("analysis design problem! input was delegated but the analysis can not be. wait until the input is done!")
                    raise

                self.process_verify_inputs(input)

                # check if input had exceptions
                analysis_exceptions=self.process_list_analysis_exceptions(input)
                if analysis_exceptions!=[]:
                    cprint("found analysis exceptions in the input:",analysis_exceptions)
                    if not self.treat_input_analysis_exceptions(analysis_exceptions):
                        if not hasattr(self,'analysis_exceptions'):
                            self.analysis_exceptions=[]
                        self.analysis_exceptions+=analysis_exceptions
                        cprint(render("{RED}ANALYSIS EXCEPTIONS:{/}"),analysis_exceptions,level='top')
                    else:
                        analysis_exceptions=[]
                # exceptions
                
                if analysis_exceptions==[]:
                    if restore_rules['run_if_can_not_delegate']:
                        cprint("no way was able to delegate, but all ready to run and allowed. will run")
                    else:
                        cprint("not allowed to run here. hopefully will run as part of higher-level delegation") 
                        raise Exception("not allowed to run but has to (delegation)!")
                        return fih,self # RETURN!

                    if restore_rules['run_if_haveto'] or self.run_for_hashe:
                        
                        mr=self.process_run_main() # MAIN!
                        self.process_timespent_interpret()
                    else:
                        raise Exception("not allowed to run but has to! at "+repr(self))

                    #cprint("new output:",self.export_data())

                    if self.rename_output_unique:
                        self.process_output_files(fih)
                    else:
                        cprint("disabled self.rename_output_unique",level='cache')

                    self.store_cache(fih)
                    #self.runtime_update("done")

            output_objects=self.process_find_output_objects()
            if output_objects!=[]:
                da=output_objects
                if self.cached:
                    cprint(render("{RED}can not be cached - can not save non-virtual objects! (at the moment){/}"),da)
                    self.cached=False

                    
                #restore_rules_for_substitute=update_dict(restore_rules,dict(explicit_input_required=False))
                restore_rules_for_substitute=update_dict(restore_rules,dict(explicit_input_required=restore_rules['substitute_output_required']))
                self.force_complete_input=restore_rules['force_complete'] # ?.. !!!!
                cprint(render("{RED}will process substitute object as input with the following rules:{/}"),restore_rules_for_substitute)

                rh,ro=self.process_input(da,restore_rules=restore_rules_for_substitute,requested_by=['output_of']+requested_by)
                cprint(render("substitute the object with dynamic input. rh:"),rh)
                cprint(render("substitute the object with dynamic input. ro:"),ro)


                cprint("--- old input hash:",fih)
                if self.allow_alias:
                    self.register_alias(fih,rh)
                    self.process_implement_output_objects(output_objects,ro)
                else:
                    cprint("alias is not allowed: using full input hash!")
                    fih=rh
                    substitute_object=ro
                    cprint("+++ new input hash:",fih)
            
            cprint("processing finished, main, object is locally complete")
            cprint("locally complete:",id(self))
            cprint("locally complete:",fih,'{log:top}')
            self._da_locally_complete=fih
        else:
            cprint("NO output is strictly required, will not attempt to get")
            if restore_rules['restore_complete']: 
                cprint("however, diagnostic complete restore is requested, trying to restore")
                if self.retrieve_cache(fih,rc):
                    cprint("cache found and retrieved",'{log:top}')
                    cprint("processing finished, object is locally complete")
                    self._da_locally_complete=fih
                    cprint("locally complete:",fih,'{log:top}')
                else:
                    cprint("NO cache found",'{log:top}')
        
        self.process_checkout_assumptions()

        process_tspent=time.time()-process_t0
        cprint(render("{MAGENTA}process took in total{/}"),process_tspent)
        self.note_resource_stats({'resource_type':'usertime','seconds':process_tspent})
        self.summarize_resource_stats()

        return_object=self
        if substitute_object is not None:
            cprint("returning substituted object")
            return_object=substitute_object

        cprint("PROCESS done",fih,return_object)
        return fih,return_object

    def register_alias(self,hash1,hash2):
        cprint("alias:",hash1)
        cprint("stands for",hash2)
        self.alias=hash2
        AnalysisFactory.register_alias(hash1,hash2)

    def process_input(self,obj=None,process_function=None,restore_rules=None,restore_config=None,requested_by=None,**extra):
        """
        walk over all input; apply process_function and implement if neccessary
        """

        cprint("{CYAN}PROCESS INPUT{/}")

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
            #cprint("will copy cached input")
            restore_config['datafile_restore_mode']="copy"
        else:
            #cprint("will NOT copy cached input")
            restore_config['datafile_restore_mode']="url_in_object"
        if self.test_files:
            restore_config['test_files']=True
        else:
            restore_config['test_files']=False
        
        if self.force_complete_input:
            restore_rules['force_complete']=True
        else:
            restore_rules['force_complete']=False
            cprint("input will not be forced!")
        
        
        cprint("input restore_rules:",restore_rules)
        cprint("input restore_config:",restore_config)
        
        if obj is None:
            # start from the object dir, look for input
            inputhashes=[]
            inputs=[]
            for a in dir(self):
                if a.startswith("input"):
                    o=getattr(self,a)
                    cprint("input item",a,o)
                    if o is NoAnalysis:
                        cprint("NoAnalysis:",o) #,o.__class__)
                        continue # yes?

                    if o is None:
                        raise Exception("input is None: vortual class: "+repr(self)+" input "+a+" requested by "+" ".join(requested_by))
                    h,l=self.process_input(obj=o,process_function=process_function,restore_rules=restore_rules,restore_config=restore_config,requested_by=requested_by)

                    if hasattr(l,'noanalysis') and l.noanalysis:
                        cprint("NoAnalysis:",o,o.__class__)
                        continue # yes?

                    if l is not None: 
                        cprint("input item",a)
                        cprint("implemented as",h,l)
                        setattr(self,a,l)
                    else:
                        cprint("input item None!",a,l)
                        raise Exception("?")

                    inputhashes.append(h)
                    inputs.append(l)

            if len(inputhashes)>1:
                return ('list',)+tuple(inputhashes),inputs
            if len(inputhashes)==1:
                return inputhashes[0],inputs[0]

            cprint("this class has no input! origin class")
            # does this ever happen?
            return None,None
        else:
            # process given input structure
            cprint("parse and get",obj,obj.__class__)

            # list or tuple
            if isinstance(obj,list) or isinstance(obj,tuple):
                cprint("parse list")
                hashes=[]
                nitems=[]
                for i in obj:
                    cprint("item:",i)
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
        if hasattr(item,'noanalysis') and item.noanalysis:
            cprint("noanalysis!")
            return None,item

        rr=dict(restore_rules.items()+dict(explicit_input_required=False,output_required=restore_rules['explicit_input_required']).items())
        if self.run_for_hashe:
            cprint("for run_for_hashe, input need a right to run")
            restore_rules['run_if_haveto']=True

        
        cprint("proceeding to run",item,"rules",restore_rules)

        try:
            cprint("item:",item._da_locally_complete)
        except Exception as e:
            raise Exception(str(item)+" has no locally complete!")
        input_hash,newitem=item.process(process_function=process_function,restore_rules=rr,restore_config=restore_config,requested_by=requested_by) # recursively for all inputs process input
        cprint("process_input finishing at the end",input_hash,newitem)
        
        if process_function is not None:
            process_function(self,newitem) # then run something to each input item

        return input_hash,newitem # return path to the item (hash) and the item

    #def get_output_hash(self):
    #    return shhash(tuple(sorted(self.export_data().items())))

    _da_resource_stats=None

    def is_noanalysis(self):
        if not hasattr(self,'noanalysis'): return False
        if isinstance(self,NoAnalysis): return True
        return self.noanalysis

    def note_resource_stats(self,info):
        if self._da_resource_stats is None:
            self._da_resource_stats=[]
        cprint('note resource stats:',info['resource_type'],'{log:resources}')
        self._da_resource_stats.append(info)
    
    def summarize_resource_stats(self):
        total_usertime=sum([a['seconds'] for a in self._da_resource_stats if a['resource_type']=='usertime'])
        cprint(render("collected resource stats, total {MAGENTA}usertime{/}"),total_usertime,'{log:resources}')
        
        total_runtime=sum([a['seconds'] for a in self._da_resource_stats if a['resource_type']=='runtime'])
        cprint(render("collected resource stats, total {MAGENTA}run time{/}"),total_runtime,'{log:resources}')
        
        total_cachetime=sum([a['stats']['copytime'] for a in self._da_resource_stats if a['resource_type']=='cache'])
        cprint(render("collected resource stats, total {MAGENTA}cache copy time{/}"),total_cachetime,'{log:resources}')

        self.resource_stats={
                                'total_usertime':total_usertime,
                                'total_runtime':total_runtime,
                                'total_cachetime':total_cachetime
                            }


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



class FileHashed(DataAnalysis):
    input_filename=None

    cached=False # never
    infactory=False
    run_for_hashe=True


    def main(self): # pointless unless fine has known hashe!
        self.md5=hash_for_file(open(self.input_filename.handle))
        return DataHandle(self.input_filename.handle+":md5:"+self.md5[:8])

    def get_filename(self):
        return self.input_filename.str()

class HashedFile(DataAnalysis):
    filename=None
    md5=None

    cached=False # never
    infactory=False

    def get_signature(self):
        return "File:"+os.path.basename(self.filename)+":"+self.md5[:5]

class HasheForFile(DataAnalysis):
    input_filename=None

    cached=False # never
    infactory=False
    run_for_hashe=True


    def main(self):
        md5=hash_for_file(open(self.input_filename.str()))
        return HashedFile(use_md5=md5,use_filename=self.input_filename.str())

class DataHandle(DataAnalysis):
    infactory=False

    def __new__(self,*a,**args): # not only kw
        return object.__new__(self)

    def __init__(self,h=None):
        self.handle=h

    def process(self,**args):
        cprint("datahandle is hash",self)
        self._da_locally_complete=True
        return self.handle,self

    def __repr__(self):
        return '[%s]'%self.handle

    
class DataAnalysisGroup(DataAnalysis): # make it 
    def process(self):
        pass

# imported

# abstract

class AnyAnalysis(DataAnalysis):
    def main(self):
        raise Exception("requested to run abstract any analysis!")


class DataFile(DataAnalysis):
    cached_path_valid_url=False

    infactory=False

    size=None

    def __init__(self,fn=None):
        self.path=fn
        self.size=os.path.getsize(fn)

    def get_cached_path(self): # not work properly if many run!
        return self.cached_path if hasattr(self,'cached_path') else self.path
    
    def get_path(self): # not work properly if many run!
# confis
        #cprint("get path:",self,self.cached_path,self.cached_path_valid_url) #,self.restored_mode)

        if hasattr(self,'cached_path') and self.cached_path_valid_url:
            return self.cached_path
            
        if hasattr(self,'_da_unique_local_path'):
            return self._da_unique_local_path

        if not hasattr(self,'restored_mode'):
            return self.path

        if self.restored_mode=="copy":
            cprint("datafile copied by no local path?",self,id(self))
            raise Exception("inconsistency!")
 #       raise Exception("inconsistency!")
            
    def get_full_path(self):
        if hasattr(self,'restored_path_prefix'):
            return self.restored_path_prefix+"/"+self.get_path()
        else:
            if hasattr(self,'cached_path') and self.cached_path_valid_url:
                return self.cached_path
            else:
                raise Exception("unable to construct full path!")

    def open(self):
        return gzip.open(self.cached_path) if hasattr(self,'cached_path') else open(self.path)

    def __repr__(self):
        return "[DataFile:%s]"%(self.path if hasattr(self,'path') else 'undefined')

class DataFileStatic(DataFile):
    cached_path_valid_url=False

    def __init__(self,fn=None):
        self.path=fn

    def get_cached_path(self): # not work properly if many run!
        return self.path
    
    def get_path(self): # not work properly if many run!
        return self.path

    def open(self):
        return gzip.open(self.cached_path) if hasattr(self,'cached_path') else open(self.path)

    def __repr__(self):
        return "[DataFileStatic:%s]"%(self.path if hasattr(self,'path') else 'undefined')
