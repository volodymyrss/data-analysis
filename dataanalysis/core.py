from io import StringIO
from pathlib import Path
import collections
import threading
import gzip
import json
import os
import errno
import tempfile
import re
import pprint
import shutil
import sys
import traceback
import time
import glob
import socket
from collections import Mapping, Set, Sequence, OrderedDict

from future.utils import with_metaclass

from dataanalysis import hashtools
from dataanalysis import jsonify
from dataanalysis.bcolors import render
import dataanalysis.bcolors as bcolors
from dataanalysis.caches import cache_core

from dataanalysis.analysisfactory import AnalysisFactory

from dataanalysis import printhook
from dataanalysis.printhook import decorate_method_log,debug_print,log_hook

from dataanalysis.printhook import get_local_log
from functools import reduce

log = get_local_log(__name__)

from dataanalysis.callback import CallbackHook

def repr_short(d):
    lim = 100

    r = repr(d)

    if len(r)<lim*2:
        return r

    return r[:lim] + " ... " + r[-lim:]

dda_hooks=[CallbackHook(),log_hook]

Cache = cache_core.CacheNoIndex()
TransientCacheInstance = cache_core.TransientCache()


# TODO:

# doi. contexual projection

# once identical data was produced, forget about the origins
# configuration nodes have this property: their

# approprate logging
# reporting classes
# cache migration
# hashe to picture
# deleting files: cleanup after analysis

# data intergity: prevent corruption during copying to/from hashe

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


# dual python 2/3 compatability, inspired by the "six" library
string_types = (str, str) if str is bytes else (str, bytes)
iteritems = lambda mapping: getattr(mapping, 'iteritems', mapping.items)()

#class DataHandle:
#    trivial = True

def named(name):
    return NamedAnalysis(name)

class NoAnalysis():
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
    #log("walking in",obj,path)
    if memo is None:
        memo = set()
    iterator = None
    if isinstance(obj, Mapping):
        iterator = iteritems
    elif isinstance(obj, (Sequence, Set)) and not isinstance(obj, string_types):
        iterator = enumerate
    if iterator:
        #log("walking interate")
        if id(obj) not in memo:
            memo.add(id(obj))
            for path_component, value in iterator(obj):
         #       log("walking",path,path_component,value)
                for result in objwalk(value, path + (path_component,), memo, sel):
                    yield result
            memo.remove(id(obj))
    else:
        #log("walk end",path,obj)
        if sel(obj):
         #   log("walk selected",path,obj)
            yield obj

#/objwalk

def update_dict(a,b):
    return dict(list(a.items())+list(b.items()))

class UnhandledAnalysisException(Exception):
    def __init__(self,analysis_node,main_log,exception):
        self.argdict=dict(
            analysis_node_name = repr(analysis_node),
            requested_by = " ".join(analysis_node._da_requested_by),
            main_log = main_log,
            exception=exception,
            exc_info=sys.exc_info(),
            tb=traceback.format_exc(),
        )

    def __repr__(self):
        log(self.argdict)
        return  "\n\n>>> main log\n" + \
                "\n".join([">>> "+l for l in self.argdict['main_log'].split("\n")])+ "\n\n" + \
                self.argdict['tb']+"\n"+ \
                ">>> "+repr(self.argdict['exception']) + '\n\n'+ \
                "in DataAnalysis node: "+self.argdict['analysis_node_name'] + '\n' +\
                "requested by: "+self.argdict['requested_by'] + '\n'

    def __str__(self):
        return repr(self)

class ConsistencyException(Exception):
    pass

class ProduceDisabledException(Exception):
    pass

def flatten_nested_structure(structure, mapping, path=[]):
    if isinstance(structure, list):
        r=[flatten_nested_structure(a, mapping, path=path + [i]) for i, a in enumerate(structure)]
        return reduce(lambda x, y: x + y, r) if len(r) > 0 else r

    if isinstance(structure, dict):
        r=[flatten_nested_structure(a, mapping, path=path + [k]) for k, a in list(structure.items())]
        return reduce(lambda x,y:x+y,r) if len(r)>0 else r

    return [mapping(path, structure)]


def map_nested_structure(structure, mapping, path=None):
    if path is None:
        path=[]

    if isinstance(structure, list):
        return [map_nested_structure(a, mapping, path=path + [i]) for i, a in enumerate(structure)]

    if isinstance(structure, dict):
        return dict([(k, map_nested_structure(a, mapping, path=path + [k])) for k, a in list(structure.items())])

    return mapping(path, structure)


class AnalysisException(Exception):
    @classmethod
    def from_list(cls,exceptions):
        if len(exceptions)==1:
            if isinstance(exceptions[0],Exception):
                return exceptions[0]
            else:
                return AnalysisException(exceptions[0])

        obj=cls(exceptions)

        return obj

class AnalysisDelegatedException(Exception):
    @property
    def signature(self):
        if self.hashe is None:
            return self._comment

        if self.hashe[0]=="analysis":
            return self.hashe[-1]

        if self.hashe[0] == "list":
            return "; ".join([repr(k[-1]) for k in self.hashe[1:]])

    def __init__(self,hashe,resources=None,comment=None,origin=None, delegation_state=None):
        self.hashe=hashe
        self.resources=[] if resources is None else resources
        self.source_exceptions=None
        self._comment=comment
        self.origin=origin
        self.delegation_state=delegation_state

    @property
    def comment(self):
        if self._comment is not None:
            return self._comment
        return ""

    @property
    def delegation_states(self):
        if isinstance(self.delegation_state,list):
            return self.delegation_state
        else:
            return [self.delegation_state]

    @classmethod
    def from_list(cls,exceptions):
        if len(exceptions)==1:
            return exceptions[0]

        obj=cls(None,origin="list")

        obj.source_exceptions=exceptions

        obj.resources=[]
        obj.delegation_state=[]
        for ex in exceptions:
            obj.resources+=ex.resources
            obj.delegation_state.append(ex.delegation_state)

        obj.hashe=tuple(['list']+[ex.hashe for ex in exceptions])

        return obj

    def __repr__(self):
        return "[{}: {}; {}; {}]".format(self.__class__.__name__,self.signature, self.comment, self.delegation_state)

    def __str__(self):
        return repr(self)

class decorate_all_methods(type):
    def __new__(cls, name, bases, local):
        # also store in the dict

        # decorate
        if printhook.global_fancy_output:
            for attr in local:
                value = local[attr]
                if callable(value) and not isinstance(value,type):
                    local[attr] = decorate_method_log(value)
        else:
            if printhook.global_catch_main_output and 'main' in local:
                debug_print("decorating only main in "+repr(cls)+"; "+repr(name))
                local['main'] = decorate_method_log(local['main'])

        c=type.__new__(cls, name, bases, local)

        if not c.trivial:
            log("declaring analysis class",name,)
            log("   constructing object...")
            o=c(update=True,origins=["from_class","with_metaclass"])
            log("   registered",o)

        return c


class DataAnalysisIdentity(object):
    def __init__(self,
                 factory_name,
                 full_name,
                 modules,
                 assumptions,
                 expected_hashe):
        self.factory_name=factory_name
        self.full_name=full_name
        self.modules=modules
        self.assumptions=assumptions
        self.expected_hashe=hashtools.hashe_replace_object(expected_hashe,None,'None')

    def get_modules_loadable(self):
        return [m[1] for m in self.modules]

    def __repr__(self):
        return "[%s: %s; %s]"%(self.factory_name,
                               ",".join([m for o,m,l in self.modules]),
                               ",".join([a if a is not None else "custom eval" for a,b in self.assumptions]))

    def serialize(self):
        result=self.__dict__

        return result

        serialized_assumptions=[]
        for assumption in result['assumptions']:
            if isinstance(assumption,tuple) and isinstance(assumption[1],dict):
                for k,v in list(assumption[1].items()):
                    if v is None:
                        assumption[1][k]='None'
                serialized_assumptions.append((assumption[0],OrderedDict(sorted(assumption[1].items()))))
            else:
                serialized_assumptions.append((assumption[0], assumption[1]))

        result['assumptions']=sorted(serialized_assumptions, key=lambda x:x[0])
        return OrderedDict(sorted(result.items()))

    @classmethod
    def from_dict(cls,d):
        obj=cls(
            factory_name=d['factory_name'],
            full_name=d['full_name'],
            modules=d['modules'],
            assumptions=d['assumptions'],
            expected_hashe=d['expected_hashe'],
        )
        return obj


class DataAnalysis(with_metaclass(decorate_all_methods, object)):
#class DataAnalysis(object):
    #__metaclass__ = decorate_all_methods

    infactory=True
    trivial=False # those that do not need to be computed

    factory=AnalysisFactory
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
    test_files_if_failed=True

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

    rename_output_unique=True

    force_complete_input=True

    def is_virtual(self):
        #if self.run_for_hashe: return False
        return self.virtual

    _da_restored=None
    _da_locally_complete=None
    _da_main_delegated=None
    _da_main_log_content=""
    _da_delegated_input=None
    _da_ignore_output_objects=False

    write_caches=[cache_core.Cache]
    read_caches=[cache_core.Cache]

    _da_settings=None

    def str(self):
        if hasattr(self,'handle'): return self.handle
        return repr(self)

    def clone(self):
        obj=self.__class__(dynamic=False)

        obj.import_data(self.export_data())

        return obj

    def get_factory_name(self):
        if hasattr(self, 'name'):
            name = self.name
        else:
            name = self.__class__.__name__

        return name

    def get_all_assumptions(self, reduce_assumptions_with_hashe=True):
        all_possible_assumptions = self.factory.factory_assumptions_stacked + self.assumptions

        if reduce_assumptions_with_hashe:        
            assumptions = []

            for assumption in all_possible_assumptions:
                print("DDA_DEBUG_TASKDATA:\n\033[36m{}\033[0m".format(
                    f"checking if {assumption} ({assumption.__class__}) belongs"
                ))

                if assumption in assumptions:
                    print("duplicate: popping")                    
                    assumptions = [a for a in assumptions if a != assumption]                

                # if not hashtools.find_object(self.expected_hashe, assumption.expected_hashe):
                #     # print("\033[31mNOT found expected_hashe:\033[0m", assumption.expected_hashe)
                #     # json.dump(assumption.expected_hashe, open('assumption_expected_hashe.json', 'w'))
                #     # json.dump(self.expected_hashe, open('expected_hashe.json', 'w'))                        
                #     # raise RuntimeError

                _assumptions = []
                for _a in assumptions:
                    if _a != assumption and assumption.get_factory_name() == _a.get_factory_name():
                        print(f"\033[31moverriding previous assumption: {_a} => {assumption}\033[0m")
                    else:
                        _assumptions.append(_a)

                assumptions = _assumptions
                    
                # else:
                #     print("found expected_hashe:", assumption.expected_hashe)
                
                assumptions.append(assumption)

            print(f"\033[31m now assumptions {len(assumptions)} / {len(all_possible_assumptions)}\033[0m")                
        else:
            assumptions = all_possible_assumptions
        
        return assumptions

    def get_identity(self):
        log("assembling portable identity", level="top")
        log("object assumptions:",self.assumptions)
        log("factory assumptions:", self.factory.cache_assumptions)

        assumptions = []
        for a in [a.serialize() for a in self.get_all_assumptions()]:
            if len(assumptions) == 0 or assumptions[-1] != a:
                assumptions.append(a)

        object_identity = DataAnalysisIdentity(
            factory_name=self.get_factory_name(),
            full_name=self.get_version(),
            modules = self.factory.get_module_description(),
            assumptions=assumptions,
            expected_hashe=self.expected_hashe,
        )

        return object_identity

    def __new__(self,*a,**args): # no need to split this in new and factory, all togather
        self=object.__new__(self)

        # otherwise construct object, test if already there

        if "origins" in args:
            origins=args.pop("origins")
        else:
            origins=[]

        self._da_attributes=dict([(a,b) for a,b in list(args.items()) if a!="assume" and not a.startswith("input") and a!="update" and a!="dynamic" and not a.startswith("use_") and not a.startswith("set_")]) # exclude registered


        update=False
        #update=True
        if 'update' in args:
            log("update in the args:",update)
            update=args['update']

        for a,b in list(args.items()):
            if a.startswith("input"):
                log("input in the constructor:",a,b)
                setattr(self,a,b)
                log("explicite input require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit input:'+repr(a)+" = "+repr(b)

            if a.startswith("use"):
                log("use in the constructor:",a,b)
                setattr(self,a.replace("use_",""),b)
                log("explicite use require non-virtual") # differently!
                self.virtual=False
                self._da_virtual_reason='explicit use:'+repr(a)+" = "+repr(b)

            if a.startswith("set"):
                log("set in the constructor:",a,b)
                setting_name=a.replace("set_","")
                setattr(self,setting_name,b)
                if self._da_settings is None:
                    self._da_settings=[]
                self._da_settings.append(setting_name)
                log('settings:',self,self._da_settings,level='top')
                #log("explicite use require non-virtual") # differently!
                #self.virtual=False
                #self._da_virtual_reason='explicit use:'+repr(a)+" = "+repr(b)

        name=self.get_signature()
        log("requested object",name,"attributes",self._da_attributes)

        if 'dynamic' in args and not args['dynamic']:
            log("not dynamic object:",self,level='dynamic')
            r=self
        else:
            log("dynamic object, from",self,level='dynamic')
            r=AnalysisFactory.get(self,update=update,origins=["from_new_constructor"]+origins)
            log("dynamic object, to",r,level='dynamic')

        if 'assume' in args and args['assume']!=[]:
            log("replacing with a copy: < ",r,level='dynamic')
            r=r.__class__(dynamic=False) # replace with a copy
            log("replacing with a copy: > ",r,level='dynamic')
            r.assumptions=args['assume']
            if not isinstance(r.assumptions,list): # iteratable?
                r.assumptions=[r.assumptions]
            log("assuming with a copy: > ",r,level='dynamic')
            log("requested assumptions:",self.assumptions)
            log("explicite assumptions require non-virtual") # differently!
            #self.virtual=False
            r._da_virtual_reason='assumptions:'+repr(self.assumptions)

        return r

    def promote(self,origins=None):
        if origins is None:
            origins=[]
        log("promoting to the factory",self)
        return AnalysisFactory.put(self,origins=["self_promote"])

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

    def jsonify(self,embed_datafiles=False,verify_jsonifiable=False):
        return self.cache.adopt_datafiles(self.export_data(embed_datafiles,verify_jsonifiable))

    def export_data(self,embed_datafiles=False,verify_jsonifiable=False,include_class_attributes=False,deep_export=False,export_caches=False):
        log("export_data with",embed_datafiles,verify_jsonifiable,include_class_attributes,deep_export)
        empty_copy=self.__class__
        log("my keys:", list(self.__dict__.keys()))
        log("my class is",self.__class__)
        log("class keys:", list(empty_copy.__dict__.keys()))

        updates=set(self.__dict__.keys())-set(empty_copy.__dict__.keys())
        log("new keys:",updates)
        # or state of old keys??

        if include_class_attributes:
            log("using class attributes",list(self.__dict__.keys()))
            updates=list(self.__dict__.keys())+list(empty_copy.__dict__.keys())

        # revise as follows:
        #  - need to be able to retore DataAnalysis references, as currently known
        #  - use behavir more clear

        def qualifies_for_export(a):
            if hasattr(type(self), a) and isinstance(getattr(type(self), a), property): return False
            if isinstance(getattr(self, a),DataFile): return True
            if isinstance(getattr(self, a), AnalysisFactory.__class__): return False
            #if isinstance(getattr(self, a),DataAnalysis): return True #?
            if callable(getattr(self,a)): return False
            if isinstance(getattr(self, a),cache_core.Cache): return False
            if a.startswith("_"): return False
            if a.startswith("set_"): return False
            if a.startswith("use_"): return False
            if a.startswith("input"): return False
            if a.startswith('assumptions'): return False
            if a.startswith('virtual'): return False
            if a.startswith('read_caches'): return False
            if a.startswith('write_caches'): return False
            return True

        if deep_export:
            updates = dir(self)
            log("deep export", [u for u in updates if qualifies_for_export(u)])

        if self.explicit_output is not None:
            log("explicit output requested",self.explicit_output)
            r=dict([[a,getattr(self,a)] for a in self.explicit_output if hasattr(self,a)])
        else:
            r=dict([[a,getattr(self,a)] for a in updates if qualifies_for_export(a)])

        if verify_jsonifiable:
            res=[]
            #log(render("{MAGENTA}verify_jsonifiable of %s : %s with %s items{/}"%(self, r, len(r.items()))),level="top")

            for a,b in list(r.items()):
                res.append([a,jsonify.jsonify(b)])
            r=dict(res)

        for k in dir(self):
            if k == "cache":
                v = getattr(self, k)
                if isinstance(v, cache_core.Cache) and export_caches:
                    log("trying to preserve linked cache", k, v)
                    r[k] = v

            if k.startswith("input"):
                v=getattr(self,k)
                log("trying to preserve linked input", k, v)

                if isinstance(v, str):
                    r['_da_stored_string_' + k] = v
                    continue

                if isinstance(v, DataHandle):
                    r['_da_stored_string_' + k] = v.str()
                    continue

                if isinstance(v, DataAnalysis):
                    m_k='_da_stored_link_' + k
                    log("storing link to",v.get_factory_name(),"as",m_k)
                    r[m_k] = v.get_factory_name() # discarding deep inputs!
                    continue

                if isinstance(v, NamedAnalysis):
                    m_k='_da_stored_link_' + k
                    log("storing link to named",v.analysis_name,"as",m_k)
                    r[m_k] = v.analysis_name# discarding deep inputs!
                    continue

                if not isinstance(v,type):
                    log("WARNING, what is this:"+repr(v)+"; "+repr(type(v)))
                else:
                    if issubclass(v, DataAnalysis):
                        m_k='_da_stored_link_' + k
                        log("storing class link to",v.__name__,"as",m_k)
                        r[m_k] = v.__name__ # discarding deep inputs!


        log("resulting output:",r)
        return r

    def import_data(self,c):
        # todo: check that the object is fresh
        log("updating analysis with data")

        for k, i in list(c.items()):
            log("restoring", k, i)

            if i=="None":
                i=None

            try:
                setattr(self, k, i)
            except Exception as e:
                print("print unable to set attribute:",self,k,i)
                raise

            if k.startswith("_da_stored_string_input"):
                nk=k.replace("_da_stored_string_input","input")
                log("restoring string input",k,nk,i)
                setattr(self,nk,i)

            if k.startswith("_da_stored_link_input"):
                nk=k.replace("_da_stored_link_input","input")
                log("restoring linked input", k, nk, i)
                setattr(self,nk,named(i))


    def serialize(self,embed_datafiles=True,verify_jsonifiable=True,include_class_attributes=True,deep_export=True):
        log("serialize", self, "as", embed_datafiles, verify_jsonifiable, include_class_attributes)
        return self.get_factory_name(), self.export_data(embed_datafiles, verify_jsonifiable, include_class_attributes, deep_export)

    # the difference between signature and version is that version can be avoided in definition and substituted later
    # it can be done differently
    # e.g. versions can be completely avoided


    def get_formatted_attributes(self):
        if hasattr(self,'_da_attributes'): #not like that
            return "_".join(["%s.%s"%(str(a),repr(b).replace("'","")) for a,b in list(self._da_attributes.items())])
        else:
            return ""

    def get_version(self, for_repr=False):
        ss="_".join(["%s:%s"%(a,repr(getattr(self,a))) for a in self._da_settings]) if self._da_settings is not None else ""
        v=self.get_signature()+"."+self.version+("."+ss if ss!="" else "")
        #if hasattr(self,'timestamp'):
        #    v+=".at_"+str(self.timestamp)
        return v

    def get_signature(self):
        a=self.get_formatted_attributes()
        if a!="": a="."+a

        state=self.get_state()
        if state is not None:
            a=a+"."+state


        return self.get_factory_name()+a

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
            for a,b in list(content.items()):
                if isinstance(b,DataFile):
                    dest_unique = b.path+"."+self.cache.hashe2signature(hashe) # will it always?
                    b._da_unique_local_path = dest_unique

                    if os.path.exists(b.path):
                        shutil.copyfile(b.path, dest_unique)
                        log("post-processing DataFile",b,"as",b._da_unique_local_path,log='datafile')
                    else:
                        log("problem post-processing output file", b.path, log='datafile')
                        raise Exception("problem post-processing output file " + b.path)

    def store_cache(self,fih):
        """
        store output 
        """

        log(render("{MAGENTA}object storing in cache{/}: %s, cache %s"%(repr(self), repr(self.cache))),level="top")
        log("hashe:",fih)

        log("transient cache", TransientCacheInstance, "storing", self)
        TransientCacheInstance.store(fih,self)

        log(render("{MAGENTA}first non-transient cache{/}"), self.cache, "storing", self, level="top")
        self.cache.store(fih,self)


    def post_restore(self):
        pass

    _da_cache_retrieve_requests=None

    def retrieve_cache(self,fih,rc=None):
        log(render("{CYAN}object requesting cache{/} for"), repr(self), " requested by "+(" ".join(self._da_requested_by)), level='top')


        if self._da_cache_retrieve_requests is None:
            self._da_cache_retrieve_requests=[]
        self._da_cache_retrieve_requests.append([fih,rc])

        if self._da_locally_complete is not None:
            log("this object has been already restored and complete",self)
            if self._da_locally_complete == fih:
                log("this object has been completed with the neccessary hash: no need to recover state",self)
                if not hasattr(self,'_da_recovered_restore_config'):
                    log("the object has not record of restore config",level='top')
                    return True
                if rc==self._da_recovered_restore_config:
                    log("the object was recovered with the same restore config:",rc,self._da_recovered_restore_config,level='top')
                    return True
                log("the object was recovered with a different restore config:",self._da_recovered_restore_config,'became',rc,level='top')
            else:
                log("state of this object isincompatible with the requested!")
                log(" was: ",self._da_locally_complete)
                log(" now: ",fih)
                #raise Exception("hm, changing analysis dictionary?")
                log("hm, changing analysis dictionary?","{log:thoughts}")

                if self.run_for_hashe:
                    log("object is run_for_hashe, this is probably the reason")

                return None

        if rc is None:
            rc={}

        r=TransientCacheInstance.restore(fih,self,rc)

        if r and r is not None:
            log("restored from transient: this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=fih # info save
            return r

        if self.cached:
            log(render("{MAGENTA}cached, proceeding to restore{/}"), level='top')
        else:
            log(render("{MAGENTA}not cached restore only from transient{/}"))
            return None # only transient!
        # discover through different caches
        #c=MemCacheLocal.find(fih)

        log(render("{CYAN}searching for cache starting from{/}"),self.cache, level='top')
        r=self.cache.restore(fih,self,rc)
        log("cache",self.cache,"returns",r, level='top')

        if r and r is not None:
            log("this object will be considered restored and complete: will not do again",self)
            self._da_locally_complete=fih # info save
            log("locally complete:",fih)
            log("marking restored mode")
            self._da_restored=True
            self.post_restore()
            if self.rename_output_unique and rc['datafile_restore_mode']=="copy":
                try:
                    self.process_output_files(fih)
                except Exception as e:
                    log("unable to restore output files: broken cache record")
                    return None
            else:
                log("disabled self.rename_output_unique",level='cache')

            self.summarize_resource_stats()
            self.process_hooks("top", self, message="restored from cache",
                         #resource_stats=self.current_resource_stats,
                         resource_summmary=self.current_resource_summary,
                         #resource_stats=dict([[rs['resource_type'],rs] for rs in self._da_resource_stats]) if self._da_resource_stats is not None else {},
                         state="node_done")

            return r
        return r # twice

    def get_hashe(self):
        return self.process(output_required=False)[0]


    def get(self,**aa):
        if 'saveonly' in aa and aa['saveonly'] is not None:
            aax=dict(list(aa.items())+[['saveonly',None]])

            try:
                tmp_dir = tempfile.mkdtemp()  # create dir
                log("tempdir:",tmp_dir)
                olddir=os.getcwd()
                os.chdir(tmp_dir)
                self.get(**aax)

                for fn in aa['saveonly']:
                    shutil.copyfile(tmp_dir+"/"+fn,olddir+"/"+fn)

            finally:
                try:
                    shutil.rmtree(tmp_dir)  # delete directory
                except OSError as exc:
                    if exc.errno != errno.ENOENT:  # ENOENT - no such file or directory
                        raise  # re-raise exception

        isolated_directory_key=aa.get('isolated_directory_key',None)
        isolated_directory_cleanup=aa.get('isolated_directory_cleanup',False)

        cwd=os.getcwd()
        if isolated_directory_key is not None:
            wd=cwd+"/"+self.get_factory_name()+"_"+isolated_directory_key
            self._da_isolated_directory=wd
        else:
            wd=cwd
            self._da_isolated_directory=None

        log('isolated directory key:',isolated_directory_key)


        try:
            try:
                os.makedirs(wd)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(wd):
                    pass
                else:
                    raise
            os.chdir(wd)
            log("object will be processed in",wd)
            result=self.process(output_required=True,**aa)[1]
        except:
            os.chdir(cwd)
            raise
        else:
            os.chdir(cwd)

        if isolated_directory_cleanup and self._da_isolated_directory is not None:
            log("isolate cleanup:",self._da_isolated_directory,level='top')
            if self._da_isolated_directory == os.environ.get('HOME'):
                raise Exception("should not clean home!")

            try:
                shutil.rmtree(self._da_isolated_directory, ignore_errors=True)
            except OSError as e:
                log("\033[31m isolate cleanup FAILED:",self._da_isolated_directory,"\033[0m",level='top')
                files = [str(p) for p in Path(".").rglob("*")]
                log("\033[31m found residual files:", files, "\033[0m", level='top')
                raise Exception(repr(e), repr(files))


        return result

    def load(self,**aa):
        fih=self._da_locally_complete
        if fih is None:
            fih=self.process(output_required=False,**aa)[0]

        log("restoring as",fih)
        self.retrieve_cache(fih)
        return self.get(**aa)

    def stamp(self,comment,**aa):
        self.comment=comment
        import time
        self.timestamp=time.time()

        fih=self._da_locally_complete
        if fih is None:
            fih=self.process(output_required=False,**aa)[0]

        log("storing as",fih)
        return self.store_cache(fih)

    def process_checkin_assumptions(self):
        if self.assumptions!=[]:
            log("assumptions checkin for",self)
            log("factory assumptions for run:",AnalysisFactory.cache_assumptions)
            log("object assumptions:",self.assumptions)
            log("non-trivial assumptions require copy of the analysis tree")
            AnalysisFactory.WhatIfCopy("requested by "+repr(self),self.assumptions)
            for a in self.assumptions:
                a.promote()
        else:
            log("no special assumptions")

    def process_checkout_assumptions(self):
        log("assumptions checkout",self)
        if self.assumptions!=[]:
            AnalysisFactory.WhatIfNot()

    def process_restore_rules(self,restore_rules,extra):
        log("suggested restore rules:",restore_rules)
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
        restore_rules=dict(list(restore_rules_default.items())+(list(restore_rules.items()) if restore_rules is not None else []))
        # to simplify input
        for k in list(extra.keys()):
            if k in restore_rules:
                restore_rules[k]=extra[k]

        # always run to process
        restore_rules['substitute_output_required']=restore_rules['output_required']
        if self.run_for_hashe:
            log(render("{BLUE}this analysis has to run for hashe! this will be treated later, requiring output{/}"))
            log(render("{BLUE}original object output required?{/}"+str(restore_rules['output_required'])))
            restore_rules['output_required']=True
            restore_rules['explicit_input_required']=True
            restore_rules['input_runs_if_haveto']=True
            restore_rules['run_if_haveto']=True

        #restore_rules['force_complete_input']=self.force_complete_input

        log("will use restore_rules:",restore_rules)
        return restore_rules

    def process_restore_config(self,restore_config):
        rc=restore_config # this is for data restore modes, passed to cache
        if restore_config is None:
            rc={'datafile_restore_mode':'copy'}

        log('restore_config:',rc)
        return rc


    def process_timespent_interpret(self):
        tspent=self._da_time_spent_in_main
        if tspent<self.min_timespent_tocache and self.cached:
            log(render("{RED}requested to cache fast analysis!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache)))
            if self.allow_timespent_adjustment:
                log(render("{MAGENTA}temporarily disabling caching for this analysis{/}"))
                self.cached=False
            else:
                log("being ignorant about it")

            if tspent<self.min_timespent_tocache_hard and self.cached:
                if self.hard_timespent_checks:
                    estr=render("{RED}requested to cache fast analysis, hard limit reached!{/} {MAGENTA}%.5lg seconds < %.5lg{/}"%(tspent,self.min_timespent_tocache_hard))
                    raise Exception(estr)
                else:
                    log("ignoring hard limit on request")

        if tspent>self.max_timespent_tocache and not self.cached:
            log(render("{BLUE}analysis takes a lot of time but not cached, recommendation is to cache!{/}"),"{log:advice}")

    def treat_input_analysis_exceptions(self,analysis_exceptions):
        return False

    def get_exceptions(self):
        if not hasattr(self,'analysis_exceptions'):
            return []
        return self.analysis_exceptions

    def note_analysis_exception(self,ae):
        if not hasattr(self,'analysis_exceptions'):
            self.analysis_exceptions=[]

        new_note = (self.get_signature(),ae)
        
        note_serial = lambda x:(x[0],str(x[1]))

        if not any([note_serial(new_note) == note_serial(note) for note in self.analysis_exceptions]):
            self.analysis_exceptions.append(new_note)

    watched_analysis=False

    def start_main_watchdog(self):
        log("main watchdog")
        self.report_runtime("starting")
        if not self.watched_analysis: return
        # do it?
        self.cache.report_analysis_state(self,"running")


    def stop_main_watchdog(self):
        self.report_runtime("done")
        if not self.watched_analysis: return
        self.cache.report_analysis_state(self,"done")

    def process_run_main(self):
        self.process_hooks("top",self,message="main starting")

        #self.runtime_update('running')
        if self.abstract:
            raise Exception("attempting to run abstract! :"+repr(self)+" requested by "+(" ".join(self._da_requested_by)))

        dll=self.default_log_level
        self.default_log_level="main"

        log(render("{RED}running main{/} of "+repr(self)),'{log:top}')
        t0=time.time()
        main_log=StringIO()
        main_logstream= printhook.LogStream(main_log, lambda x:True, name="main stream")
        main_logstream.register()
        main_logstream_file= printhook.LogStream("main.log", lambda x:True, name="main stream file")
        main_logstream_file.register()
        log("starting main log stream",main_log,main_logstream,level='logstreams')

        self.start_main_watchdog()

        try:
            mr=self.main() # main!
        except AnalysisException as ae:
            self.note_analysis_exception(ae)
            mr=None
            log(render("{RED}ANALYSIS EXCEPTION IN MAIN{/}: ")+render("{MAGENTA}"+repr(ae)+"{/}"+" in "+repr(self)),level='top')
            #self.process_hooks("top",self,message="analysis exception",exception=repr(ae),state="node_analysis_exception")
        except Exception as ex:
            self.stop_main_watchdog()


            try:
                self.cache.report_exception(self,ex)
                self.report_runtime("failed "+repr(ex))
            except Exception:
                log("unable to report exception!")

            self.process_hooks("top",self,message="unhandled exception",exception=repr(ex),mainlog=main_log.getvalue(),state="node_unhandled_exception")

            raise UnhandledAnalysisException(
                analysis_node=self,
                main_log=main_log.getvalue(),
                exception=ex,
            )
        self.stop_main_watchdog()

        main_logstream.forget()
        main_logstream_file.forget()
        self._da_main_log_content=main_log.getvalue()
        main_log.close()
        log("main log stream collected",len(self._da_main_log_content),level="logstreams")
        log("closing main log stream",main_log,main_logstream,level="logstreams")

        tspent=time.time()-t0
        self._da_time_spent_in_main=tspent
        log(render("{RED}finished main{/} in {MAGENTA}%.5lg seconds{/}"%tspent),'{log:resources}')
        self.report_runtime("done in %g seconds"%tspent)
        self.note_resource_stats({'resource_type':'runtime','seconds':tspent})

        self.default_log_level=dll
        #self.runtime_update("storing")

        if mr is not None:
            log("main returns",mr,"attaching to the object as list")

            if isinstance(mr, collections.Iterable):
                mr=list(mr)
            else:
                mr=[mr]
            for r in mr:
                if isinstance(r,DataAnalysis):
                    log("returned dataanalysis:",r,"assumptions:",r.assumptions)
            setattr(self,'output',mr)

        log(render("node %s main {GREEN}DONE{/}!"%repr(self)),level='top')
 
        self.summarize_resource_stats()

        if self.get_exceptions() == []:
            self.process_hooks("top",self,message="main done",
                            resource_stats=self.current_resource_stats,
                            resource_summary=self.current_resource_summary,
                            state="main_done")
        else:
            self.process_hooks("top",self,message="analysis exception",
                            resource_stats=self.current_resource_stats,
                            resource_summary=self.current_resource_summary,
                            exceptions=repr(self.get_exceptions()),
                            state="node_analysis_exception")

    def process_find_output_objects(self):
        if self._da_ignore_output_objects:
            return []

        da=list(objwalk(self.export_data(),sel=lambda y:isdataanalysis(y)))
        if da!=[]:
            log(render("{BLUE}resulting object exports dataanalysis, should be considered:{/}"),da)
            log(render("{RED}carefull!{/} it will substitute the object!"),da)

            if len(da)==1:
                da=da[0]
        return da

    def process_implement_output_objects(self,output_objects,implemented_objects):
        log("was",output_objects,level='output_objects')
        log("has",implemented_objects,level='output_objects')

        try:
            for newobj,obj in zip(implemented_objects,output_objects):
                log("replace", obj, id(obj), "with", newobj, id(newobj), level='top')

        except TypeError:
            implemented_objects=[implemented_objects]
            output_objects=[output_objects]

        assert len(output_objects) == len(implemented_objects)

        for newobj,obj in zip(implemented_objects,output_objects):
            log("replace",obj,id(obj),"with",newobj,id(newobj),level='top')

            #for key in newobj.export_data().keys(): # or all??
            obj._da_locally_complete = newobj._da_locally_complete
            exported_data=newobj.export_data(include_class_attributes=True, deep_export=True)
            for key in list(exported_data.keys()):  # or all??
                log("key to",obj,"from newobj",newobj,key)
                if hasattr(newobj,key):
                    v=getattr(newobj,key)
                else:
                    v=exported_data[key]
                setattr(obj,key,v)


    def get_delegation(self):
        if self._da_main_delegated is not None:
            return self._da_main_delegated

    def process_list_delegated_inputs(self,input):
        return [] # disabled

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
            log("input is None, it is fine")
            return []

        raise Exception("can not understand input: "+repr(input))

    def process_verify_inputs(self,input):
        # walk input recursively
        if not self.force_complete_input:
            log("loose input evaluation - not forced")
            return

        if isinstance(input,list) or isinstance(input,tuple):
            for input_item in input:
                self.process_verify_inputs(input_item)
            return

        if isinstance(input,DataAnalysis):
            log("will verify:",input)
            if not input._da_locally_complete:
                log("input is not completed! this should not happen!",input)
                raise("input is not completed! this should not happen!")
            return

        if input is None:
            log("input is None, it is fine")
            return

        raise Exception("can not understand input: "+repr(input))

    use_hashe=None

    def process_substitute_hashe(self,fih):
        if self.use_hashe is not None:
            substitute_hashe=self.use_hashe[0]
            hashe_mappings=self.use_hashe[1:]

            for a,b in hashe_mappings:
                log("mapping",a,b,getattr(self,b)._da_expected_full_hashe)
                substitute_hashe= hashtools.hashe_replace_object(substitute_hashe, a, getattr(self, b)._da_expected_full_hashe)

            log("using substitute hashe:",substitute_hashe)
            log("instead of:",fih)
            return substitute_hashe
        else:
            return fih

    report_runtime_destination=None
    runtime_id=None

    def report_runtime(self, message): # separet
        if self.report_runtime_destination is None:
            return

    _da_output_origin=None

    @property
    def expected_hashe(self):
        return self.process(output_required=False)[0]

    @property
    def resource_stats(self):
        return self._da_resource_summary

    def set_callback(self,callback_url):
        if self._da_callbacks is None:
            self._da_callbacks=[]

        if isinstance(callback_url,list):
            for u in callback_url:
                self.set_callback(u)
        else:
            if callback_url not in self._da_callbacks:
                self._da_callbacks.append(callback_url)

    def process_hooks(self,*args,**kwargs):
        for dda_hook in dda_hooks:
            log("running hook", dda_hook, self)
            dda_hook(*args,**kwargs)


    def process(self,process_function=None,restore_rules=None,restore_config=None,requested_by=None,**extra):
        #log("stacked factory assumptions:", self.factory.factory_assumptions_stacked, level="assumptions")
        log(render("{BLUE}PROCESS{/} "+repr(self)))

        if requested_by is None:
            requested_by=['direct']

        log('cache assumptions:',AnalysisFactory.cache_assumptions,'{log:top}')
        log('object assumptions:',self.assumptions,'{log:top}')

        if 'callback_url' in extra:
            log(self, "setting callback from process:",extra['callback_url'])
            self.set_callback(extra['callback_url'])
            log(self, "now callbacks:", self.callbacks)

        restore_config=self.process_restore_config(restore_config)
        restore_rules=self.process_restore_rules(restore_rules,extra)

        if restore_rules['explicit_input_required']:
            self.process_hooks("top", self, message="started object processing",hashe=getattr(self, '_da_expected_full_hashe', "unknown"))

        log(render("{BLUE}requested "+("OUTPUT" if restore_rules['output_required'] else "")+" by{/} "+" ".join(requested_by)),'{log:top}')
        requested_by=[("+" if restore_rules['output_required'] else "-")+self.get_version()]+requested_by

        self._da_requested_by=requested_by

        if hasattr(self,'_da_obscure_origin_hashe') and self._da_obscure_origin_hashe is not None:
            input_hash = self._da_obscure_origin_hashe[1]
            input = []
        else:
            self.process_checkin_assumptions()

            rr= list(dict(list(restore_rules.items()))) + list(dict(output_required=False).items()) # no need to request input results unless see below

            #### process input

            if restore_rules['explicit_input_required']:
                self.process_hooks("top", self, message="treating dependencies",
                         hashe=getattr(self, '_da_expected_full_hashe', "unknown"))

            input_hash,input=self.process_input(obj=None,
                                                process_function=process_function,
                                                restore_rules=update_dict(restore_rules,dict(
                                                                                                output_required=False,
                                                                                                can_delegate=restore_rules['can_delegate_input'])),
                                                requested_by=["input_of"]+requested_by)

            if restore_rules['explicit_input_required']:
                self.process_hooks("top", self, message="dependencies ready",
                         hashe=getattr(self, '_da_expected_full_hashe', "unknown"))

        #### /process input

        if not hasattr(self,'_da_resource_summary'):
            self._da_resource_summary={}

        self._da_resource_summary['process_t0']=time.time()

        log("input hash:",input_hash)
        log("input objects:",input)

        fih=('analysis',input_hash,self.get_version()) # construct hash
        log("full hash:",fih)

        fih=self.process_substitute_hashe(fih)

        self._da_expected_full_hashe=fih

        substitute_object=None

        if restore_rules['output_required']: #
            log("output required, try to GET from cache")
            output_origin=None
            if self.retrieve_cache(fih,restore_config): # will not happen with self.run_for_hashe
                log("cache found and retrieved",id(self),'{log:top}')
                log(fih,level='top')
                output_origin = "cache"
                self._da_output_origin=output_origin

            else:
                log("no cache",'{log:top}')
                log(fih,'{log:top}')

                if hasattr(self,'produce_disabled') and self.produce_disabled:
                    if restore_rules['force_complete']:
                        open("ProduceDisabledException-hash.txt", "wt").write(pprint.pformat(fih) + "\n")

                        ftext=""
                        for factorization in self.factory.factorizations:
                            ftext+=("=" * 80)+"\n"
                            for k,v in list(factorization.items()):
                                ftext += "|" + k + "\n"
                                ftext += ("-" * 80) + "\n"
                                ftext += pprint.pformat(v)+"\n\n"

                        open("ProduceDisabledException-factorizations.txt", "w").write(ftext)

                        raise ProduceDisabledException("not allowed to produce but has to! at "+repr(self)+"; hashe: "+repr(fih))
                    else:
                        self.incomplete=True
                        return fih,self


                if restore_rules['explicit_input_required']:
                    log("exclicite input is available")
                else:
                    log("need to guarantee that explicit input is available")

                    ## if output has to be generated, but explicite input was not prepared, do it
                    ## process
                    return self.process(process_function=process_function,
                                        restore_rules=update_dict(restore_rules,dict(explicit_input_required=True)),requested_by=['output_required_by_parent']+requested_by )
                                        #restore_rules=update_dict(restore_rules,dict(output_required=True,explicit_input_required=True)) )
                    ##  /process


                if restore_rules['can_delegate'] and self.cached:
                    log("will delegate this analysis")
                    hashekey=self.cache.register_delegation(self,fih)
                    self._da_main_delegated=hashekey
                    return fih,self # RETURN!

             # check if can and inpust  relaxe

                self.process_verify_inputs(input)

                # check if input had exceptions
                analysis_exceptions=self.process_list_analysis_exceptions(input)
                if analysis_exceptions!=[]:
                    log("found analysis exceptions in the input:",analysis_exceptions)
                    if not self.treat_input_analysis_exceptions(analysis_exceptions):
                        if not hasattr(self,'analysis_exceptions'):
                            self.analysis_exceptions=[]
                        self.analysis_exceptions+=analysis_exceptions
                        log(render("{RED}ANALYSIS EXCEPTIONS:{/}"),analysis_exceptions,level='top')
                        self.process_hooks("top",self,message="analysis exception",exception=repr(analysis_exceptions),state="node_analysis_exception")
                    else:
                        analysis_exceptions=[]
                # exceptions

                if analysis_exceptions==[]:
                    if restore_rules['run_if_can_not_delegate']:
                        log("no way was able to delegate, but all ready to run and allowed. will run")
                    else:
                        log("not allowed to run here. hopefully will run as part of higher-level delegation")
                        raise Exception("not allowed to run but has to (delegation)!")
                        #return fih, self # RETURN!

                    if restore_rules['run_if_haveto'] or self.run_for_hashe:

                        mr=self.process_run_main() # MAIN!

                        # failing should lead to checking cache

                        self.process_timespent_interpret()
                        output_origin="main"
                        self._da_output_origin=output_origin
                    else:
                        raise Exception("not allowed to run but has to! at "+repr(self))


                    #log("new output:",self.export_data())

                    if self.rename_output_unique and restore_config['datafile_restore_mode']=="copy":
                        self.process_output_files(fih)
                    else:
                        log("disabled self.rename_output_unique",level='cache')

                    #log("object storing in the cache",level='top')
                    self.store_cache(fih)
                    #self.runtime_update("done")
                else:
                    log("object input had untreated exceptions, storing exception in the cache",level='top')
                    self.store_cache(fih)

                    #raise AnalysisException.from_list(analysis_exceptions)


            output_objects=self.process_find_output_objects()
            if output_objects!=[]:
                da=output_objects
                if self.cached:
                    log(render("{RED}can not be cached - can not save non-virtual objects! (at the moment){/}"),da)
                    self.cached=False

                restore_rules_for_substitute=update_dict(restore_rules,dict(explicit_input_required=restore_rules['substitute_output_required']))
                self.force_complete_input=restore_rules['force_complete'] # ?.. !!!!
                log(render("{RED}will process substitute object as input with the following rules:{/}"),restore_rules_for_substitute)

                rh,ro=self.process_input(da,restore_rules=restore_rules_for_substitute,requested_by=['output_of']+requested_by)
                log(render("substitute the object with dynamic input. rh:"),rh)
                log(render("substitute the object with dynamic input. ro:"),ro,ro.__class__)
                log("output object was",da,da.__class__)

                if not isinstance(ro,list) and not isinstance(ro,tuple):
                    ros=[ro]
                else:
                    ros=ro
                
                if not isinstance(da,list) and not isinstance(da,tuple):
                    das=[da]
                else:
                    das=da

                for _output_object,_substitute_object in zip(das,ros):
                    log("output object",_output_object,"cache",_output_object.cache,"substitute object",_substitute_object,"cache",_substitute_object.cache,level="generative")


                log("--- old input hash:",fih)
                if self.allow_alias:
                    self.register_alias(fih,rh)
                    self.process_implement_output_objects(output_objects,ro)
                else:
                    log("alias is not allowed: using full input hash!")
                    fih=rh
                    substitute_object=ro
                    log("+++ new input hash:",fih)

            log("processing finished, main, object is locally complete")
            log("locally complete:",id(self),"from",output_origin)
            log("locally complete:",fih,'{log:top}')
            self._da_locally_complete=fih
        else:
            log("NO output is strictly required, will not attempt to get")
            if restore_rules['restore_complete']:
                log("however, diagnostic complete restore is requested, trying to restore")
                if self.retrieve_cache(fih,restore_config):
                    log("cache found and retrieved",'{log:top}')
                    log("processing finished, object is locally complete")
                    self._da_locally_complete=fih
                    log("locally complete:",fih,'{log:top}')
                else:
                    log("NO cache found",'{log:top}')

        self.process_checkout_assumptions()

        self._da_resource_summary['process_tspent']=time.time()-self._da_resource_summary['process_t0']
        log(render("{MAGENTA}process took in total{/}"),self._da_resource_summary['process_tspent'])
        self.note_resource_stats({'resource_type':'usertime','seconds':self._da_resource_summary['process_tspent']})
        self.summarize_resource_stats()
        
        #self.process_hooks("top",self,message="processing over",resource_stats=self._da_resource_summary,hashe=getattr(self,'_da_expected_full_hashe',"unknown"))

        return_object=self
        if substitute_object is not None:
            log("returning substituted object",substitute_object)
            return_object=substitute_object

        log("PROCESS done",fih,return_object)
        return fih,return_object
                
    def raise_stored_exceptions(self):
        exceptions=self.get_exceptions()

        if exceptions!=[]:
            log('found exceptions in',self,':',exceptions,level='top')
            to_raise=AnalysisException.from_list(exceptions)
            log('will raise',to_raise,level='top')
            raise to_raise

        log('no exceptions found in state',level='top')

    def register_alias(self,hash1,hash2):
        log("alias:",hash1)
        log("stands for",hash2)
        self.alias=hash2
        AnalysisFactory.register_alias(hash1,hash2)

    _da_callbacks=None

    @property
    def callbacks(self):
        if self._da_callbacks is None:
            return []
        else:
            log("requested callbacks property", self._da_callbacks, level="callback")
            return self._da_callbacks

    def prepare_restore_config(self,restore_config):
        if restore_config is None:
            restore_config={}

        if self.copy_cached_input:
            #log("will copy cached input")
            restore_config['datafile_restore_mode']="copy"
        else:
            #log("will NOT copy cached input")
            restore_config['datafile_restore_mode']="url_in_object"

        if self.test_files: # may be not boolean
            restore_config['test_files']=True
        else:
            restore_config['test_files']=False

        return restore_config

    def prepare_restore_rules(self,restore_rules, extra):
        restore_rules_default = dict(explicit_input_required=False, restore_complete=False)
        restore_rules = dict(
            list(restore_rules_default.items()) + list(restore_rules.items()) if restore_rules is not None else [])

        # to simplify input
        for k in list(extra.keys()):
            if k in restore_rules:
                restore_rules[k] = extra[k]

        if self.force_complete_input:
            restore_rules['force_complete']=True
        else:
            restore_rules['force_complete']=False
            log("input will not be forced!")

        return restore_rules

    def list_inputs(self):
        hashe, inputs_dda = self.process_input()
        log("\n\ncalling REQUIRES for", self, "\n\n")
        log(inputs_dda)

        if inputs_dda is None:
            inputs_dda = []

        if not isinstance(inputs_dda, list):
            inputs_dda = [inputs_dda]

        return inputs_dda

    def guess_main_resources(self):
        return dict(guess_source='mock')

    def process_input(self,obj=None, restore_rules=None,restore_config=None,requested_by=None, **extra):
        """
        walk over all input
        """

        log("{CYAN}PROCESS INPUT{/}")

        restore_config=self.prepare_restore_config(restore_config)
        restore_rules=self.prepare_restore_rules(restore_rules,extra)

        log("input restore_rules:",restore_rules)
        log("input restore_config:",restore_config)

        if obj is None:
            # start from the object dir, look for input
            inputhashes=[]
            inputs=[]
            delegated=[]
            for a in dir(self):
                if a.startswith("input"):
                    o=getattr(self,a)
                    log("input item",a,o)
                    if o is NoAnalysis:
                        log("NoAnalysis:",o)
                        continue

                    if o is None:
                        raise Exception("input is None: virtual class: "+repr(self)+" input "+a+" requested by "+" ".join(requested_by))

                    try:
                        h,l=self.process_input(obj=o,restore_rules=restore_rules,restore_config=restore_config,requested_by=requested_by)
                    except AnalysisDelegatedException as delegated_exception:
                        delegated.append(delegated_exception)
                        continue

                    if not isinstance(l,list) and l.is_noanalysis():
                        log("NoAnalysis:",o,o.__class__)
                        continue

                    # results are put instead
                    if l is not None:
                        log("input item",a)
                        log("implemented as",h,l)
                        setattr(self,a,l)
                    else:
                        log("processed input item None!",a,l)
                        raise Exception("?")

                    inputhashes.append(h)
                    inputs.append(l)

            if delegated != []:
                log("delegated:", len(delegated), repr_short(delegated),level="top")
                log("still implemented:", len(inputhashes),inputs)

                raise AnalysisDelegatedException.from_list(delegated)

            if len(inputhashes)>1:
                return ('list',)+tuple(inputhashes),inputs

            if len(inputhashes)==1:
                return inputhashes[0],inputs[0]

            log("this class has no input! origin class")
            # does this ever happen?
            return None,None
        else:
            # process given input structure
            log("parse and get",obj,obj.__class__)

            # list or tuple
            if isinstance(obj,list) or isinstance(obj,tuple):
                log("parse list")
                hashes=[]
                nitems=[]
                delegated=[]
                for i in obj:
                    log("item:",i)
                    try:
                        hi,ni=self.process_input(obj=i,restore_rules=restore_rules,restore_config=restore_config,requested_by=requested_by)
                    except AnalysisDelegatedException as delegated_exception:
                        delegated.append(delegated_exception)
                        continue

                    hashes.append(hi)
                    nitems.append(ni)

                if delegated!=[]:
                    log("delegated:", len(delegated), (lambda x:x[:100]+" ... "+x[-100:])(repr(delegated)))
                    raise AnalysisDelegatedException.from_list(delegated)
                else:
                    log("no delegated analysis found")

                if all([i is None for i in nitems]):
                    return tuple(['list']+hashes),None

                if any([i is None for i in nitems]):
                    raise Exception("process input returned None for a fraction of a structured input! this should not happen")

                return tuple(['list']+hashes),nitems

       # we are down to the input item finally

        item=self.interpret_item(obj)  # this makes DataAnalysis object
        #if hasattr(item,'noanalysis') and item.noanalysis:
        #    log("noanalysis!")
        if item.is_noanalysis():
            log("noanalysis!")
            return None,item

        rr=dict(list(restore_rules.items())+list(dict(explicit_input_required=False,output_required=restore_rules['explicit_input_required']).items()))
        if self.run_for_hashe:
            log("for run_for_hashe, input need a right to run")
            restore_rules['run_if_haveto']=True


        log("proceeding to run",item,"rules",restore_rules)

        try:
            log("item:",item._da_locally_complete)
        except Exception as e:
            raise Exception(str(item)+" has no locally complete!")
        input_hash,newitem = item.process(
                                restore_rules=rr,
                                restore_config=restore_config,
                                requested_by=requested_by, 
                                callback_url=self.callbacks
                            ) # recursively for all inputs process input
        log("process_input finishing at the end",input_hash,newitem)

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
        log('note resource stats:',info['resource_type'],'{log:resources}')
        self._da_resource_stats.append(info)

    @property
    def current_resource_stats(self):
        if self._da_resource_stats is None:
            return {}
        return self._da_resource_stats 

    @property
    def current_resource_summary(self):
        if self._da_resource_summary is None:
            return {}
        return self._da_resource_summary

    def summarize_resource_stats(self):
        total_usertime=sum([a['seconds'] for a in self.current_resource_stats if a['resource_type']=='usertime'])
        log(render("collected resource stats, total {MAGENTA}usertime{/}"),total_usertime,'{log:resources}')

        total_runtime=sum([a['seconds'] for a in self.current_resource_stats if a['resource_type']=='runtime'])
        log(render("collected resource stats, total {MAGENTA}run time{/}"),total_runtime,'{log:resources}')

        total_cachetime=sum([a['stats']['copytime'] for a in self.current_resource_stats if a['resource_type']=='cache'])
        log(render("collected resource stats, total {MAGENTA}cache copy time{/}"),total_cachetime,'{log:resources}')

        main_exectured_on=dict(
            hostname=socket.gethostname(),
            fqhname=socket.getfqdn(),
            pid=os.getpid(),
            thread_id=threading.current_thread().ident,
            requested_by=self._da_requested_by,
        )

        if not hasattr(self,'_da_resource_summary'):
            self._da_resource_summary={}

        self._da_resource_summary.update({
                                'total_usertime':total_usertime,
                                'total_runtime':total_runtime,
                                'total_cachetime':total_cachetime,
                                'main_executed_on':main_exectured_on,
                            })


    def __call__(self):
        return self

    def __repr__(self):
        return "[%s%s]"%(self.get_version(),";NoneInTheInput" if self.virtual else "")

    @classmethod
    def from_data(cls,name,data,**kwargs):
        c = type(cls)
        newcls = c(name, (cls,), dict(cls.__dict__))()

        for k,v in list(data.items()):
            setattr(newcls,k,v)

        extradata={}
        for arg_k,arg_v in list(kwargs.items()):
            if arg_k.startswith("input_"):
                setattr(newcls,arg_k,arg_v)
            elif arg_k.startswith("use_"):
                setattr(newcls, arg_k[len("use_"):], arg_v)
                extradata[arg_k]=arg_v
            else:
                raise RuntimeError("unrecognized arguement: "+arg_k)

        newcls.version=hashtools.shhash(tuple(map(tuple,sorted(list(data.items())+list(extradata.items())))))[:8]

        obj=newcls()

        return obj

    @classmethod
    def from_data(cls,name,data,**kwargs):
        c = type(cls)
        newcls = c(name, (cls,), dict(cls.__dict__))()

        for k,v in list(data.items()):
            setattr(newcls,k,v)

        extradata={}
        for arg_k,arg_v in list(kwargs.items()):
            if arg_k.startswith("input_"):
                setattr(newcls,arg_k,arg_v)
            elif arg_k.startswith("use_"):
                setattr(newcls, arg_k[len("use_"):], arg_v)
                extradata[arg_k]=arg_v
            else:
                raise RuntimeError("unrecognized arguement: "+arg_k)

        newcls.version=hashtools.shhash(tuple(map(tuple,sorted(list(data.items())+list(extradata.items())))))[:8]

        obj=newcls()

        return obj


    @classmethod
    def from_hashe_and_data(cls, hashe, data=None, cached=False):
        name,version=hashe[-1].split(".",1)
        log("object name:",name)
        log("object version:", version)


        c = type(cls)
        newcls = c(name, (cls,), dict(cls.__dict__))()

        if data is not None:
            for k,v in list(data.items()):
                setattr(newcls,k,v)

        newcls.version=version

        obj=newcls()
        obj._da_obscure_origin_hashe = hashe
        obj.produce_disabled = True
        obj.cached=cached

        if data is not None:
            #obj._da_locally_complete = hashe
            log("storing obscure to the TransientCache:")
            TransientCacheInstance.store(hashe, obj)

        return obj

    @classmethod
    def from_hashe(cls, hashe, cached=True):
        return cls.from_hashe_and_data(hashe, data=None, cached=cached)

class NamedAnalysis(object):
    def __init__(self,name):
        self.analysis_name=name

    def resolve(self):
        return AnalysisFactory.byname(self.analysis_name)

class FileHashed(DataAnalysis):
    input_filename=None

    cached=False # never
    infactory=False
    run_for_hashe=True


    def main(self): # pointless unless fine has known hashe!
        self.md5= hashtools.hash_for_file(open(self.input_filename.handle))
        return DataHandle(self.input_filename.handle+":md5:"+self.md5[:8])

    def get_filename(self):
        return self.input_filename.str()

class HashedFile(DataAnalysis):
    filename=None
    md5=None

    cached=False # never
    infactory=False

    def get_signature(self):
        if self.filename is not None:
            return "File:"+os.path.basename(self.filename)+":"+self.md5[:5]
        else:
            return "File: None"

class HasheForFile(DataAnalysis):
    input_filename=None

    cached=False # never
    infactory=False
    run_for_hashe=True


    def main(self):
        md5= hashtools.hash_for_file(open(self.input_filename.str()))
        return HashedFile(use_md5=md5,use_filename=self.input_filename.str())

class DataHandle(DataAnalysis):
    infactory=False
    trivial = True

    def __new__(self,*a,**args): # not only kw
        return object.__new__(self)

    def __init__(self,h=None,update=False):
        self.handle=h

    def process(self,*a,**args):
        assert len(a)==0, "unexpected arguements:"+repr(a)
        log("datahandle is hash",self)
        self._da_locally_complete=True
        return self.handle,self

    def __repr__(self):
        return '[%s]'%self.handle


# imported

# abstract


class DataFile(DataAnalysis):
    cached_path_valid_url=False

    infactory=False

    size=None

    trivial=True

    def __init__(self,fn=None,update=False):
        self.path=fn
        self.size=os.path.getsize(fn)

    def get_cached_path(self): # not work properly if many run!
        return self.cached_path if hasattr(self,'cached_path') else self.path

    def get_path(self): # not work properly if many run!
# confis
        #log("get path:",self,self.cached_path,self.cached_path_valid_url) #,self.restored_mode)

        if hasattr(self,'cached_path'):
            log("have cached path",self.cached_path)

        if hasattr(self,'cached_path') and self.cached_path_valid_url:
            return self.cached_path

        if hasattr(self,'_da_unique_local_path'):
            return self._da_unique_local_path

        if not hasattr(self,'restored_mode'): # means it was just produced
            return self.path

        if self.restored_mode=="copy":
            log("datafile copied but no local path?",self,id(self))
            raise Exception("inconsistency: "+repr(self))
            
    def get_full_path(self):
        if hasattr(self,'restored_path_prefix'):
            return self.restored_path_prefix+"/"+self.get_path()
        else:
            if hasattr(self,'cached_path') and self.cached_path_valid_url:
                return self.cached_path
            else:
                raise Exception("unable to construct full path!")

    def open(self, f="r"):
        if hasattr(self,'cached_path'):
            if os.path.exists(self.cached_path):
                return gzip.open(self.cached_path)
            else:
                m = f"\033[31mERROR\033[0m: object {self} has cached path: {self.cached_path} but it does not exist\n"
                m += f"\033[31mERROR\033[0m: cwd {os.getcwd()}\n"
                m += f"\033[31mERROR\033[0m: have here {glob.glob('*')}"
                log(m, level="top")
                raise RuntimeError(m)
        else:
            return open(self.path, f)

    def __repr__(self):
        return "[DataFile:%s]"%(self.path if hasattr(self,'path') else 'undefined')
                        

    def jsonify(self,*a,**aa):
        assert len(a) == 0, "unexpected:" + repr(a)

        if self.size<100e3:
            try:
                return json.load(self.open("rt"))
            except:    
                try:
                    content=self.open("rt").read()
                except UnicodeDecodeError:
                    content=self.open("rb").read().decode('utf-8', 'ignore') #??

            try:
                from astropy.io import fits
                return jsonify.jsonify_fits(fits.open())
            except Exception as e:
                log("can not interpret as fits:",e)
            
            try:
                json.dumps(content) # just to try
                return content
            except Exception as e:
                return str(self)+" can not encode to json "+str(self.size)+" fits error "+repr(e)
        else:
            return str(self)+" too big "+str(self.size)

    @classmethod
    def from_object(cls, key, obj, optional=True):
        if isinstance(key,str):
            name = key
        else:
            name = re.sub("[^a-zA-Z0-9\-]", "_", "_".join(map(str, key)))

        import numpy as np
        if isinstance(obj,np.ndarray):
            if len(obj)>50:
                log("adoption as numpy:", len(obj), obj)
                fn=name+"_numpy_array.txt"
                np.savetxt(fn,obj)
                r=cls(fn)
                r.adopted_format="numpy"
                r.pre_adoption_key = key
                return r
            else:
                log("too small for adoption:", len(obj), obj)

        import pandas as pd
        if isinstance(obj,pd.DataFrame):
            if len(obj)>50:
                log("adoption as pandas:", len(obj), obj)
                fn = name + "_pandas_dataframe.csv"
                obj.to_csv(fn)
                r=cls(fn)
                r.adopted_format = "pandas"
                r.pre_adoption_key = key
                return r
            else:
                log("too small for adoption:",len(obj),obj)

        log("not good for adoption:", obj)

        return obj

    def restore_adoption(self):
        if not hasattr(self,'adopted_format'):
            raise Exception("requested to restore adoption when no format was saved!")

        if self.adopted_format == "numpy":
            import numpy as np
            return np.loadtxt(self.open())
        elif self.adopted_format == "pandas":
            import pandas as pd
            return pd.read_csv(self.open())
        else:
            raise Exception("unknown adopted format:",self.adopted_format)




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

def reset():
    AnalysisFactory.reset()
    TransientCacheInstance.reset()


def debug_output():
    printhook.global_all_output=True
    printhook.global_permissive_output=True
    printhook.global_fancy_output=False
    printhook.LogStreams=[printhook.LogStream(sys.stdout,levels=None,name="original stdout set in debug")]

AnalysisFactory.blueprint_class=DataAnalysis
AnalysisFactory.blueprint_DataHandle=DataHandle
AnalysisFactory.named_blueprint_class=NamedAnalysis

byname = lambda x: AnalysisFactory.byname(x)

def get_object(a):
    return AnalysisFactory[a]


def get_schema(self,graph=None,write_png_fn=None):
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

    # do it all from hash, no need to construct again

    if write_png_fn is not None:
        self.get_schema().write_png(write_png_fn)

    return graph
