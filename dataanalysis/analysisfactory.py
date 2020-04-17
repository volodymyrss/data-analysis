import sys

from dataanalysis import printhook, hashtools
from dataanalysis.bcolors import render
from dataanalysis.printhook import log, decorate_method_log, debug_print

#from dataanalysis import core
#print(core.__file__)

#from dataanalysis.core import DataAnalysis, DataFile, DataHandle

class decorate_factory(type):
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
        return c

class AnalysisFactoryClass(metaclass=decorate_factory):  # how to unify this with caches?..
    # handles all object equivalence
    cache = {}
    dda_modules_used = []

    factorizations= []

    def __repr__(self):
        return "[AnalysisFactory: %i]" % len(self.cache)

    def reset(self):
        self.comment=""
        self.cache = {}
        self.cache_assumptions=[]
        self.cache_stack=[]
        self.factorizations=[]
        self.dda_modules_used=[]

    def note_factorization(self,factorization):
        self.factorizations.append(factorization)

    def implement_serialization(self,serialization):
        name, data = serialization
        obj = self[name]
        obj.import_data(data)
        obj.infactory = True
        obj.virtual = True
        return obj

    def inject_serialization(self, serialization):
        log("injecting", serialization)
        obj=self.implement_serialization(serialization)
        self.put(obj,origins=["inject_serialization"])
        log("result of injection",self.byname(obj.get_signature()))

    def assume_serialization(self, serialization):
        log("assuming injection", serialization)
        obj = self.implement_serialization(serialization)
        self.WhatIfCopy("injection_of_" + repr(obj),obj)
        log("result of injection", self.byname(obj.get_signature()))

    def assume_module_used(self,module,sanitize=True):
        if sanitize and module.__name__.startswith("dataanalysis."):
            return # really??

        if self.dda_modules_used == [] or self.dda_modules_used[-1] != module:
            self.dda_modules_used.append(module)

        while True:
            cleaned=hashtools.remove_repeating_stacks(self.dda_modules_used)
            if cleaned==self.dda_modules_used:
                self.dda_modules_used=cleaned
                break
            self.dda_modules_used = cleaned


        #self.dda_modules_used()
        #remove_repeating_stacks()

    def put(self, obj, sig=None, origins=None):
        log("requested to put in factory:", obj, sig)
        log(obj,"origins:",origins)
        log("factory assumptions:", self.cache_assumptions)

        if not obj.infactory:
            log("object is not in-factory, not putting")
            return obj

        if obj.assumptions != []:
            log("object has assumptions:", obj, obj.assumptions)
            raise Exception("can not store in cache object with assumptions")

        module_record = sys.modules[obj.__module__]

        if origins is not None and "with_metaclass" in origins:
            log("assume module",module_record,"triggered by",obj)
            self.assume_module_used(module_record)
        else:
            log("origins",origins,"not suitable to assume module", module_record, "triggered by", obj)

        if isinstance(obj, type):
            log("requested to put class, it will be constructed")
            obj = obj()

        sig = obj.get_signature() if sig is None else sig  # brutal
        log("put object:", obj, "signature", sig)
        saved = None
        if sig in self.cache:
            saved = self.cache[sig]
        self.cache[sig] = obj
        return saved

    def get(self, item, update=False, origins=None):
        """
        generates and instance of DataAnalysis from something

        """

        if origins is None:
            origins=[]

        log("interpreting", item)
        log("on get factory knows", self.cache)  # .keys())

        if item is None:
            log("item is None: is it a virtual class? should not be in the analysis!")
            raise Exception("virtual class, class with None inputs, is not allowed directly in the analysis")

        if not hasattr(self,'blueprint_class'):
            log("no analysis yet")
            return item

        if hasattr(self,'named_blueprint_class') and isinstance(item,self.named_blueprint_class):
            item=item.resolve()

        if isinstance(item, type) and issubclass(item, self.blueprint_class):
            log("is subclass of DataAnalysis, probably need to construct it")
            name = item.__name__
            log("class name:", name)
            if name in self.cache:
                c = self.cache[name]
                log("have cache for this name:", c)
                if isinstance(item, type):
                    log("it is class, constructing")
                    c = c()
                    log("constructed", c)
                    self.put(c,origins=["get","from_class"]+origins)
                    # log("will store",c)
                    return c
                if isinstance(item, self.blueprint_class):
                    log("cache returns object, will use it:", c)
                    return c
            else:
                log("there is no such class registered!", name)
                raise Exception("there is no such class registered: " + name + " !")

        if isinstance(item, self.blueprint_class):
            log("is instance of DataAnalysis, signature", item.get_signature())

            if item.trivial:
                log("trivial class: is datahandle or file, returning", item)
                return item
            #if isinstance(item, DataHandle) or isinstance(item, DataFile):  # make as object call
            #    log("is datahandle or file, returning", item)
            #    return item

            if not item.infactory:  # make as object call
                log("is not in-factory, returning")
                return item

            s = item.get_signature()
            if s in self.cache:
                storeditem = self.cache[item.get_signature()]

                if isinstance(storeditem, type):
                    raise Exception("no classes can be stored!")

                # if item!=storeditem: # object was constructed during the declaration of the class or interpreted earlier, either way the ___new__ call had to guarantee it is the same. it has to be the same
                #    raise Exception("critical violation of sanity check! object was constructed twice "+str(item)+" vs "+str(storeditem))

                log("so, offered object:", item)
                log("     stored object:", storeditem)

                if not item.is_virtual():  # careful!
                    log("     offered object is non-virtual, simply returning")
                    log("     offered object complettion:", item._da_locally_complete)
                    log("     offered virtual reason:",
                           item._da_virtual_reason if hasattr(item, '_da_virtual_reason') else "")
                    # log("     offered object is non-virtual, forcing it")
                    # update=True #!!!!
                    ## think about this! update??
                    return item

                if len(item.assumptions) != 0:
                    log("     offered object has assumptions", item.assumptions)
                    log("     offered object complettion:", item._da_locally_complete)
                    log("     will copy and assign assumptions")

                    copied_storeditem = storeditem.__class__(dynamic=False)
                    copied_storeditem.assumptions = item.assumptions

                    return copied_storeditem

                if update:
                    log("recommendation is to force update")
                    self.put(item,origins=["get","force_update"]+origins)
                    return item
                else:
                    log("attention! offered object is discarded:", item)  # ,item.export_data())
                    log("stored in factory (this will be the valid instance):",
                           storeditem)  # ,storeditem.export_data())
                    return storeditem

            log("no such object registered! registering")  # attention!
            self.put(item,origins=["get","from_new_object"]+origins)
            return item

        if isinstance(item, str):
            log("considering string data handle")
            return self.blueprint_DataHandle(item)

        if isinstance(item, str):
            log("considering unicode string data handle")
            return self.blueprint_DataHandle(str(item))

        log("unable to interpret item: " + repr(item))
        log("after get factory knows",AnalysisFactory.cache)
        raise Exception("unable to interpret item: " + repr(item)+" blueprint: "+repr(self.blueprint_class))
        # return None

    cache_stack = []
    cache_assumptions = []
    comment = ""

    @property
    def factory_assumptions_stacked(self):
        r=[]
        for a in self.cache_assumptions:
            r+=a
        log("stacked cache assumptions",r,level="top")
        return r

    def WhatIfCopy(self, description, new_assumptions):
        if isinstance(new_assumptions, tuple):
            new_assumptions = list(new_assumptions)
        if not isinstance(new_assumptions, list):
            new_assumptions = [new_assumptions]

        log(render("{RED}go deeper! what if?{/} stack of size %i" % len(self.cache_stack)))
        if self.comment == description:
            log("will not push copy, already in the assumption", description)
            return

        log("pushing into stack current version, using copy", description)
        self.comment = description
        self.cache_stack.append(self.cache)
        self.cache_assumptions.append(new_assumptions)
        self.cache = {}  # this makes assumptions reset
        log("cache stack of size", len(self.cache_stack))
        log("cache stack last entry:", self.cache_stack[-1])

        for i, o in list(self.cache_stack[-1].items()):
            log("promoting", i, 'assumptions', o.assumptions)
            # if o.is_virtual():
            #    log("virtual object, constructing empty copy")
            #log('whatifcopy for',description,'will serialize',i,o,level='top')
            serialization=o.serialize(verify_jsonifiable=False)
            if hasattr(o, '_da_obscure_origin_hashe'):
                serialization[1]['_da_obscure_origin_hashe']=o._da_obscure_origin_hashe

            o.__class__(dynamic=False).promote(origins=["what_if_copy"])  # assume??
            self.inject_serialization(serialization) # conditional reality contains shallow copies of the fundamentals

        for assumptions in self.cache_assumptions:
            log("assumption group:", assumptions)
            for a in assumptions:
                log("assumption", a)
                #a.promote()
                serialization = a.export_data(verify_jsonifiable=False,include_class_attributes=True,deep_export=True,export_caches=True)
                log("export for cloning:",serialization)
                obj=a.__class__(dynamic=False)
                obj.import_data(serialization)
                obj.promote()

                #   else:
                #       log("non-virtual object! promoting object itself")
                #       o.promote() # assume??
        # self.cache=copy.deepcopy(self.cache_stack[-1]) # dangerous copy: copying call __new__, takes them from current cache
        log("current cache copied, factory knows:", self.cache)

    def WhatIfNot(self):
        log("before whatifnot factory knows", self.cache)  # .keys())
        if self.cache_stack == []:
            log("empty stack!")
            return
            # raise Exception("empty stack!")

        log("poping from stack last version")
        self.cache = self.cache_stack.pop()
        assumptions = self.cache_assumptions.pop()
        log("discarding last stacked", self.comment)
        log("discarding:", assumptions)
        log("current assumptions level %i:" % len(self.cache_assumptions),
               self.cache_assumptions[-1] if self.cache_assumptions != [] else "none")
        self.comment = ""
        log("after whatifnot factory knows", self.cache)  # .keys())
        log("complete cache stack:")
        for i,(assumption,cache) in enumerate(zip(self.cache_assumptions,self.cache_stack)):
            log(i,assumption,cache)

    def byname(self, name):
        if name not in self.cache:
            raise Exception("name is not known, can not get this: " + repr(name) + "\n\n" +
                            "available names:\n" +
                            "; ".join(map(repr,list(self.cache.keys())))
                            )
        return self.cache[name]

    def get_by_name(self, name):
        return self.byname(name)

    def __getitem__(self, name):
        return self.byname(name)

    def __iter__(self):
        for i in list(self.cache.keys()):
            yield i

    aliases = []

    def register_alias(self, h1, h2):
        if self.aliases is None: self.aliases = []
        self.aliases.append((h1, h2))

    definitions = []

    def register_definition(self, c, h):
        self.definitions.append([c, h])

    def list_relevant_aliases(self, obj):
        h0 = obj.get_version()

        def contains(graph, key):
            if graph == key: return True
            if isinstance(graph, tuple):
                if graph[0] == 'analysis':
                    if graph[2] == key: return True
                    return contains(graph[1], key)
                if graph[0] == 'list':
                    return any([contains(k, key) for k in graph[1:]])

        # log('aliases:',self.aliases)
        return self.aliases

    def get_definitions(self):
        return self.definitions

        #   return [[a,b] for a,b in self.aliases if contains(h0,a)]

    def format_module_description(self,modules):
        module_description = []
        for m in modules:
            if m.startswith("git://"):
                name=m[len("git://"):]
                module_description.append(["git", name, None])
            else:
                module_description.append(['filesystem', m, None])

        return module_description

    def get_module_description(self):
        module_description = []
        for m in AnalysisFactory.dda_modules_used:
            log("module", m)
            if hasattr(m, "__dda_module_global_name__"):
                log("dda module global name", m.__dda_module_global_name__)
                module_origin='cache'
                if hasattr(m, "__dda_module_origin__"):
                   module_origin=m.__dda_module_origin__
                module_description.append([module_origin, m.__name__, m.__dda_module_global_name__])
            else:
                if hasattr(m,'__file__'):
                    module_description.append(['filesystem', m.__name__, m.__file__])
                else:
                    module_description.append(['filesystem', m.__name__, None])

        return module_description


AnalysisFactory = AnalysisFactoryClass()
