import sys
from dataanalysis.bcolors import render

from dataanalysis import printhook
from dataanalysis.printhook import cprint,decorate_method_log,debug_print

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

class AnalysisFactoryClass:  # how to unify this with caches?..
    # handles all object equivalence
    __metaclass__ = decorate_factory
    # dictionary of current object names, aliases?..
    cache = {}
    dda_modules_used = []

    def __repr__(self):
        return "[AnalysisFactory: %i]" % len(self.cache)

    def reset(self):
        self.cache = {}

    def inject_serialization(self, serialization):
        print("injecting", serialization)
        name, data = serialization
        obj = self[name]
        obj.import_data(data)
        obj.infactory = True
        obj.virtual = True
        self.put(obj)

    def put(self, obj, sig=None):
        cprint("requested to put in factory:", obj, sig)
        cprint("factory assumptions:", self.cache_assumptions)

        if not obj.infactory:
            cprint("object is not in-factory, not putting")
            return obj

        if obj.assumptions != []:
            cprint("object has assumptions:", obj, obj.assumptions)
            raise Exception("can not store in cache object with assumptions")

        module_record = sys.modules[obj.__module__]
        if self.dda_modules_used == [] or self.dda_modules_used[-1] != module_record:
            self.dda_modules_used.append(module_record)

        if isinstance(obj, type):
            cprint("requested to put class, it will be constructed")
            obj = obj()

        sig = obj.get_signature() if sig is None else sig  # brutal
        cprint("put object:", obj, "signature", sig)
        saved = None
        if sig in self.cache:
            saved = self.cache[sig]
        self.cache[sig] = obj
        return saved

    def get(self, item, update=False):
        """
        generates and instance of DataAnalysis from something

        """

        cprint("interpreting", item)
        cprint("factory knows", self.cache)  # .keys())

        if item is None:
            cprint("item is None: is it a virtual class? should not be in the analysis!")
            raise Exception("virtual class, class with None inputs, is not allowed directly in the analysis")

        if not hasattr(self,'blueprint_class'):
            cprint("no analysis yet")
            return item

        if item.trivial:
            return item

        if isinstance(item, type) and issubclass(item, self.blueprint_class):
            cprint("is subclass of DataAnalysis, probably need to construct it")
            name = item.__name__
            cprint("class name:", name)
            if name in self.cache:
                c = self.cache[name]
                cprint("have cache for this name:", c)
                if isinstance(item, type):
                    cprint("it is class, constructing")
                    c = c()
                    cprint("constructed", c)
                    self.put(c)
                    # cprint("will store",c)
                    return c
                if isinstance(item, self.blueprint_class):
                    cprint("cache returns object, will use it:", c)
                    return c
            else:
                cprint("there is no such class registered!", name)
                raise Exception("there is no such class registered: " + name + " !")

        if isinstance(item, self.blueprint_class):
            cprint("is instance of DataAnalysis, signature", item.get_signature())

            if item.trivial:
                cprint("trivial class: is datahandle or file, returning", item)
                return item
            #if isinstance(item, DataHandle) or isinstance(item, DataFile):  # make as object call
            #    cprint("is datahandle or file, returning", item)
            #    return item

            if not item.infactory:  # make as object call
                cprint("is not in-factory, returning")
                return item

            s = item.get_signature()
            if s in self.cache:
                storeditem = self.cache[item.get_signature()]

                if isinstance(storeditem, type):
                    raise Exception("no classes can be stored!")

                # if item!=storeditem: # object was constructed during the declaration of the class or interpreted earlier, either way the ___new__ call had to guarantee it is the same. it has to be the same
                #    raise Exception("critical violation of sanity check! object was constructed twice "+str(item)+" vs "+str(storeditem))

                cprint("so, offered object:", item)
                cprint("     stored object:", storeditem)

                if not item.is_virtual():  # careful!
                    cprint("     offered object is non-virtual, simply returning")
                    cprint("     offered object complettion:", item._da_locally_complete)
                    cprint("     offered virtual reason:",
                           item._da_virtual_reason if hasattr(item, '_da_virtual_reason') else "")
                    # cprint("     offered object is non-virtual, forcing it")
                    # update=True #!!!!
                    ## think about this! update??
                    return item

                if len(item.assumptions) != 0:
                    cprint("     offered object has assumptions", item.assumptions)
                    cprint("     offered object complettion:", item._da_locally_complete)
                    cprint("     will copy and assign assumptions")

                    copied_storeditem = storeditem.__class__(dynamic=False)
                    copied_storeditem.assumptions = item.assumptions

                    return copied_storeditem

                if update:
                    cprint("recommendation is to force update")
                    self.put(item)
                    return item
                else:
                    cprint("attention! offered object is discarded:", item)  # ,item.export_data())
                    cprint("stored in factory (this will be the valid instance):",
                           storeditem)  # ,storeditem.export_data())
                    return storeditem

            cprint("no such object registered! registering")  # attention!
            self.put(item)
            return item

        if isinstance(item, str):
            cprint("considering string data handle")
            return DataHandle(item)

        if isinstance(item, unicode):
            cprint("considering unicode string data handle")
            return DataHandle(str(item))

        cprint("unable to interpret item: " + repr(item))
        cprint("factory knows",AnalysisFactory.cache)
        raise Exception("unable to interpret item: " + repr(item)+" blueprint: "+repr(self.blueprint_class))
        # return None

    cache_stack = []
    cache_assumptions = []
    comment = ""

    def WhatIfCopy(self, description, new_assumptions):
        if isinstance(new_assumptions, tuple):
            new_assumptions = list(new_assumptions)
        if not isinstance(new_assumptions, list):
            new_assumptions = [new_assumptions]

        cprint(render("{RED}go deeper! what if?{/} stack of size %i" % len(self.cache_stack)))
        if self.comment == description:
            cprint("will not push copy, already in the assumption", description)
            return

        cprint("pushing into stack current version, using copy", description)
        self.comment = description
        self.cache_stack.append(self.cache)
        self.cache_assumptions.append(new_assumptions)
        self.cache = {}  # this makes assumptions reset
        cprint("cache stack of size", len(self.cache_stack))
        cprint("cache stack last entry:", self.cache_stack[-1])

        for i, o in self.cache_stack[-1].items():
            cprint("promoting", i, 'assumptions', o.assumptions)
            # if o.is_virtual():
            #    cprint("virtual object, constructing empty copy")
            o.__class__(dynamic=False).promote()  # assume??

        for assumptions in self.cache_assumptions:
            cprint("assumption group:", assumptions)
            for a in assumptions:
                cprint("assumption", a)
                a.promote()

                #   else:
                #       cprint("non-virtual object! promoting object itself")
                #       o.promote() # assume??
        # self.cache=copy.deepcopy(self.cache_stack[-1]) # dangerous copy: copying call __new__, takes them from current cache
        cprint("current cache copied:", self.cache)

    def WhatIfNot(self):
        cprint("factory knows", self.cache)  # .keys())
        if self.cache_stack == []:
            cprint("empty stack!")
            return
            # raise Exception("empty stack!")

        cprint("poping from stack last version")
        self.cache = self.cache_stack.pop()
        assumptions = self.cache_assumptions.pop()
        cprint("discarding last stacked", self.comment)
        cprint("discarding:", assumptions)
        cprint("current assumptions level %i:" % len(self.cache_assumptions),
               self.cache_assumptions[-1] if self.cache_assumptions != [] else "none")
        self.comment = ""
        cprint("factory knows", self.cache)  # .keys())

    def byname(self, name):
        if name not in self.cache:
            raise Exception("name is not known, can not get this: " + name)
        return self.cache[name]

    def __getitem__(self, name):
        return self.byname(name)

    def __iter__(self):
        for i in self.cache.keys():
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

        # cprint('aliases:',self.aliases)
        return self.aliases

    def get_definitions(self):
        return self.definitions

        #   return [[a,b] for a,b in self.aliases if contains(h0,a)]

    def get_module_description(self):
        module_description = []
        for m in AnalysisFactory.dda_modules_used:
            cprint("module", m)
            if hasattr(m, "__dda_module_global_name__"):
                cprint("dda module global name", m.__dda_module_global_name__)
                module_description.append(['cache', m.__name__, m.__dda_module_global_name__])
            else:
                module_description.append(['filesystem', m.__name__, m.__file__])

        return module_description


AnalysisFactory = AnalysisFactoryClass()
