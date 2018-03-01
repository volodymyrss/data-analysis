import dataanalysis.importing as importing
from dataanalysis.caches.resources import jsonify
from dataanalysis.printhook import log
import dataanalysis.graphtools

dd_module_names=[]

def import_ddmodules(module_names=None):
    if module_names is None:
        module_names=dd_module_names

    modules=[]
    for dd_module_name in module_names:
        if isinstance(dd_module_name,str) and dd_module_name.startswith("dataanalysis."):
            continue

        log("importing", dd_module_name)
        dd_module=importing.load_by_name(dd_module_name)
        modules.append(dd_module[0])

        log("module",dd_module[1],"as",dd_module[0],"set to global namespace")
        globals()[dd_module[1]]=dd_module[0]

 #       reload(dd_module[0])

    return modules

class InconsitentEmergence(Exception):
    def __init__(self,message,cando,wanttodo):
        self.message=message
        self.cando=cando
        self.wanttodo=wanttodo

    def __repr__(self):
        return self.__class__.__name__+": "+repr(self.message)

def emerge_from_identity(identity):
    import dataanalysis.core as da
    da.reset()

    reload(dataanalysis.graphtools)
    print("fresh factory knows",da.AnalysisFactory.cache)


    import_ddmodules(identity.modules)

    log("assumptions:",identity.assumptions)

    #A = da.AnalysisFactory.byname(identity.factory_name)

    for assumption in identity.assumptions:
        log("requested assumption:", assumption)

        if assumption[0] == '':
            log("ATTENTION: dangerous eval from string",assumption[1])
            da.AnalysisFactory.WhatIfCopy('queue emergence', eval(assumption[1]))
        else:
            a = da.AnalysisFactory.byname(assumption[0])
            a.import_data(assumption[1])

            print(a, "from", assumption)

    A = da.AnalysisFactory.byname(identity.factory_name)
    producable_hashe=A.get_hashe()

    if identity.expected_hashe is None:
        log("expected hashe verification skipped")
    elif jsonify(producable_hashe) != jsonify(identity.expected_hashe):
        log("producable:\n",jsonify(producable_hashe),"\n")
        log("requested:\n",jsonify(identity.expected_hashe))

        try:
            from dataanalysis import displaygraph
            displaygraph.plot_hashe(producable_hashe,"producable.png")
            displaygraph.plot_hashe(identity.expected_hashe,"expected.png")
        except Exception as e:
            pass

        raise InconsitentEmergence(
            "unable to produce\n"+repr(jsonify(identity.expected_hashe))+"\n while can produce"+repr(jsonify(producable_hashe)),
            jsonify(producable_hashe),
            jsonify(identity.expected_hashe),
        )

    return A

def emerge_from_graph(graph):
    pass
