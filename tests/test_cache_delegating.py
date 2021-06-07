

import pytest

import dataanalysis.core as da
import imp


def define_analysis():
    import ddmoduletest
    imp.reload(ddmoduletest)


@pytest.mark.mysql
def test_queue_cache():
    from dataanalysis import caches
    import dataanalysis.caches.queue

    da.debug_output()
    da.reset()

    q_cache=caches.queue.QueueCache()
    q_cache.queue.purge()

    define_analysis()

    assert len(da.AnalysisFactory.dda_modules_used)==1

    A=dataanalysis.core.AnalysisFactory['AAnalysis']
    A.produce_disabled=True
    A.cache = q_cache
    A.cached = True

    with pytest.raises(da.AnalysisDelegatedException):
        A.get()

    print((q_cache.queue.info))
    assert q_cache

    # worker part

    f_cache=caches.cache_core.CacheNoIndex()
    #f_cache.parent=q_cache

    define_analysis()

    A = dataanalysis.core.AnalysisFactory['AAnalysis']
    A.produce_disabled = False
    A.cache = f_cache
    A.cached = True

    worker=caches.queue.QueueCacheWorker("/tmp/queue")
    print((q_cache.queue.info))
    print(("with worker",worker))

    print((worker.run_all()))

@pytest.mark.mysql
def test_queue_cache_generative():
    from dataanalysis import caches
    import dataanalysis.caches.queue

    da.debug_output()
    da.reset()

    q_cache=caches.queue.QueueCache("./queue_test")
    q_cache.queue.purge()

    assert len(q_cache.queue.list())==0

    define_analysis()

    A=dataanalysis.core.AnalysisFactory['GenerativeAnalysis']

    for name in 'AAnalysis',:
        aA=dataanalysis.core.AnalysisFactory[name].__class__
        aA.cache = q_cache
        aA.produce_disabled=True
        aA.cached = True

    with pytest.raises(da.AnalysisDelegatedException) as e:
        A.get()

    print(('AnalysisDelegatedException',e.value,e.value.origin,e.value.hashe))
    assert len(e.value.hashe[1:])==2

    assert len(e.value.source_exceptions)==2
    for se in e.value.source_exceptions:
        print(('found multiple AnalysisDelegatedException',se,se.origin))


    print((q_cache.queue.list()))
#    assert len(q_cache.queue.list())==2
    assert len(q_cache.queue.list())==1

    # worker part

    f_cache=caches.cache_core.CacheNoIndex()
    #f_cache.parent=q_cache

    define_analysis()

    A = dataanalysis.core.AnalysisFactory['AAnalysis']
    A.produce_disabled = False
    A.cache = f_cache
    A.cached = True

    worker=caches.queue.QueueCacheWorker("./queue_test")

    print((worker.run_once()))

@pytest.mark.mysql
def test_queue_reconstruct_env():
    from dataanalysis import caches
    import dataanalysis.caches.queue

    da.debug_output()
    da.reset()

    q_cache=caches.queue.QueueCache()
    q_cache.wipe_queue(["waiting", "done", "running"])

    define_analysis()

    A=dataanalysis.core.AnalysisFactory['AAnalysis']
    A.produce_disabled=True
    A.cache = q_cache
    A.cached = True

    with pytest.raises(da.AnalysisDelegatedException):
        A.get()

    # worker part

    f_cache=caches.cache_core.CacheNoIndex()
    #f_cache.parent=q_cache

    da.reset()

    #A = dataanalysis.core.AnalysisFactory['Analysis']
    #A.produce_disabled = False
    #A.cache = f_cache
    #A.cached = True

    worker=caches.queue.QueueCacheWorker()

    print((worker.run_all()))


#    worker.run_all()

def test_delegating_analysis():
    #import dataanalysis
    from dataanalysis import caches
    import dataanalysis.caches.delegating

   # da.debug_output()
    da.reset()

    q_cache=caches.delegating.DelegatingCache()

    da.DataAnalysis.cache.tail_parent(q_cache)

    class A1Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata1"

    class A2Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata2"

    class A3Analysis(da.DataAnalysis):

        def main(self):
            self.data="datadata3"

    class BAnalysis(da.DataAnalysis):
        input_a1 = A1Analysis
        input_a2 = A2Analysis
        input_a3 = A3Analysis

        def main(self):
            self.data="datadata"

    A=BAnalysis()

    print((A.cache.list_parent_stack()))

    with pytest.raises(da.AnalysisDelegatedException):
        A.get()

    try:
        A.get()
    except da.AnalysisDelegatedException as e:
        print(("resources:",e.resources))

    del da.DataAnalysis.cache.parent

def test_delegating_generative():
    #import dataanalysis
    from dataanalysis import caches
    import dataanalysis.caches.delegating

   # da.debug_output()
    da.reset()

    q_cache=caches.delegating.DelegatingCache()

    da.DataAnalysis.cache.tail_parent(q_cache)

    class A1Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata1"

    class A2Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata2"

    class BAnalysis(da.DataAnalysis):
        input_a1 = A1Analysis
        input_a2 = A2Analysis

        def main(self):
            self.data=[A1Analysis(),A2Analysis()]

    A=BAnalysis()

    print((A.cache.list_parent_stack()))

    try:
        A.get()
    except da.AnalysisDelegatedException as e:
        print(("delegating exception:",e.__class__,e,e.hashe))
        print(("resources:",e.resources))

    del da.DataAnalysis.cache.parent

def test_selective_delegation():
    import dataanalysis.caches.delegating
    da.debug_output()

    class TCache(dataanalysis.caches.delegating.SelectivelyDelegatingCache):
        delegating_analysis=['AAnalysis','BAnalysis$']

    #cache=Cache()

    class AAnalysis(da.DataAnalysis):
        #read_caches = [q_cache.__class__]
        cached = True

        cache=TCache()

        def main(self):
            #self.data = "datadata1"
            raise Exception("this should have been delegated")

    class BAnalysis(da.DataAnalysis):
        #read_caches = [q_cache.__class__]
        cached = True

        cache=TCache()

        def main(self):
            self.data = "datadata2"

    A=AAnalysis()
    with pytest.raises(da.AnalysisDelegatedException):
        A.get()

    B=BAnalysis()
    B.get()

def test_multiple_delegation():
    #import dataanalysis
    from dataanalysis import caches
    import dataanalysis.caches.delegating

   # da.debug_output()
    da.reset()

    q_cache=caches.delegating.DelegatingCache()

    da.DataAnalysis.cache.tail_parent(q_cache)

    class A1Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata1"

    class A2Analysis(da.DataAnalysis):
        read_caches = [q_cache.__class__]
        cached = True

        def main(self):
            self.data="datadata2"

    class BAnalysis(da.DataAnalysis):
        input_a1 = A1Analysis
        input_a2 = A2Analysis

        def main(self):
            self.data=[A1Analysis(),A2Analysis()]

    A=BAnalysis()

    print((A.cache.list_parent_stack()))

    try:
        A.get()
    except da.AnalysisDelegatedException as e:
        print(("delegating exception:",e.__class__,e,e.hashe))
        print(("resources:",e.resources))

    del da.DataAnalysis.cache.parent
