from __future__ import print_function

import pytest

import dataanalysis.core as da


def define_analysis():
    class Analysis(da.DataAnalysis):
        def main(self):
            self.data="datadata"


def test_queue_cache():
    from dataanalysis import caches
    import dataanalysis.caches.delegating

    #da.debug_output()
    da.reset()

    q_cache=caches.delegating.QueueCache()
    q_cache.wipe_queue()

    define_analysis()

    A=dataanalysis.core.AnalysisFactory['Analysis']
    A.produce_disabled=True
    A.cache = q_cache
    A.cached = True

    with pytest.raises(caches.delegating.DelegatedNoticeException):
        A.get()

    q_cache

    # worker part

    f_cache=caches.cache_core.CacheNoIndex()
    #f_cache.parent=q_cache

    define_analysis()

    A = dataanalysis.core.AnalysisFactory['Analysis']
    A.produce_disabled = False
    A.cache = f_cache
    A.cached = True

    worker=caches.delegating.QueueCacheWorker()

    print(worker.run_once())


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

    print(A.cache.list_parent_stack())

    with pytest.raises(da.AnalysisDelegatedException):
        A.get()

    try:
        A.get()
    except da.AnalysisDelegatedException as e:
        print("expectations:",e.expectations)
