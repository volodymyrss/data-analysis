import pytest

def define_analysis():
    import dataanalysis as da

    class Analysis(da.DataAnalysis):
        def main(self):
            self.data="datadata"



def test_queue_cache():
    import caches.delegating
    import caches.core
    import dataanalysis as da
    import analysisfactory

    #da.debug_output()
    da.reset()

    q_cache=caches.delegating.QueueCache()
    q_cache.wipe_queue()

    define_analysis()

    A=analysisfactory.AnalysisFactory['Analysis']
    A.produce_disabled=True
    A.cache = q_cache
    A.cached = True

    with pytest.raises(caches.delegating.DelegatedNoticeException):
        A.get()

    # worker part

    f_cache=caches.core.CacheNoIndex()
    #f_cache.parent=q_cache

    define_analysis()

    A = analysisfactory.AnalysisFactory['Analysis']
    A.produce_disabled = False
    A.cache = f_cache
    A.cached = True

    worker=caches.delegating.QueueCacheWorker()

    print worker.run_once()


#    worker.run_all()

