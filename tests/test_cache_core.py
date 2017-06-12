
def test_base_cache():
    from dataanalysis import core as da
    from dataanalysis import caches

    cache=caches.core.Cache()

    class Analysis(da.DataAnalysis):
        pass

    A=Analysis()

    A.data="somedata"
    hashe=('testhashe')

    cache.store(hashe,A)

    B = Analysis()

    cache.restore(hashe,B)

    assert B.data == A.data