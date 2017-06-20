def test_store_restore():
    import dataanalysis.core as da
    from dataanalysis.caches.sdsc import SDSCCache

    cache=SDSCCache()

    class Analysis(da.DataAnalysis):
        pass

    A=Analysis()

    A.data="somedata"
    hashe=('testhashe')

    cache.store(hashe,A)

    B = Analysis()

    cache.restore(hashe,B)

    assert B.data == A.data
