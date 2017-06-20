def test_store_restore():
    import dataanalysis.core as da
    from dataanalysis.caches.sdsc import SDSCCache, blob_store

    cache=SDSCCache()

    class Analysis(da.DataAnalysis):
        cached=True

    A=Analysis()

    A.data="somedata"
    hashe=('testhashe')

    cache.store(hashe,A)

    print(blob_store.blobs)

    #da.reset()

    #B = Analysis()

    #cache.restore(hashe,B)

    #assert B.data == A.data
