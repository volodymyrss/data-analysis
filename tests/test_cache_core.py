
def test_base_cache():
    from dataanalysis import core as da
    from dataanalysis import caches

    cache=caches.cache_core.Cache()

    class Analysis(da.DataAnalysis):
        pass

    A=Analysis()

    A.data="somedata"
    hashe=('testhashe')

    cache.store(hashe,A)

    B = Analysis()

    cache.restore(hashe,B)

    assert B.data == A.data

def test_cache_blob():
    from dataanalysis import core as da
    from dataanalysis import caches
    import glob

    cache = caches.cache_core.Cache()

    class Analysis(da.DataAnalysis):
        cached=True

        read_caches=[]

        def main(self):
            self.data="datax"
            open("file.txt","w").write("filedata")
            self.datafile=da.DataFile("file.txt")

    A = Analysis()
    A.get()
    A._da_locally_complete
    assert  A._da_locally_complete == A._da_expected_full_hashe

    print(glob.glob(A._da_cached_path+"/*"))
    assert len(glob.glob(A._da_cached_path+"/*"))>2

    da.debug_output()

    cache.store_to_directory(A._da_locally_complete,A,"/tmp/blob/")

    print glob.glob("/tmp/blob/*")
    assert len(glob.glob("/tmp/bundle/*")) > 2

    blob=cache.assemble_blob(A._da_locally_complete, A).read()

    print blob
    assert len(blob)>50

