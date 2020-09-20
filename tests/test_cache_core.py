
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

def test_base_cache_object():
    from dataanalysis import core as da
    from dataanalysis import caches
    da.reset()

    cache=caches.cache_core.Cache()

    class Analysis(da.DataAnalysis):
        cached=True

        def main(self):
            self.data="test123"

    A=Analysis()
    A.get()

    da.reset()

    class Analysis(da.DataAnalysis):
        cached = True
        produce_disabled=True

    B = Analysis()
    B.get()


    assert B.data == A.data

def test_unhandled_handling():
    pass

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
    hashe = A._da_locally_complete
    assert  A._da_locally_complete == A._da_expected_full_hashe

    print((glob.glob(A._da_cached_path+"/*")))
    assert len(glob.glob(A._da_cached_path+"/*"))>2

    da.debug_output()

    cache.store_to_directory(A._da_locally_complete,A,"/tmp/blob/")

    print((glob.glob("/tmp/blob/*")))
    assert len(glob.glob("/tmp/blob/*")) > 2

    blob=cache.assemble_blob(A._da_locally_complete, A).read()

    print(blob)
    assert len(blob)>50

    open("blob.tgz", "wb").write(blob)

    ## restore

    rA = Analysis()

    print("restoring cache...")

    rc={'datafile_target_dir': '.'}
    cache.restore_from_blob("blob.tgz", hashe, rA, restore_config=A.prepare_restore_config(rc))

    print("restored:", A, A.data, A.datafile)

def test_base_cache_check_location():
    from dataanalysis import core as da
    from dataanalysis import caches
    da.reset()
    da.debug_output()

    cache=caches.cache_core.Cache()

    class Analysis(da.DataAnalysis):
        cached=True
        read_caches=[]
        store_preview_yaml=True

        def main(self):
            self.data="test123"

    A=Analysis()
    A.get()

    cached_path=cache.construct_cached_file_path(A._da_locally_complete,A)

    import os
    assert os.path.exists(cached_path)
    assert os.path.exists(cached_path+"/hash.txt")
    assert os.path.exists(cached_path+"/object_identity.yaml.gz")
    assert os.path.exists(cached_path + "/cache_preview.yaml.gz")


    #assert B.data == A.data
