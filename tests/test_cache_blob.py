
def test_cache_blob():
    from dataanalysis import core as da
    from dataanalysis import caches
    import glob

    class MyCacheBlob(caches.cache_core.CacheBlob):
        fn = "temp-test-blob.bin"

        def deposit_blob(self, hashe, blob):
            open(self.fn, "wb").write(blob.read())
        
        def retrieve_blob(self, hashe):
            return open(self.fn, "rb")

    blob_cache = MyCacheBlob()

    class AnalysisInBlob(da.DataAnalysis):
        cached = True
        cache = blob_cache

        read_caches = [MyCacheBlob]

        def main(self):
            self.data = "datax"
            open("file.txt","w").write("filedata")
            self.datafile=da.DataFile("file.txt")

    A = AnalysisInBlob()
    A.get()

    indata = A.export_data()
    inf = indata.pop('datafile')

    hashe = A._da_locally_complete
    assert  A._da_locally_complete == A._da_expected_full_hashe

    da.debug_output()
    da.reset()

    ## restore

    rA = AnalysisInBlob()

    rA.produce_disabled = True
    rA.get()

    redata = rA.export_data()
    redata.pop('produce_disabled')

    ref = redata.pop('datafile')


    print("data before", indata)
    print("data after", redata)
    
    assert indata == redata


    assert inf.open().read() == ref.open().read()

    print("succesfully restored from cache blob", rA.export_data())


