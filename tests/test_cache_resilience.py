import os
import glob

def test_cache_corruption():
    from dataanalysis import core as da
    from dataanalysis import caches
    da.reset()

    cache=caches.cache_core.Cache()


    class InputAnalysis(da.DataAnalysis):
        cached=True

        def main(self):
            fn = "data.txt"
            open(fn, "w").write("input-test123")            
            self.data_file = da.DataFile(fn)

    class Analysis(da.DataAnalysis):
        input_a = InputAnalysis
        
        def main(self):
            c = open(self.input_a.data_file.get_path()).read()
            self.data = "test123-" + c

    I = InputAnalysis()
    I.get()
    
    I.produce_disabled = True

    
    for fn in glob.glob("filecache/InputAnalysis.v0/*/data.txt.gz") + glob.glob("data.txt*"):
        os.remove(fn)    

    da.reset()

    InputAnalysis()

    A = Analysis()
    A.get()

    print(A.data)
