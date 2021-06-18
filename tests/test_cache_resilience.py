import os
import glob
import pytest

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
            print(f"running {self} writting {fn}")

    class Analysis(da.DataAnalysis):
        input_a = InputAnalysis
        
        def main(self):
            c = open(self.input_a.data_file.get_path()).read()
            self.data = "test123-" + c

    I = InputAnalysis()
    I.get()
        
    for fn in glob.glob(os.path.join(
                os.environ.get("DDA_DEFAULT_CACHE_ROOT", "filecache"), 
                "InputAnalysis.v0/*/data.txt.gz"
              )) + \
              glob.glob("data.txt*"):
        print("removing", os.path.abspath(fn))
        os.remove(fn)    

    da.reset()

    InputAnalysis.produce_disabled = True
    I = InputAnalysis()
    I.produce_disabled = True
    
    with pytest.raises(da.ProduceDisabledException):
        A = Analysis()
        A.get()
    

