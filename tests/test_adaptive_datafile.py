

def test_numpy_to_datafile():
    from dataanalysis import core as da
    import os
    da.reset()

    import numpy as np

    class Analysis(da.DataAnalysis):
        read_caches=[]

        cached=True

        def main(self):
            print("test")
            self.data = np.linspace(0,1,100)


    A = Analysis()
    A.get()

    #da.TransientCacheInstance.reset()
    da.reset()
    da.debug_output()

    B = Analysis()
    B.produce_disabled=True
    B.read_caches = [A.cache.__class__]
    B.get()

    #B.cache.restore(A._da_locally_complete,A)

    assert hasattr(B,'_datafile_data')

    assert hasattr(B._datafile_data,'adopted_format')
    assert B._datafile_data.adopted_format == "numpy"

    print B._datafile_data.get_path()
    assert os.path.exists(B._datafile_data.get_path())

    assert hasattr(B, 'data')

    assert all(A.data == B.data)



