

def test_recursive_datafile():
    from dataanalysis import core as da
    import os
    da.reset()
    da.debug_output()
    class Analysis(da.DataAnalysis):
        cached=True

        def main(self):
            print("test")
            fn="file.txt"
            with open(fn,"w") as f:
                f.write("test")

            fn1="file1.txt"
            with open(fn1,"w") as f:
                f.write("test")

            self.data = [da.DataFile(fn)]
            self.data1 = da.DataFile(fn1)


    A = Analysis()
    A.read_caches = []
    A.get()

    da.reset()

    os.remove("file.txt")
    os.remove("file1.txt")
    B = Analysis()
    B.produce_disabled=True
    B.get()

    #B.cache.restore(A._da_locally_complete,A)

    assert hasattr(B,'data')

    print((B.data))

    #assert hasattr(B._datafile_data,'adopted_format')
    #assert B._datafile_data.adopted_format == "numpy"

    #print B._datafile_data.get_path()
    assert os.path.exists(B.data1.get_path())
    assert os.path.exists(B.data[0].get_path())

    #assert hasattr(B, 'data')




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

    print((B._datafile_data.get_path()))
    assert os.path.exists(B._datafile_data.get_path())

    assert hasattr(B, 'data')

    assert all(A.data == B.data)

#    assert all(A.data == B.data)


def test_numpy_to_datafile_recursive():
    from dataanalysis import core as da
    import os
    da.reset()

    import numpy as np
    import pandas as pd

    class Analysis(da.DataAnalysis):
        read_caches=[]

        cached=True

        def main(self):
            print("test")
            self.data = {'a':np.linspace(0,1,100),'b/c!':pd.DataFrame(np.linspace(0,2,200))}


    A = Analysis()
    A.get()

    content=A.export_data()
    cc=A.cache.adopt_datafiles(content)
    assert cc['data']['a'] is None

    #da.TransientCacheInstance.reset()
    da.reset()
    da.debug_output()

    B = Analysis()
    B.produce_disabled=True
    B.read_caches = [A.cache.__class__]
    B.get()

    print((B.data['a']))

    #B.cache.restore(A._da_locally_complete,A)

    assert hasattr(B,'_datafile_data_a')

    assert hasattr(B._datafile_data_a,'adopted_format')
    assert B._datafile_data_a.adopted_format == "numpy"

    print((B._datafile_data_a.get_path()))
    assert os.path.exists(B._datafile_data_a.get_path())

    assert hasattr(B, 'data')

    assert all(A.data['a'] == B.data['a'])

#    assert all(A.data == B.data)




def test_pandas_to_datafile():
    from dataanalysis import core as da
    import os
    da.reset()

    import pandas as pd
    import numpy as np

    class Analysis(da.DataAnalysis):
        read_caches=[]

        cached=True

        def main(self):
            print("test")
            self.data = pd.DataFrame()
            self.data['data']=np.linspace(0,1,100)


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
    assert B._datafile_data.adopted_format == "pandas"

    print((B._datafile_data.get_path()))
    assert os.path.exists(B._datafile_data.get_path())

    assert hasattr(B, 'data')

    assert all(abs(A.data.data - B.data.data)<1e-5)