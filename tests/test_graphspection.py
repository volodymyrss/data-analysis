from __future__ import print_function

import os


def test_factorize():

    import dataanalysis.core as da

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        input_a=AAnalysis

    class CAnalysis(da.DataAnalysis):
        input_b=BAnalysis

    c=CAnalysis().get()
    print(c._da_locally_complete)


def test_factorize_note():

    import dataanalysis.core as da
    da.reset()

    da.AnalysisFactory.note_factorization(dict(a=1,b=2,c=[1,2,{1:2}]))

    class AAnalysis(da.DataAnalysis):
        read_caches=[]
        cached=True

    A=AAnalysis().get()

    fn=A._da_cached_path+"/factorizations.txt"
    assert os.path.exists(fn)

    print(">>>>\n"+open(fn).read()+"<<<<")
