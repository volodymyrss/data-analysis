from __future__ import print_function

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

