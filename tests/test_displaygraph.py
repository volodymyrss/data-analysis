import os

import dataanalysis.core as da
from dataanalysis import displaygraph


def test_plot():
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        input_a = AAnalysis

    class CAnalysis(da.DataAnalysis):
        input_a = AAnalysis
        input_b = BAnalysis

    class FAnalysis(da.DataAnalysis):
        input_a = AAnalysis
        input_b = BAnalysis
        input_c = CAnalysis

    b=FAnalysis().get()

    try:
        displaygraph.plot_hashe(b._da_locally_complete,"test.png",show=os.environ.get("DISPLAY","")!="")
    except:
        pass
