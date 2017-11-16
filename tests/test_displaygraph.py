import dataanalysis.core as da
from dataanalysis import displaygraph


def test_plot():
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        input_a=AAnalysis

    b=BAnalysis().get()

    displaygraph.plot_hashe(b._da_locally_complete,"test.png",show=False)