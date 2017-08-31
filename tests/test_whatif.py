def test_whatif():
    from dataanalysis import core as da

    da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        pass


    aa=AAnalysis(use_arg="arg1")
    aa.promote()

    aa1 = AAnalysis()
    assert aa1.arg == "arg1"

    da.AnalysisFactory.WhatIfCopy("test",[BAnalysis()])

    aa1 = AAnalysis()
    assert aa1.arg == "arg1"

