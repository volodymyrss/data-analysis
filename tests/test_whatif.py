def test_whatif():
    from dataanalysis import core as da

  #  da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        pass


    aa=AAnalysis(use_arg="arg1",use_nonearg=None)
    aa.promote()

    aa1 = AAnalysis()
    assert aa1.arg == "arg1"
    serial=aa1.serialize(verify_jsonifiable=False)
    print(serial)

    assert serial[1]['arg'] == "arg1"
    assert serial[1]['nonearg'] != str(None)
    assert serial[1]['nonearg'] is None

    da.AnalysisFactory.WhatIfCopy("test",[BAnalysis()])

    aa1 = AAnalysis()
    assert aa1.arg == "arg1"

    assert hasattr(aa1,'nonearg')


    print(("NoneArg:",aa1.nonearg,type(aa1.nonearg)))

    assert aa1.nonearg is None



def test_assumption():
    from dataanalysis import core as da

  #  da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        input_a=AAnalysis

        def main(self):
            self.d=self.input_a.arg


    ba=BAnalysis(assume=AAnalysis(use_arg="arg1",use_nonearg=None)).get()
    assert ba._da_locally_complete
    assert ba.d=="arg1"




def test_self_assumption():
    from dataanalysis import core as da

    da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass

    aa = AAnalysis(assume=AAnalysis(use_arg="arg2", use_nonearg=None)).get()
    assert aa._da_locally_complete

    assert not hasattr(aa,'arg') # current behavoir and it is bad
    #assert aa.arg == "arg2"








