import pytest
import imp

def test_object_export_import():
    from dataanalysis import core as da

    #da.debug_output()
    da.reset()

    class Analysis(da.DataAnalysis):
        #cached = True


        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data1"

    A=Analysis()
    A.get()

    print((A.data))

    assert A.data == "data1"

    object_data=A.export_data()

    da.reset()

    B=Analysis()

    with pytest.raises(AttributeError):
        print((B.data))

    B.import_data(object_data)
    assert B.data == "data1"


def test_object_injection_identity():
    from dataanalysis import core as da
    from dataanalysis.emerge import emerge_from_identity

    da.reset()

    import ddmoduletest
    imp.reload(ddmoduletest)

    da.AnalysisFactory.assume_serialization(("AAnalysis",{'assumed_data':"bdata1"}))

    B=ddmoduletest.BAnalysis()
    B.get()

    assert B.data=="dataB_A:dataAassumed:bdata1"

    ident=B.get_identity()

    assert len(ident.assumptions)==1
    print((ident.assumptions))
    assert ident.assumptions[0][1]['assumed_data']=="bdata1"

    da.reset()

    C=emerge_from_identity(ident)
    C.get()

    assert C.data == "dataB_A:dataAassumed:bdata1"


def test_object_injection():
    from dataanalysis import core as da

    #da.debug_output()
    da.reset()

    class Analysis(da.DataAnalysis):
        #cached = True

        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data2"

    A=Analysis()
    A.get()

    assert A.data == "data2"

    serialization=A.serialize()

    da.reset()

    class Analysis(da.DataAnalysis):
        pass

    da.AnalysisFactory.inject_serialization(serialization)
    B=Analysis()

    assert B.data == "data2"

def test_object_input_injection():
    from dataanalysis import core as da

    da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
#        arg=None
        pass

    A1 = AAnalysis(use_arg="arg1")

    assert  A1.arg == "arg1"
    d1 = A1.export_data()
    print(("has data:", d1))
    assert d1['arg'] == 'arg1'

    A2 = AAnalysis(use_arg="arg2")


    a1 = A1.serialize()
    a2 = A2.serialize()

    da.AnalysisFactory.inject_serialization(a1)

    print(("factory has",da.AnalysisFactory.cache['AAnalysis']))

    aanalysis=da.AnalysisFactory['AAnalysis']
    assert aanalysis.arg == "arg1"

    class Analysis(da.DataAnalysis):
        #cached = True
        input_arg=AAnalysis

        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data_"+self.input_arg.arg

    A=Analysis()
    A.get()

    print((A.data))

    assert A.data == "data_arg1"

    da.reset()

    class AAnalysis(da.DataAnalysis):
        pass


    class Analysis(da.DataAnalysis):
        #cached = True
        input_arg=AAnalysis

        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data_"+self.input_arg.arg


    da.AnalysisFactory.inject_serialization(a2)

    B=Analysis()
    B.get()

    assert B.data == "data_arg2"

def test_object_serialization_with_use():
    from dataanalysis import core as da

    da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
        class_arg=1

    A=AAnalysis(use_usearg=2)
    A.promote()

    AA=AAnalysis()
    aac=AA.serialize()

    assert aac[1]['usearg'] == 2
    assert aac[1]['class_arg'] == 1

    assert 'version' in aac[1]



def test_object_injection_external():
    from dataanalysis import core as da

    da.debug_output()
    da.reset()

    class AAnalysis(da.DataAnalysis):
#        arg=None
        pass

    A1 = AAnalysis(use_arg="arg1")

    assert  A1.arg == "arg1"
    d1 = A1.export_data()
    print(("has data:", d1))
    assert d1['arg'] == 'arg1'


    a1 = A1.serialize()

    print(("serilization:",a1))

    da.AnalysisFactory.inject_serialization(a1)

    print(("factory has",da.AnalysisFactory.cache['AAnalysis']))

    aanalysis=da.AnalysisFactory['AAnalysis']
    assert aanalysis.arg == "arg1"

    class Analysis(da.DataAnalysis):
        #cached = True
        input_arg=AAnalysis

        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data_"+self.input_arg.arg

    A=Analysis()
    A.get()

    print((A.data))

    assert A.data == "data_arg1"

def test_object_injection_reset():
    from dataanalysis import core as da
    da.reset()
    da.debug_output()

    class AAnalysis(da.DataAnalysis):
#        arg=None
        pass

    A1 = AAnalysis(use_arg="arg1")

    assert  A1.arg == "arg1"
    d1 = A1.export_data()
    print(("has data:", d1))
    assert d1['arg'] == 'arg1'

    a1 = A1.serialize()

    print(("serilization:",a1))

    da.reset()
    da.debug_output()

    class AAnalysis(da.DataAnalysis):
        pass

    da.AnalysisFactory.inject_serialization(a1)


    print(("factory has",da.AnalysisFactory.cache['AAnalysis']))

    aanalysis=da.AnalysisFactory['AAnalysis']
    assert aanalysis.arg == "arg1"

    class Analysis(da.DataAnalysis):
        #cached = True
        input_arg=AAnalysis

        def main(self):
            print("test")
            # time.sleep(3)
            self.data = "data_"+self.input_arg.arg

    A=Analysis()
    A.get()

    print((A.data))

    assert A.data == "data_arg1"

