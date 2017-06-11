import pytest

def test_object_export_import():
    import caches.delegating
    import caches.core
    import dataanalysis as da
    import analysisfactory

    #da.debug_output()
    da.reset()

    class Analysis(da.DataAnalysis):
        #cached = True


        def main(self):
            print "test"
            # time.sleep(3)
            self.data = "data1"

    A=Analysis()
    A.get()

    print A.data

    assert A.data == "data1"

    object_data=A.export_data()

    da.reset()

    B=Analysis()

    with pytest.raises(AttributeError):
        print B.data

    B.import_data(object_data)
    assert B.data == "data1"

def test_object_injection():
    import caches.delegating
    import caches.core
    import dataanalysis as da
    import analysisfactory

    #da.debug_output()
    da.reset()

    class Analysis(da.DataAnalysis):
        #cached = True

        def main(self):
            print "test"
            # time.sleep(3)
            self.data = "data2"

    A=Analysis()
    A.get()

    assert A.data == "data2"

    serialization=A.serialize()

    da.reset()

    class Analysis(da.DataAnalysis):
        pass

    analysisfactory.AnalysisFactory.inject_serialization(serialization)
    B=Analysis()

    assert B.data == "data2"

def test_object_export_import():
    import caches.delegating
    import caches.core
    import dataanalysis as da
    import analysisfactory

    #da.debug_output()
    da.reset()

    return

    class AAnalysis(da.DataAnalysis):
        arg="arg1"


    class AAnalysis(da.DataAnalysis):
        arg="arg2"

    class Analysis(da.DataAnalysis):
        #cached = True
        input_arg=AAnalysis

        def main(self):
            print "test"
            # time.sleep(3)
            self.data = "data1"

    A=Analysis()
    A.get()

    print A.data

    assert A.data == "data1"

    object_data=A.export_data()

    da.reset()

    B=Analysis()

    with pytest.raises(AttributeError):
        print B.data

    B.import_data(object_data)
    assert B.data == "data1"