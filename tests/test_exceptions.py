

def test_one_object():
    from dataanalysis import core as da

    class Analysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data ="data"
            raise Exception("test error")

    A=Analysis()

    try:
        A.get()
    except da.UnhandledAnalysisException as e:
        print(e)

    assert A.data == 'data'


