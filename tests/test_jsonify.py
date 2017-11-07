

def test_one_object():
    from dataanalysis import core as da
    da.reset()

    class Analysis(da.DataAnalysis):

        def main(self):
            print("test")
            self.data ="datacontent"


    A = Analysis()
    A.get()

    assert A.data == 'datacontent'

    j=A.jsonify()

    assert j['data']=='datacontent'

def test_big_object():
    from dataanalysis import core as da
    import numpy as np
    da.reset()

    class Analysis(da.DataAnalysis):

        def main(self):
            print("test")
            self.data ="datacontent"
            self.dataarray=np.linspace(0,1,200)


    A = Analysis()
    A.get()

    assert A.data == 'datacontent'

    j=A.jsonify()

    assert j['data']=='datacontent'
    assert j['dataarray'] is None
