

def test_one_object():
    import dataanalysis as da
    da.reset()

    class Analysis(da.DataAnalysis):

        def main(self):
            print "test"
            self.data ="datacontent"


    A = Analysis()
    A.get()

    assert A.data == 'datacontent'

    j=A.jsonify()

    assert j['data']=='datacontent'
