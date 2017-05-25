import dataanalysis as da

def debug_output():
    da.printhook.global_all_output=True
    da.printhook.global_permissive_output=True
    da.printhook.global_fancy_output=True
    da.printhook.LogStreams=[]

def test_generate_list():
    debug_output()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print "test",self.c
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
        def main(self):
            print "test",self.__class__
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r

    A=AAnalysis()
    r=A.get()

    print r[1].c,r[1].data
    assert r[0].data == 'data 0'
    assert r[1].data == 'data 1'


def test_generate_join():
    debug_output()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print "test",self.c
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
        def main(self):
            print "test",self.__class__
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r
    
    class CAnalysis(da.DataAnalysis):
        input_list=AAnalysis

        data=None

        def main(self):
            print "test",self.__class__
            self.data=" ".join([repr(c) for c in self.input_list])

    A=CAnalysis()
    print A._da_expected_full_hashe
    r=A.get()

    print r._da_locally_complete
