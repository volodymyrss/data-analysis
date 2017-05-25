import dataanalysis as da

def debug_output():
    da.printhook.global_all_output=True
    da.printhook.global_permissive_output=True
    da.printhook.global_fancy_output=True
    da.printhook.LogStreams=[]

def test_generate_one():
    debug_output()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print "test",self.c
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class Analysis(da.DataAnalysis):
        def main(self):
            print "test",self.__class__
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r

    A=Analysis()
    r=A.get()

    print r
    print r[1].c,r[1].data
    assert r[0].data == 'data 0'
    assert r[1].data == 'data 1'



def test_clear_memory():
    pass

