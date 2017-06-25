from dataanalysis import core as da


def debug_output():
    import dataanalysis
    dataanalysis.printhook.global_all_output=True
    dataanalysis.printhook.global_permissive_output=True
    dataanalysis.printhook.global_fancy_output=True
    dataanalysis.printhook.LogStreams=[]

def test_generate_list():
    debug_output()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print("test",self.c)
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
	#run_for_hashe=True
        def main(self):
            print("test",self.__class__)
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r

    A=AAnalysis()
    r=A.get()

    print(r[1].c,r[1].data)
    assert r[0].data == 'data 0'
    assert r[1].data == 'data 1'


def test_generate_join():
    debug_output()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print("test",self.c)
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
        def main(self):
            print("test",self.__class__)
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r
    
    class CAnalysis(da.DataAnalysis):
        input_list=AAnalysis

        data=None

        def main(self):
            print("test",self.__class__)
            self.data=" ".join([repr(c) for c in self.input_list])

    A=CAnalysis()
    A.process(output_required=False)
    expected_hashe=A._da_expected_full_hashe
    r=A.get()

    got_hashe=r._da_locally_complete

    print(expected_hashe)
    print(got_hashe)

    assert expected_hashe != got_hashe
