from dataanalysis import cache_core as cache_core
from dataanalysis import core as da


def debug_output():
    import dataanalysis
    dataanalysis.printhook.global_all_output=True
    dataanalysis.printhook.global_permissive_output=True
    dataanalysis.printhook.global_fancy_output=True
    dataanalysis.printhook.LogStreams=[]

def test_generate_list():
    da.debug_output()
    da.reset()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print(("test",self.c))
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
        run_for_hashe=True
        def main(self):
            print(("test",self.__class__))
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r

    A=AAnalysis()
    r=A.get()

    print((r[1].c,r[1].data))
    assert r[0].data == 'data 0'
    assert r[1].data == 'data 1'


def test_generate_join():
    debug_output()
    da.AnalysisFactory.reset()

    class BAnalysis(da.DataAnalysis):
        c=None

        _da_settings=["c"]

        def main(self):
            print(("test",self.c))
            self.data="data "+repr(self.c)

        def __repr__(self):
            return "[A %s]"%repr(self.c)

    
    class AAnalysis(da.DataAnalysis):
        def main(self):
            print(("test",self.__class__))
            r=[BAnalysis(use_c=c) for c in range(3)]
            return r
    
    class CAnalysis(da.DataAnalysis):
        input_list=AAnalysis

        data=None

        def main(self):
            print(("test",self.__class__))
            self.data=" ".join([repr(c) for c in self.input_list])

    A=CAnalysis()
    A.process(output_required=False)
    expected_hashe=A._da_expected_full_hashe
    r=A.get()

    got_hashe=r._da_locally_complete

    print(expected_hashe)
    print(got_hashe)

    assert expected_hashe != got_hashe


def test_generate_structures():
    debug_output()
    da.reset()

    class BAnalysis(da.DataAnalysis):
        c = None

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[A %s]" % repr(self.c)

    class CAnalysis(da.DataAnalysis):
        c = None

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[C %s]" % repr(self.c)


    class AAnalysis(da.DataAnalysis):
        # run_for_hashe=True
        def main(self):
            print(("test", self.__class__))
            r = [[BAnalysis(use_c=c),CAnalysis(use_c=c)] for c in range(3)]
            return r

    A = AAnalysis()
    r = A.get()

    print(r)

    print((r[1].c, r[1].data))

    #assert r[0]

    #assert isinstance(r[0][0],BAnalysis)
    assert r[0].data == 'data 0'
    assert r[2].data == 'data 1'

    assert r[1]._da_locally_complete

def test_generate_aliased():
    da.reset()
    da.debug_output()

    my_cache=cache_core.Cache("./local-test")

    class BAnalysis(da.DataAnalysis):
        c = None

        cache=my_cache

        cached=True

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[A %s]" % repr(self.c)

    b=BAnalysis(use_c=1).get()
    assert b.cached
    assert b._da_locally_complete

    da.reset()

    class BAnalysis(da.DataAnalysis):
        c = None

        cached=True

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[B %s]" % repr(self.c)

    class CAnalysis(da.DataAnalysis):
        c = None

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[C %s]" % repr(self.c)


    class AAnalysis(da.DataAnalysis):
        run_for_hashe=True
        allow_alias=True

        def main(self):
            print(("test", self.__class__))
            r = [[BAnalysis(use_c=c),CAnalysis(use_c=c)] for c in range(3)]
            return r



    A = AAnalysis()
    r = A.get()

    print(r)
    print((r.output))

    for b,c in r.output:
        print((b,b.data,b._da_locally_complete))
        print((c, c.data,c._da_locally_complete))
        print()

    print((r.output[1][0].c, r.output[1][0].data))

    assert isinstance(r.output[0][0], BAnalysis)
    assert r.output[0][0].cached
    assert r.output[0][0]._da_locally_complete
    assert r.output[0][0].data == 'data 0'

    assert isinstance(r.output[0][1], CAnalysis)
    assert not r.output[0][1].cached
    assert r.output[0][1]._da_locally_complete
    assert r.output[1][1].data == 'data 1'
    #assert r[0]


def test_generate_factory_assumptions_leak():
    debug_output()
    da.reset()

    class BAnalysis(da.DataAnalysis):
        c = None

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[B %s]" % repr(self.c)

    class AAnalysis(da.DataAnalysis):
        input_b=BAnalysis

        # run_for_hashe=True
        def main(self):
            print(("test", self.__class__))
            self.data="A,b:"+self.input_b.data

        def __repr__(self):
            return "[A %s]" % repr(self.input_b)

    class CAnalysis(da.DataAnalysis):
        input_a=AAnalysis

    da.AnalysisFactory.WhatIfCopy("test",AAnalysis(input_b=BAnalysis,use_version="v2"))

#    assert da.AnalysisFactory.cache_assumptions is None

    C = CAnalysis(assume=BAnalysis(use_c=1))
    C.get()
    assert C._da_locally_complete
    assert C.input_a.data=="A,b:data 1"

    C = CAnalysis(assume=BAnalysis(use_c=2))
    C.get()
    assert C._da_locally_complete
    assert C.input_a.data=="A,b:data 2"


def test_generate_factory_assumptions_references():
    debug_output()
    da.reset()

    class BAnalysis(da.DataAnalysis):
        c = None

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[B %s]" % repr(self.c)

    class AAnalysis(da.DataAnalysis):
        input_b=None

        # run_for_hashe=True
        def main(self):
            print(("test", self.__class__))
            self.data="A,b:"+self.input_b.data

        def __repr__(self):
            return "[A %s]" % repr(self.input_b)

    class CAnalysis(da.DataAnalysis):
        input_a=AAnalysis

    da.AnalysisFactory.WhatIfCopy("test",AAnalysis(input_b=BAnalysis,use_version="v2"))

    print((da.AnalysisFactory.cache_assumptions))
    assert len(da.AnalysisFactory.cache_assumptions)==1

    C = CAnalysis(assume=BAnalysis(use_c=1))
    C.get()
    assert C._da_locally_complete
    assert C.input_a.data=="A,b:data 1"

    C = CAnalysis(assume=BAnalysis(use_c=2))
    C.get()
    assert C._da_locally_complete
    assert C.input_a.data=="A,b:data 2"


def test_runtimenamed():
    debug_output()
    da.reset()

    class BAnalysis(da.DataAnalysis):
        c = 1

        _da_settings = ["c"]

        def main(self):
            print(("test", self.c))
            self.data = "data " + repr(self.c)

        def __repr__(self):
            return "[B %s]" % repr(self.c)

    class AAnalysis(da.DataAnalysis):
        input_b = da.NamedAnalysis("BAnalysis")

        # run_for_hashe=True
        def main(self):
            print(("test", self.__class__))
            self.data = "A,b:" + self.input_b.data

        def __repr__(self):
            return "[A %s]" % repr(self.input_b)

    class CAnalysis(da.DataAnalysis):
        input_a = AAnalysis

    C = CAnalysis(assume=BAnalysis(use_c=1))
    C.get()
    assert C._da_locally_complete
    assert C.input_a.data == "A,b:data 1"
