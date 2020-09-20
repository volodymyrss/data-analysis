

def test_import():
    from dataanalysis import core as da
    print((da.__file__))


def test_one_object():
    from dataanalysis import core as da

    class Analysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data="data"
    A=Analysis()
    A.get()
        
    assert A.data == 'data'
    
def test_two_object():
    from dataanalysis import core as da

    class BAnalysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data="data"
    
    class Analysis(da.DataAnalysis):
        input_b=BAnalysis

        def main(self):
            print("test")
            self.data=self.input_b.data

    A=Analysis()
    A.get()
        
    assert A.data == 'data'


def test_constructor_input():
    from dataanalysis import analysisfactory
    from dataanalysis import core as da
    da.reset()

    class BAnalysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data="data"

    class Analysis(da.DataAnalysis):
        #input_b=BAnalysis

        def main(self):
            print("test")
            if hasattr(self,'input_b'):
                self.data = self.input_b.data
            else:
                self.data = 'otherdata'

    A=Analysis()
    A.get()

    assert A.data == 'otherdata'

    A=Analysis(input_b=BAnalysis())

    assert hasattr(A,'input_b')

    da.debug_output()
    print(("*" * 80))
    A.promote()
    print(("/" * 80))

    A_fn=analysisfactory.AnalysisFactory.get_by_name(A.get_signature()).get()
    print((A, A_fn))
    assert A == A_fn

    A_f = analysisfactory.AnalysisFactory.get(A).get()

    print((A,A_f))
    assert A == A_f

    A.get()

    assert A.data == 'data'

    inputs=A.list_inputs()
    assert isinstance(inputs,list)
    assert len(inputs)==1

def test_transient_cache():
    from dataanalysis import core as da

    da.reset()

    class Analysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data = "data"

    A=Analysis()

    A.get()
    assert A.data == "data"

    assert A._da_locally_complete is not None
    assert isinstance(A._da_locally_complete,tuple)

    B=Analysis()

    assert A._da_locally_complete in da.TransientCacheInstance.cache
    data=da.TransientCacheInstance.cache[A._da_locally_complete]

    assert  data['data']=='data'




def test_optional_object():
    try:
        from dataanalysis import core as da
        from dataanalysis import analysisfactory

        da.AnalysisFactory.reset()
       # da.printhook.global_all_output=True
       # da.printhook.global_permissive_output=True
       # da.printhook.LogStream(None,lambda x:True)


        class aAnalysis(da.DataAnalysis):
            def main(self):
                print("test")
                self.data="data"
        
        class bAnalysis(da.DataAnalysis):
            produce_disabled=True
            def main(self):
                print("test")
                self.data="data"
        
        aA=aAnalysis()
        aA.get()


        print((analysisfactory.AnalysisFactory.get("aAnalysis"), aA))
        #assert analysisfactory.AnalysisFactory.get("aAnalysis") == aA

        class gAnalysis(da.DataAnalysis):
            input_a=aAnalysis
            input_b=bAnalysis
    
            force_complete_input=False

            def main(self):
                print("test")
                self.data=self.input_a.data


        A=gAnalysis()
        A.get()
            
        assert A.data == 'data'
        assert A.input_a.incomplete == False
        assert A.input_b.incomplete == True
        
    except:
        raise
    
def test_caching():
    from dataanalysis import core as da
    import time

    da.debug_output()
    da.reset()

    class Analysis(da.DataAnalysis):
        cached=True

        read_caches = []

        def main(self):
            print("test")
            #time.sleep(3)
            self.data="data"

    A=Analysis()
    A.get()

    da.reset()

    print(A)
    print("constructing again..")
    
    class Analysis(da.DataAnalysis):
        cached=True

        def main(self):
            raise RuntimeError("this should have been cached")

    A1=Analysis()
    print(A1)

    t0=time.time()
    A1.get()
    tspent=time.time()-t0
        
    assert tspent<1
    assert hasattr(A1,'data')
    print((A1.data))
    assert A1.data == 'data'


def test_factory():
    from dataanalysis import core as da

    class Analysis(da.DataAnalysis):
        x=None

        def main(self):
            print("test")
            self.data = "data"
            if self.x is not None:
                self.data+=repr(self.x)


    A = Analysis()
    A.get()

    assert A.factory.cache_assumptions == []

    A.factory.WhatIfCopy("testass",Analysis(use_x=1))

    assert len(A.factory.cache_assumptions) == 1

    iden=A.get_identity()
    print((iden.assumptions))



