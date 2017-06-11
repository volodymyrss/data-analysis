

def test_import():
    import dataanalysis as da

def test_one_object():
    import dataanalysis as da
    
    class Analysis(da.DataAnalysis):
        def main(self):
            print "test"
            self.data="data"
    A=Analysis()
    A.get()
        
    assert A.data == 'data'
    
def test_two_object():
    import dataanalysis as da

    class BAnalysis(da.DataAnalysis):
        def main(self):
            print "test"
            self.data="data"
    
    class Analysis(da.DataAnalysis):
        input_b=BAnalysis

        def main(self):
            print "test"
            self.data=self.input_b.data

    A=Analysis()
    A.get()
        
    assert A.data == 'data'
    
def test_optional_object():
    try:
        import dataanalysis as da
        da.AnalysisFactory.reset()
       # da.printhook.global_all_output=True
       # da.printhook.global_permissive_output=True
       # da.printhook.LogStream(None,lambda x:True)


        class aAnalysis(da.DataAnalysis):
            def main(self):
                print "test"
                self.data="data"
        
        class bAnalysis(da.DataAnalysis):
            produce_disabled=True
            def main(self):
                print "test"
                self.data="data"
        
        aA=aAnalysis()
        aA.get()
        
        class gAnalysis(da.DataAnalysis):
            input_a=aAnalysis
            input_b=bAnalysis
    
            force_complete_input=False

            def main(self):
                print "test"
                self.data=self.input_a.data


        A=gAnalysis()
        A.get()
            
        assert A.data == 'data'
        assert A.input_a.incomplete == False
        assert A.input_b.incomplete == True
        
    except:
        da.printhook.global_all_output=False
        da.printhook.global_permissive_output=False
        da.printhook.LogStreams=[]
        raise
    
def test_caching():
    import dataanalysis as da
    import time
    
    class Analysis(da.DataAnalysis):
        cached=True

        def main(self):
            print "test"
            #time.sleep(3)
            self.data="data"

    A=Analysis()
    A.get()

    reload(da)

    print A
    print "construting again.."
    
    class Analysis(da.DataAnalysis):
        cached=True

        def main(self):
            raise

    A1=Analysis()
    print A1

    t0=time.time()
    A1.get()
    tspent=time.time()-t0
        
    assert tspent<1
    assert A1.data == 'data'

