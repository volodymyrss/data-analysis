import unittest


class TestStringMethods(unittest.TestCase): 
    def test_import(self):
        import dataanalysis as da

    def test_one_object(self):
        import dataanalysis as da
        
        class Analysis(da.DataAnalysis):
            def main(self):
                print "test"
                self.data="data"
        A=Analysis()
        A.get()
            
        self.assertEqual(A.data, 'data')
    
    def test_two_object(self):
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
            
        self.assertEqual(A.data, 'data')
    
    def test_caching(self):
        import dataanalysis as da
        import time
        
        class Analysis(da.DataAnalysis):
            cached=True

            def main(self):
                print "test"
                time.sleep(3)
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
            
        self.assertTrue(tspent<1)
        self.assertEqual(A1.data,'data')


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)

