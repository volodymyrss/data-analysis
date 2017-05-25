import unittest
import os


class TestDDOSA(unittest.TestCase): 
    def test_rundda(self):
        os.system("rundda.py ii_skyimage -c -m ddosa  -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")'")
    
    def test_rundda_fcache(self):
        os.system("rundda.py ii_skyimage -c -m ddosa  -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")' -F ii_skyimage")
    
    #def test_rundda_import(self):
    #    os.system("rundda.py ii_skyimage -c -m ddosa -m /imagebinsstd4/v1 -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")'")
    
    def test_rundda_writecache(self):
        os.system("INTEGRAL_DDCACHE_ROOT=./ddcache rundda.py ii_skyimage -c -m ddosa -m git://onlybright  -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")'")
        import pyfits
        d=pyfits.open("./ddcache/byscw/0665/066500120010.001/ii_skyimage.v2/4ada4c34/isgri_sky_res.fits.gz")[2].data
        self.assertTrue((d[0]['FLUX']-185.239)**2<0.01)



if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDDOSA)
    unittest.TextTestRunner(verbosity=5).run(suite)

