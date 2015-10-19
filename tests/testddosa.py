import unittest
import os


class TestDDOSA(unittest.TestCase): 
    def test_rundda(self):
        os.system("rundda.py ii_skyimage -v -m ddosa  -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")'")
    
    def test_rundda_import(self):
        os.system("rundda.py ii_skyimage -v -m ddosa -m /imagebinsstd4/v1 -a 'ddosa.ScWData(input_scwid=\"066500120010.001\")'")


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDDABasic)
    unittest.TextTestRunner(verbosity=5).run(suite)

