

import os
import imp

def test_hashe_stability():
    c=('analysis', ('analysis', ('analysis', ('analysis', 'None', 'AnyAnalysis.v0'), 'BAnalysis.v0'), 'CAnalysis.v0'), 'DAnalysis.v0')
    from dataanalysis import hashtools as ht

    print((c,ht.shhash(c)))


def test_clone():
    import dataanalysis.core as da
    import dataanalysis.graphtools as gt
    da.reset()
    imp.reload(gt)

    class AAnalysis(da.DataAnalysis):
        def main(self):
            self.a=1

    aa=AAnalysis(use_b=2).get()
    aa.c=3

    ab=aa.clone().get()

    assert 'a' in aa.export_data()
    assert 'b' in aa.export_data()
    assert 'c' in aa.export_data()

    assert aa is not ab

    aakeys = sorted(list(aa.export_data().keys()))
    abkeys = sorted(list(ab.export_data().keys()))
    print(("aakeys",aakeys))
    print(("abkeys",abkeys))

    assert aakeys == abkeys


def test_factorize():
    import dataanalysis.core as da
    import dataanalysis.graphtools as gt
    da.reset()
    imp.reload(gt)
    
    da.debug_output()


    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        input_a=AAnalysis

    class CAnalysis(da.DataAnalysis):
        input_b=BAnalysis

    class DAnalysis(da.DataAnalysis):
        input_c=CAnalysis

    c=DAnalysis().get()
    print(("got:",c._da_locally_complete))

    fct=gt.Factorize(use_root='DAnalysis',use_leaves=['BAnalysis'])

    fct=fct.get()

    print(fct)

    assert isinstance(fct, da.DataHandle)

    
    print(("got this",fct.str()))
    assert fct.str() == "Factorize.v0.Factor_DAnalysis.By_BAnalysis.processing_definition.8609173b"


def test_factorize_run_for_hashe_analysis():
    import dataanalysis.core as da
    import dataanalysis.graphtools as gt
    da.reset()
    imp.reload(gt)

    class AAnalysis(da.DataAnalysis):
        pass

    class BAnalysis(da.DataAnalysis):
        run_for_hashe=True
        input_a=AAnalysis

    class CAnalysis(da.DataAnalysis):
        input_b=BAnalysis

    class DAnalysis(da.DataAnalysis):
        input_c=CAnalysis

    c=DAnalysis().get()
    print(("got:",c._da_locally_complete))

    fct=gt.Factorize(use_root='DAnalysis',use_leaves=['BAnalysis'])

    fct=fct.get()

    print(fct)

    assert isinstance(fct, da.DataHandle)
    assert fct.str() == "Factorize.v0.Factor_DAnalysis.By_BAnalysis.processing_definition.8609173b"


def test_factorize_note():

    import dataanalysis.core as da
    da.reset()

    da.AnalysisFactory.note_factorization(dict(a=1,b=2,c=[1,2,{1:2}]))

    class AAnalysis(da.DataAnalysis):
        read_caches=[]
        cached=True

    A=AAnalysis().get()

    fn=A._da_cached_path+"/factorizations.txt"
    assert os.path.exists(fn)

    print((">>>>\n"+open(fn).read()+"<<<<"))
