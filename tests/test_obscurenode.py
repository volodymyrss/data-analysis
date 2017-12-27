def test_obscure_node():
    from dataanalysis import core as da
    da.reset()
    da.debug_output()

    class BAnalysis(da.DataAnalysis):
        data="1"

    class AAnalysis(da.DataAnalysis):
        input_b=BAnalysis

        cached = True

        def main(self):
            self.data="a."+self.input_b.data

    A=AAnalysis().get()

    data=A.export_data()
    hashe=A._da_locally_complete

    cached_path=A.cache.construct_cached_file_path(A._da_locally_complete, A)

    import os
    assert os.path.exists(cached_path)
    print(cached_path)

    da.reset()

    nA=da.DataAnalysis.from_hashe_and_data(hashe, data).get()
    print(nA)

    assert nA.export_data()['data'] == data['data']
    assert nA._da_locally_complete == hashe


def test_obscure_node_cached():
    from dataanalysis import core as da
    da.reset()
    da.debug_output()

    class BAnalysis(da.DataAnalysis):
        data="1"

    class AAnalysis(da.DataAnalysis):
        input_b=BAnalysis

        cached = True

        def main(self):
            self.data="a."+self.input_b.data

    A=AAnalysis().get()

    data=A.export_data()
    hashe=A._da_locally_complete

    cached_path=A.cache.construct_cached_file_path(A._da_locally_complete, A)

    import os
    assert os.path.exists(cached_path)
    print(cached_path)

    da.reset()

    nA=da.DataAnalysis.from_hashe(hashe).get()
    print(nA)

    assert nA.export_data()['data'] == data['data']
    assert nA._da_locally_complete == hashe

def test_obscure_assumptions():
    from dataanalysis import core as da
    da.reset()
    da.debug_output()

    class BAnalysis(da.DataAnalysis):
        data="1"

    class AAnalysis(da.DataAnalysis):
        input_b=BAnalysis

        cached = True

        def main(self):
            self.data="a."+self.input_b.data

    A=AAnalysis().get()

    data=A.export_data()
    hashe=A._da_locally_complete

    cached_path=A.cache.construct_cached_file_path(A._da_locally_complete, A)

    import os
    assert os.path.exists(cached_path)
    print(cached_path)

    da.reset()

    nA=da.DataAnalysis.from_hashe_and_data(hashe, data).get()
    print(nA)

    assert nA.export_data()['data'] == data['data']
    assert nA._da_locally_complete == hashe
