

def test_create_datanode():
    from dataanalysis import core as da
    da.reset()
    #da.debug_output()

    data=dict(
        data="x",
        dataint=2,
    )

    data2 = dict(
        data="xy",
        dataint=3,
    )

    A=da.DataAnalysis.from_data("TestAnalysis",data).get()

    Aa = da.DataAnalysis.from_data("aTestAnalysis", data).get()
    B=da.DataAnalysis.from_data("TestAnalysis",data2,input_a=Aa,use_b=2).get()

    print(A)
    print(B)

    assert A.data == data['data']
    assert B.data == data2['data']
    assert B.b == 2

    C=da.byname("TestAnalysis").get()

    assert C.data == B.data

    print((C._da_locally_complete))

    aB=B.__class__().get()
    assert aB.export_data() == B.export_data()
    print((B.export_data()))

    assert aB._da_locally_complete == B._da_locally_complete
    print((B._da_locally_complete))

