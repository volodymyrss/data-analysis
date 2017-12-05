def test_simple():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    A=ddmoduletest.AAnalysis()
    A.get()

    ident=A.get_identity()

    da.reset()

    import dataanalysis.emerge as emerge

    eA=emerge.emerge_from_identity(ident)
    eA.get()

    assert A.data == eA.data


def test_simple_chain():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    B=ddmoduletest.BAnalysis()
    B.get()

    ident=B.get_identity()

    da.reset()

    import dataanalysis.emerge as emerge

    eB=emerge.emerge_from_identity(ident)
    eB.get()

    assert B.data == eB.data

def test_simple_assumption():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    B=ddmoduletest.BAnalysis(assume=[ddmoduletest.AAnalysis(use_assumed_data="_assumedthis")])
    B.get()

    ident=B.get_identity()

    assert len(ident.assumptions)==1

    import dataanalysis.emerge as emerge
    da.reset()

    eB=emerge.emerge_from_identity(ident)
    eB.get()

    assert B.data == eB.data

def test_serialize():
    import dataanalysis.core as da
    da.reset()
    da.debug_output()

    import ddmoduletest
    reload(ddmoduletest)
    S = ddmoduletest.SAnalysis(input_s="inputassumedthis").get()

    ed=S.export_data()

    assert '_da_stored_string_input_s' in ed


def test_input_assumption():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    C=ddmoduletest.SAAnalysis(assume=[ddmoduletest.SAnalysis(input_s="inputassumedthis")])
    C.get()

    ident=C.get_identity()

    assert len(ident.assumptions)==1

    print "assumption",ident.assumptions[0]

    import dataanalysis.emerge as emerge
    da.reset()

    eC=emerge.emerge_from_identity(ident)
    eC.get()

    assert C.data == eC.data