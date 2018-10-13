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

    print("assumption",ident.assumptions[0])

    import dataanalysis.emerge as emerge
    da.reset()

    eC=emerge.emerge_from_identity(ident)
    eC.get()

    assert C.data == eC.data

def test_input_assumption_version():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    C=ddmoduletest.BAnalysis(assume=[ddmoduletest.AAnalysis(use_version="xxx_version")])
    C.get()

    print C.data

    ident=C.get_identity()

    assert len(ident.assumptions)==1

    print "assumption",ident.assumptions[0]
    print "assumption data",ident.assumptions[0][1]['assumed_data'],type(ident.assumptions[0][1]['assumed_data'])

    import dataanalysis.emerge as emerge
    da.reset()

    #print ident.assumptions[]

    eC=emerge.emerge_from_identity(ident)
    eC.get()

    assert C.data == eC.data

    print C._da_locally_complete
    assert C._da_locally_complete[1][2]=='AAnalysis.xxx_version'

def test_used_modules_stacking():
    import dataanalysis.core as da
    da.reset()

    import ddmoduletest
    reload(ddmoduletest)
    import ddmoduletest2
    reload(ddmoduletest)
    reload(ddmoduletest2)
    reload(ddmoduletest)
    reload(ddmoduletest2)

    print()
    for i,m in enumerate(da.AnalysisFactory.dda_modules_used):
        print(i,m)

    assert len(da.AnalysisFactory.dda_modules_used) == 2

    C=ddmoduletest.AAnalysis()
    C.get()


    ident=C.get_identity()

    print("identity",ident)

    import dataanalysis.emerge as emerge
    da.reset()

    eC=emerge.emerge_from_identity(ident)
    eC.get()

    assert C.data == eC.data

def test_destacking_tool():
    input=[
        1,
        2,
        3,
        1,
        2,
        1,
        2,
        3,
        5,
        5,
    ]
    from dataanalysis.hashtools import remove_repeating_stacks
    output=remove_repeating_stacks(input)

    assert output==[
        1,
        2,
        3,
        1,
        2,
        3,
        5,
    ]