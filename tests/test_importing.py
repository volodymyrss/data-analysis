def test_basic():
    import dataanalysis.core as da
    import dataanalysis.importing as importing

    da.reset()

    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    m=importing.load_by_name(['filesystem','moduletest',dir_path+"/ddmoduletest.py"])

    assert not hasattr(m[0], '__dda_module_global_name__')
    assert not hasattr(m[0], '__dda_module_origin__')

def test_git():
    import dataanalysis.core as da
    import dataanalysis.importing as importing

    da.reset()

    m=importing.load_by_name("git://ddosa")

    assert hasattr(m[0], '__dda_module_global_name__')
    assert hasattr(m[0], '__dda_module_origin__')
