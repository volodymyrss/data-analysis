
def test_basic():
    import dataanalysis.core as da
    import dataanalysis.importing as importing

    da.reset()

    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    m=importing.load_by_name(['filesystem','moduletest',dir_path+"/ddmoduletest.py"])

    assert not hasattr(m[0], '__dda_module_global_name__')
    assert not hasattr(m[0], '__dda_module_origin__')

def test_git_list():
    import os
    import dataanalysis.core as da
    import dataanalysis.importing as importing

    da.reset()

    m=importing.load_by_name("git://test",local_gitroot=os.environ.get("TMPDIR","/tmp")+"/git/",remote_git_root='volodymyrss-public')

    assert hasattr(m[0], '__dda_module_global_name__')
    assert hasattr(m[0], '__dda_module_origin__')

    assert m[0].__dda_module_origin__=="git"
    assert m[0].__dda_module_global_name__ == "git://test"

def test_nested_modules():
    pass