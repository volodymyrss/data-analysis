def test_basic():
    import dataanalysis.core as da
    import dataanalysis.importing as importing

    da.reset()

    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    importing.load_by_name(['filesystem','moduletest',dir_path+"/ddmoduletest.py"])
