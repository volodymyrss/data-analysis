import dataanalysis

def test_print():
    print(("permissive:",dataanalysis.printhook.global_permissive_output))

    import sys

    try:
        from io import StringIO
    except ImportError:
        from io import StringIO

    pipe=StringIO()

    raw_stdout=sys.stdout
    sys.raw_stdout=raw_stdout
    sys.stdout=pipe
    pipe.__repr__=lambda :"[intercepted pipe]"

    dataanalysis.core.debug_output()

    assert dataanalysis.printhook.LogStreams[0].target == pipe

    class Analysis(dataanalysis.core.DataAnalysis):
        def main(self):
            print(("testoutput\n"*100))

            for ls in dataanalysis.printhook.LogStreams:
                raw_stdout.write("\n- logstream now:"+repr(ls))

            assert dataanalysis.printhook.LogStreams[0].target == pipe

            assert sys.stdout.get_origOut() == pipe

            raw_stdout.write(repr(sys.stdout))
            raw_stdout.write("\norig:"+repr(sys.stdout.get_origOut()))
            #orig_stdout.write(repr(sys.stdout) + "\n" + ("testoutput" * 100))
            #sys.stdout.write(repr(sys.stdout) + "\n" + ("testoutput" * 100))


    A=Analysis()

    A.get()

    sys.stdout=raw_stdout
    #print(pipe.getvalue())

    assert 'running main' in pipe.getvalue()

    assert 'testoutput' in pipe.getvalue()

    assert 'testoutput' in A._da_main_log_content


def test_standard_output():
    import sys
    try:
        from io import StringIO
    except ImportError:
        from io import StringIO

    pipe=StringIO()

    raw_stdout=sys.stdout
    sys.raw_stdout=raw_stdout
    sys.stdout=pipe
    pipe.name="piped for test"

    dataanalysis.core.reset()
    dataanalysis.printhook.reset()

    for ls in dataanalysis.printhook.LogStreams:
        raw_stdout.write("+ logstream now:" + repr(ls) + "\n")

    class Analysis(dataanalysis.core.DataAnalysis):
        def main(self):
            print(("testoutput\n"*10))
            raw_stdout.write("\n")
            for ls in dataanalysis.printhook.LogStreams:
                raw_stdout.write("- logstream now:"+repr(ls)+"\n")


    A=Analysis()

    A.get()

    sys.stdout=raw_stdout
    #print(pipe.getvalue())

#    assert 'running main' not in pipe.getvalue()

    print(("A._da_main_log_content:",A._da_main_log_content))

    assert 'testoutput' in A._da_main_log_content

    assert 'testoutput' in pipe.getvalue()

