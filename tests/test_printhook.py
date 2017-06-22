import dataanalysis

def test_print():
    print "permissive:",dataanalysis.printhook.global_permissive_output

    import StringIO,sys
    pipe=StringIO.StringIO()

    raw_stdout=sys.stdout
    sys.raw_stdout=raw_stdout
    sys.stdout=pipe
    pipe.__repr__=lambda :"[intercepted pipe]"

    dataanalysis.core.debug_output()

    assert dataanalysis.printhook.LogStreams[0].target == pipe

    class Analysis(dataanalysis.core.DataAnalysis):
        def main(self):
            print "testoutput\n"*100

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
    #print pipe.getvalue()

    assert 'running main' in pipe.getvalue()

    assert 'testoutput' in pipe.getvalue()

    assert 'testoutput' in A._da_main_log_content