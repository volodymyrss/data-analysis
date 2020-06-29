
def test_context():
    from dataanalysis import core as da
    from dataanalysis import context

    class RAnalysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data="data-r"

    class AAnalysis(da.DataAnalysis):
        input_b=RAnalysis

        data_class="aclassdata"

        def main(self):
            print("test")
            self.data=self.input_b.data+".a"

    class BAnalysis(da.DataAnalysis):
        input_b=RAnalysis

        def main(self):
            print("test")
            self.data=self.input_b.data+".b"

    class CAnalysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data=self.input_b.data+".c"

    AAnalysis().get()
    BAnalysis().get()


    ctx=context.InContext(input_root=RAnalysis,input_leaves=[AAnalysis,BAnalysis]).get()
    #ctx = context.InContext(use_root='RAnalysis', use_leaves=['AAnalysis', 'BAnalysis']).get()

    print((ctx.data))

    assert 'RAnalysis' in ctx.data
    assert 'AAnalysis' in ctx.data['RAnalysis']
    assert 'BAnalysis' in ctx.data['RAnalysis']
    assert 'CAnalysis' not in ctx.data['RAnalysis']

    assert ctx.data['RAnalysis']['AAnalysis']['data']=='data-r.a'
    assert ctx.data['RAnalysis']['AAnalysis']['data_class'] == 'aclassdata'
    assert ctx.data['RAnalysis']['BAnalysis']['data'] == 'data-r.b'

    #assert A.data == 'data'

