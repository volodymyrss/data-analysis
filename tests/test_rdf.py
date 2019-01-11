def test_simple_ontology():
    from dataanalysis import core as da
    from dataanalysis import rdf as dardf
    da.reset()

    class BAnalysis(da.DataAnalysis):
        def main(self):
            print("test")
            self.data = "data"

    class Analysis(da.DataAnalysis):
        input_b = BAnalysis

        def main(self):
            print("test")
            self.data = self.input_b.data


