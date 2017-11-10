from dataanalysis import core as da

class AAnalysis(da.DataAnalysis):
    def main(self):
        self.data="dataA"

class BAnalysis(da.DataAnalysis):
    input_a = AAnalysis

    def main(self):
        self.data="dataB"+self.input_a.data

