from dataanalysis import core as da
from dataanalysis.caches.delegating import SelectivelyDelegatingCache


class TestCache(SelectivelyDelegatingCache):
    pass

class AAnalysis(da.DataAnalysis):
    def main(self):
        self.data="dataA"

class BAnalysis(da.DataAnalysis):
    input_a = AAnalysis

    def main(self):
        self.data="dataB"+self.input_a.data

class CAnalysis(da.DataAnalysis):
    input_a = BAnalysis

    def main(self):
        self.data="dataB"+self.input_a.data
