from dataanalysis import core as da
from dataanalysis.caches.resources import CacheDelegateToResources, WebResourceFactory

class LocalResourceFactory(WebResourceFactory):
    host="localhost"
    port=6767
    api_version = "v0"

class TestCache(CacheDelegateToResources):
    delegating_analysis = ["ServerDelegatableAnalysis.*"]
    resource_factory = LocalResourceFactory

cache=TestCache()

class ClientDelegatableAnalysisA(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        self.data = "dataA"

class ClientDelegatableAnalysisB(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        self.data = "dataB"

class ServerDelegatableAnalysisA(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        pass


class AAnalysis(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        self.data="dataA"

class BAnalysis(da.DataAnalysis):
    cached=True
    cache=cache

    input_a = AAnalysis

    def main(self):
        self.data="dataB"+self.input_a.data

class CAnalysis(da.DataAnalysis):
    cached=True
    cache=cache

    input_a = BAnalysis

    def main(self):
        self.data="dataB"+self.input_a.data

class TwoCDInputAnalysis(da.DataAnalysis):
    cached = True
    cache = cache

    input_a = ClientDelegatableAnalysisA
    input_b = ClientDelegatableAnalysisB

    def main(self):
        pass
