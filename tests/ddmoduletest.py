from dataanalysis import core as da
from dataanalysis.caches.resources import CacheDelegateToResources, WebResourceFactory

class LocalResourceFactory(WebResourceFactory):
    host="localhost"
    port=6767
    api_version = "v0"

class TestCache(CacheDelegateToResources):
    #delegating_analysis = ["AAnalysis"]
    resource_factory = LocalResourceFactory

cache=TestCache()

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
