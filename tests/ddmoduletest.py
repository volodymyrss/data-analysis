from dataanalysis import core as da
from dataanalysis.caches.cache_core import CacheNoIndex
from dataanalysis.caches.resources import CacheDelegateToResources, WebResourceFactory


class LocalResourceFactory(WebResourceFactory):
    host="localhost"
    port=6767
    api_version = "v0"

local_resource_factory = LocalResourceFactory()

class TestCache(CacheDelegateToResources):
    delegating_analysis = ["ServerDelegatableAnalysis.*"]
    resource_factory = local_resource_factory

cache=TestCache()
server_local_cache=CacheNoIndex()

class ClientDelegatableAnalysisA(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        self.data = "dataA"

class ClientDelegatableAnalysisA1(da.DataAnalysis):
    cached=True
    cache=cache

    def main(self):
        self.data = "dataA1"

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


class ServerCachableAnalysis(da.DataAnalysis):
    cached = True
    cache = server_local_cache

    version="v2"

    input_a = ClientDelegatableAnalysisA
    input_b = ClientDelegatableAnalysisB

    def main(self):
        self.data=self.input_a.data+self.input_b.data



class ChainedDelegator(da.DataAnalysis):
    cached = True
    cache = cache

    input_a = ServerDelegatableAnalysisA

    def main(self):
        pass