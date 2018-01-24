import time

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

    sleep=None

    def get_version(self):
        v=da.DataAnalysis.get_version(self)
        if self.sleep is not None:
            v+="sleep.%i"%self.sleep
        return v

    def main(self):
        self.data = "dataA"
        if self.sleep is not None:
            time.sleep(self.sleep)

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

class SAnalysis(da.DataAnalysis):
    input_s=None

    cached=False

    def main(self):
        self.data="dataS_"+self.input_s.str()


class SAAnalysis(da.DataAnalysis):
    input_sa = SAnalysis

    cached = False

    def main(self):
        self.data = "dataSA_" + self.input_sa.data


class AAnalysis(da.DataAnalysis):
    cached=True
    cache=cache

    assumed_data=None

    def main(self):
        self.data="dataA"
        if self.assumed_data is not None:
            self.data += self.assumed_data

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
        self.data=self.input_a.data+".x"


class ChainedServerProducer(da.DataAnalysis):
    cached = True
    cache = cache

    input_a = AAnalysis
    input_b = BAnalysis

    def main(self):
        self.data=dict(a=self.input_a.data,b=self.input_b.data)
        self.resource_stats_a = self.input_a.resource_stats
        self.resource_stats_b = self.input_b.resource_stats


class GenerativeAnalysis(da.DataAnalysis):
    def main(self):
        self.data=AAnalysis(use_assumed_data="data1"), BAnalysis(use_assumed_data="data2")
        print("cache of generated:",self.data[0].cache)