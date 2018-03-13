import time

from dataanalysis import core as da
from dataanalysis.caches.cache_core import CacheNoIndex
from dataanalysis.caches.resources import CacheDelegateToResources, WebResourceFactory


class D2_AAnalysis(da.DataAnalysis):
    cached=True
    #cache=cache

    assumed_data=None

    def main(self):
        self.data="dataA"
        if self.assumed_data is not None:
            self.data += self.assumed_data

