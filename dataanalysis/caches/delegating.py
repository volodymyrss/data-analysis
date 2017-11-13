import re
import time

from persistqueue import Queue, Empty

import dataanalysis.caches
import dataanalysis.caches.cache_core
import dataanalysis.core as da
from dataanalysis.printhook import log


class DelegatedNoticeException(da.AnalysisDelegatedException):
    pass

class WaitingForDependency(da.AnalysisDelegatedException):
    pass

class DelegatingCache(dataanalysis.caches.cache_core.Cache):
    def delegate(self, hashe, obj):
        pass

    def restore(self,hashe,obj,restore_config=None):
        self.delegate(hashe, obj)
        raise da.AnalysisDelegatedException(hashe)

class SelectivelyDelegatingCache(DelegatingCache):
    delegating_analysis=None

    def will_delegate(self,hashe,obj=None):
        log("trying for delegation",hashe)

        if self.delegating_analysis is None:
            log("this cache has no delegations allowed")
            return False

        if any([hashe[-1] == option or re.match(option,hashe[-1]) for option in self.delegating_analysis]):
            log("delegation IS allowed")
            return True
        else:
            log("failed to find:",hashe[-1],self.delegating_analysis)
            return False

    def restore(self,hashe,obj,restore_config=None):
        if self.will_delegate(hashe, obj):
            super(SelectivelyDelegatingCache, self).restore(hashe, obj, restore_config)
        else:
            return self.restore_from_parent(hashe, obj, restore_config)

# execution is also from cache?..

# no traces required in the network ( traces are kept for information )
# callbacks

class ResourceProvider(object):
    def find_resource(self,hashe,modules,assumptions):
        pass


class CacheDelegateToResources(SelectivelyDelegatingCache):
    def delegate(self, hashe, obj):
        # here we attempt to find the resource provider


        raise WaitingForDependency(hashe)

class QueueCache(DelegatingCache):

    def __init__(self,queue_file="/tmp/queue"):
        super(QueueCache, self).__init__()
        self.queue_file=queue_file
        self.queue = Queue(self.queue_file)

    def delegate(self, hashe, obj):
        self.queue.put([obj.get_factory_name(),dataanalysis.core.AnalysisFactory.get_module_description(),hashe])

    def wipe_queue(self):
        while True:
            try:
                item=self.queue.get(block=False)
            except Empty:
                break
            self.queue.task_done()


class QueueCacheWorker(object):
    def __init__(self,queue_file="/tmp/queue"):
        self.queue_file = queue_file
        self.queue = Queue(self.queue_file)

    def run_once(self):
        object_name,hashe,modules=self.queue.get(block=False)
        print("object name",object_name)
        print("modules",modules)
        print("hashe",hashe)

        A=dataanalysis.core.AnalysisFactory[object_name]

        return A.get()

    def run_all(self,burst=True):
        while True:
            print(time.time())

            try:
                item=self.queue.get(block=False)
            except Empty:
                break

            hashe, modules = item
            print(hashe)
            print(modules)
            self.queue.task_done()




