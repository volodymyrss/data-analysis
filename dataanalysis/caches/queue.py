import time

from persistqueue import Queue, Empty

import dataanalysis
import dataanalysis.core as da
import dataanalysis.importing as da_importing
from dataanalysis.caches.delegating import DelegatingCache
from dataanalysis.printhook import log


class QueueCache(DelegatingCache):

    def __init__(self,queue_file="/tmp/queue"):
        super(QueueCache, self).__init__()
        self.queue_file=queue_file
        self.queue = Queue(self.queue_file)

    def delegate(self, hashe, obj):


        self.queue.put(dict(
            object_identity=obj.get_identity(),
            request_origin="undefined",
        ))

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

    def run_task(self,object_identity):
        da.reset()

        print(object_identity)

        for module in object_identity.modules:
            log("importing",module)
            da_importing.load_by_name(module)

        A=dataanalysis.core.AnalysisFactory[object_identity.factory_name]

        expectable_hashe=A.get_hashe()

        if len(object_identity.assumptions) > 0:
            assumptions = ",".join([a[0] for a in object_identity.assumptions])
            log(assumptions)
            da.AnalysisFactory.WhatIfCopy('commandline', eval(assumptions))

        if expectable_hashe != object_identity.expected_hashe:
            raise Exception("unable to produce\n"+repr(object_identity.expected_hashe)+"\n while can produce"+repr(expectable_hashe))

        return A.get()


    def run_once(self):
        object_identity=self.queue.get(block=False)['object_identity']

        print("object identity",object_identity)

        self.run_task(object_identity)
        self.queue.task_done()

    def run_all(self,burst=True):
        while True:
            print("now",time.time(), self.queue.info)

            try:
                item=self.queue.get(block=False)
            except Empty:
                break

            hashe, modules = item
            print(hashe)
            print(modules)
            self.queue.task_done()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("queue",default="./queue")

    args=parser.parse_args()

    qcworker=QueueCacheWorker(args.queue)
    qcworker.run_all()




