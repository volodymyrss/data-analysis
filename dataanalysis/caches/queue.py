import time

from persistqueue import Queue, Empty

import dataanalysis.emerge as emerge
from dataanalysis.caches.delegating import DelegatingCache


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

        self.load_queue()

    def load_queue(self):
        self.queue = Queue(self.queue_file)

    def run_task(self,object_identity):
        print(object_identity)

        A=emerge.emerge_from_identity(object_identity)

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




