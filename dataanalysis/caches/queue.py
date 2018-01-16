import time

import fsqueue

import dataanalysis.emerge as emerge
import dataanalysis.printhook
from dataanalysis.caches.delegating import DelegatingCache


class QueueCache(DelegatingCache):

    def __init__(self,queue_directory="/tmp/queue"):
        super(QueueCache, self).__init__()
        self.queue_directory=queue_directory
        self.queue = fsqueue.Queue(self.queue_directory)

    def delegate(self, hashe, obj):
        self.queue.put(dict(
            object_identity=obj.get_identity(),
            request_origin="undefined",
        ))

    def wipe_queue(self):
        self.queue.wipe()


class QueueCacheWorker(object):
    def __init__(self,queue_directory="/tmp/queue"):
        self.queue_directory = queue_directory

        self.load_queue()

    def load_queue(self):
        self.queue = fsqueue.Queue(self.queue_directory)

    def run_task(self,object_identity):
        print(object_identity)
        A=emerge.emerge_from_identity(object_identity)

        return A.get()


    def run_once(self):
        object_identity=self.queue.get()['object_identity']

        print("object identity",object_identity)

        self.run_task(object_identity)
        self.queue.task_done()

    def run_all(self,burst=True,wait=1):
        while True:
            print("now",time.time(), self.queue.info)

            try:
                task=self.queue.get()
            except fsqueue.Empty:
                if burst:
                    break
                else:
                    time.sleep(wait)
                    continue

            try:
                self.run_task(task['object_identity'])
            except Exception as e:
                self.queue.task_failed() # history and current status
            else:
                self.queue.task_done()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("queue",default="./queue")
    parser.add_argument('-V', dest='very_verbose',  help='...',action='store_true', default=False)
    parser.add_argument('-b', dest='burst_mode',  help='...',action='store_true', default=False)

    args=parser.parse_args()

    if args.very_verbose:
        dataanalysis.printhook.global_permissive_output=True


    qcworker=QueueCacheWorker(args.queue)
    qcworker.run_all(burst=args.burst_mode)




