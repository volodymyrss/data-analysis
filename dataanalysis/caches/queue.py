import time

import fsqueue

import dataanalysis.core as da
import dataanalysis.emerge as emerge
import dataanalysis.printhook
from dataanalysis.caches.delegating import DelegatingCache


class QueueCache(DelegatingCache):

    def __init__(self,queue_directory="/tmp/queue"):
        super(QueueCache, self).__init__()
        self.queue_directory=queue_directory
        self.queue = fsqueue.Queue(self.queue_directory)

    def delegate(self, hashe, obj):
        return self.queue.put(dict(
            object_identity=obj.get_identity().serialize(),
            request_origin="undefined",
        ))

    def wipe_queue(self):
        self.queue.wipe()

    def __repr__(self):
        return "["+self.__class__.__name__+": queue in \""+self.queue_directory+"\"]"


class QueueCacheWorker(object):
    def __repr__(self):
        return "[%s: %i]"%(self.__class__.__name__,id(self))

    def __init__(self,queue_directory="/tmp/queue"):
        self.queue_directory = queue_directory

        self.load_queue()

    def load_queue(self):
        self.queue = fsqueue.Queue(self.queue_directory)

    def process_callback(self,task_data,result):
        pass

    def run_task(self,task_data):
        object_identity=da.DataAnalysisIdentity.from_dict(task_data['object_identity'])
        da.reset()

        print(object_identity)
        A=emerge.emerge_from_identity(object_identity)

        print("emerged object:",A)

        result=A.get(requested_by=[repr(self)])
        self.process_callback(task_data,result)

        return result


    def run_once(self):
        task_data=self.queue.get()
        object_identity=task_data['object_identity']

        print("object identity",object_identity)

        self.run_task(task_data)
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
                self.run_task(task)
            except Exception as e:
                print("task failed:",e)
                raise
                self.queue.task_failed() # history and current status
            else:
                self.queue.task_done()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("queue",default="./queue")
    parser.add_argument('-V', dest='very_verbose',  help='...',action='store_true', default=False)
    parser.add_argument('-b', dest='burst_mode',  help='...',action='store_true', default=False)
    parser.add_argument('-w', dest='watch', type=int, help='...', default=0)

    args=parser.parse_args()

    if args.very_verbose:
        #dataanalysis.printhook.global_permissive_output=True
        da.debug_output()

    qcworker = QueueCacheWorker(args.queue)
    if args.watch>0:
        while True:
            print(qcworker.queue.info)
            time.sleep(args.watch)
    else:
        qcworker.run_all(burst=args.burst_mode)





